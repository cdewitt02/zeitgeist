package headlinepulse.poller;

import java.net.URI;
import java.net.http.HttpClient;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.InvalidProducerEpochException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import headlinepulse.common.Topics;

/**
 * Copies Jetstream into {@code bsky.raw}. Each connection is a session: start a transactional producer,
 * read the last committed cursor, connect from 5 s before it, and commit events and cursor together
 * every 500 ms. Any failure ends the session, and the next one replays from the committed cursor, so
 * restarts and crashes cause duplicates but never gaps.
 */
final class Poller {
    private static final Logger log = LoggerFactory.getLogger(Poller.class);

    private static final Duration COMMIT_INTERVAL = Duration.ofMillis(500);
    private static final Duration REWIND = Duration.ofSeconds(5);
    private static final Duration STALL_TIMEOUT = Duration.ofSeconds(30);
    private static final Duration MIN_BACKOFF = Duration.ofSeconds(1);
    private static final Duration MAX_BACKOFF = Duration.ofSeconds(30);
    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(20);
    /** Jetstream keeps about a day of history (36 h measured on 2026-10-20); older cursors may be gone. */
    private static final Duration HISTORY = Duration.ofDays(1);
    /** With no saved cursor, ask for more than Jetstream keeps, so it starts from its oldest event. */
    private static final Duration FIRST_RUN_LOOKBACK = Duration.ofDays(2);
    /** A first event this far past the requested cursor means Jetstream no longer had what's between. */
    private static final Duration GAP_TOLERANCE = Duration.ofMinutes(1);
    /** How long to watch the cursor after being fenced, to see whether another poller is committing. */
    private static final Duration FENCE_CHECK = Duration.ofSeconds(2);
    private static final int QUEUE_CAPACITY = 10_000;
    private static final String TRANSACTIONAL_ID = "poller";

    private final String jetstreamUrl;
    private final String bootstrapServers;
    private final HttpClient http = HttpClient.newHttpClient();
    private final CountDownLatch stopSignal = new CountDownLatch(1);
    private final Stats stats = new Stats();

    Poller(String jetstreamUrl, String bootstrapServers) {
        this.jetstreamUrl = jetstreamUrl;
        this.bootstrapServers = bootstrapServers;
    }

    /** Runs until {@link #stop()} is called, reconnecting with backoff after failures. */
    void run() throws InterruptedException {
        try (http) {
            Duration backoff = MIN_BACKOFF;
            while (!stopping()) {
                boolean received = false;
                try {
                    received = runSession();
                } catch (InterruptedException e) {
                    throw e;
                } catch (Exception e) {
                    if (isFenced(e) && anotherPollerIsRunning()) {
                        log.error("Another poller took over (transactional.id '{}'). Exiting.", TRANSACTIONAL_ID);
                        return;
                    }
                    log.warn("Session failed: {}", e.toString());
                }
                if (stopping()) {
                    return;
                }
                if (received) {
                    backoff = MIN_BACKOFF;
                }
                log.info("Reconnecting in {}", human(backoff));
                if (stopSignal.await(backoff.toMillis(), TimeUnit.MILLISECONDS)) {
                    return;
                }
                Duration doubled = backoff.multipliedBy(2);
                backoff = doubled.compareTo(MAX_BACKOFF) < 0 ? doubled : MAX_BACKOFF;
            }
        }
    }

    /** Ends the current session after committing what has been received. Safe to call from any thread. */
    void stop() {
        stopSignal.countDown();
    }

    private boolean stopping() {
        return stopSignal.getCount() == 0;
    }

    /** Returns whether any events arrived. */
    private boolean runSession() throws Exception {
        var producer = new KafkaProducer<String, String>(producerProps());
        try {
            // Fences any earlier poller and aborts its open transaction before the cursor is read.
            producer.initTransactions();
            OptionalLong saved = CursorStore.readLast(bootstrapServers);
            long requested = startCursor(saved);

            var queue = new ArrayBlockingQueue<RawEvent>(QUEUE_CAPACITY);
            var listener = new JetstreamListener(queue);
            URI uri = URI.create(jetstreamUrl + (jetstreamUrl.contains("?") ? "&" : "?") + "cursor=" + requested);
            try {
                http.newWebSocketBuilder()
                        .connectTimeout(CONNECT_TIMEOUT)
                        .buildAsync(uri, listener)
                        .get(CONNECT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
                log.info("Connected to {}", uri);
                return pump(producer, queue, listener, saved, requested);
            } finally {
                listener.abort();
            }
        } finally {
            producer.close(Duration.ofSeconds(5));
        }
    }

    private boolean pump(KafkaProducer<String, String> producer, BlockingQueue<RawEvent> queue,
            JetstreamListener listener, OptionalLong saved, long requested) throws InterruptedException {
        List<RawEvent> batch = new ArrayList<>(QUEUE_CAPACITY);
        long sent = -1;
        long committed = -1;
        boolean received = false;
        long nextCommit = System.nanoTime() + COMMIT_INTERVAL.toNanos();
        producer.beginTransaction();

        while (true) {
            String endReason = stopping() ? "stopping"
                    : listener.isClosed() ? "connection closed"
                    : listener.isStalled(STALL_TIMEOUT) ? "no events for " + human(STALL_TIMEOUT)
                    : null;
            if (endReason != null) {
                // Stop reading, then commit whatever is already queued.
                listener.abort();
            }

            RawEvent head = endReason == null ? queue.poll(100, TimeUnit.MILLISECONDS) : queue.poll();
            if (head != null) {
                batch.add(head);
                queue.drainTo(batch);
            }
            for (RawEvent e : batch) {
                if (!received) {
                    reportFirstEvent(saved, requested, e.timeUs());
                    received = true;
                }
                producer.send(new ProducerRecord<>(Topics.BSKY_RAW, null, e.timeUs() / 1000, e.did(), e.json()));
                sent = Math.max(sent, e.timeUs());
                stats.count(e);
            }
            batch.clear();

            if (endReason != null || System.nanoTime() >= nextCommit) {
                if (sent > committed) {
                    producer.send(CursorStore.record(sent));
                }
                producer.commitTransaction();
                committed = sent;
                if (endReason != null) {
                    log.info("Session ended ({}); committed through {}", endReason,
                            committed > 0 ? time(committed) : "nothing new");
                    return received;
                }
                producer.beginTransaction();
                nextCommit = System.nanoTime() + COMMIT_INTERVAL.toNanos();
            }
            stats.maybeLog(queue.size(), committed, listener.malformedCount());
        }
    }

    private static long startCursor(OptionalLong saved) {
        long nowUs = ChronoUnit.MICROS.between(Instant.EPOCH, Instant.now());
        if (saved.isEmpty()) {
            log.info("No saved cursor; starting from the oldest event Jetstream still has");
            return nowUs - micros(FIRST_RUN_LOOKBACK);
        }
        long cursor = saved.getAsLong();
        Duration age = Duration.of(nowUs - cursor, ChronoUnit.MICROS);
        if (age.compareTo(HISTORY) > 0) {
            log.warn("Saved cursor {} is {} old, which may be past Jetstream's history. If it is, a GAP "
                    + "warning below will say how much is missing.", time(cursor), human(age));
        } else {
            log.info("Resuming from cursor {} ({} ago)", time(cursor), human(age));
        }
        return cursor - micros(REWIND);
    }

    private static void reportFirstEvent(OptionalLong saved, long requested, long firstUs) {
        Duration gap = Duration.of(firstUs - requested, ChronoUnit.MICROS);
        if (saved.isPresent() && gap.compareTo(GAP_TOLERANCE) > 0) {
            log.warn("GAP: asked for events from {} but the first was from {}. About {} of posts are missing.",
                    time(requested), time(firstUs), human(gap));
        } else {
            log.info("First event is from {}", time(firstUs));
        }
    }

    private Map<String, Object> producerProps() {
        return Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.TRANSACTIONAL_ID_CONFIG, TRANSACTIONAL_ID,
                ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd",
                // Longer than the commit interval, so each commit sends one large batch per partition,
                // which compresses far better than many small ones.
                ProducerConfig.LINGER_MS_CONFIG, 1_000,
                ProducerConfig.BATCH_SIZE_CONFIG, 512 * 1024,
                ProducerConfig.MAX_BLOCK_MS_CONFIG, 30_000,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    }

    /** Fencing shows up as either exception, depending on the broker's transaction protocol version. */
    private static boolean isFenced(Throwable e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof ProducerFencedException || t instanceof InvalidProducerEpochException) {
                return true;
            }
        }
        return false;
    }

    /**
     * A fence can also come from the broker timing out our transaction, so only give up if the cursor keeps
     * moving while this poller is idle. Otherwise two pollers would fence each other forever.
     */
    private boolean anotherPollerIsRunning() throws InterruptedException {
        OptionalLong before = CursorStore.readLast(bootstrapServers);
        stopSignal.await(FENCE_CHECK.toMillis(), TimeUnit.MILLISECONDS);
        return !CursorStore.readLast(bootstrapServers).equals(before);
    }

    private static long micros(Duration d) {
        return d.toNanos() / 1_000;
    }

    static String time(long timeUs) {
        return Instant.EPOCH.plus(timeUs, ChronoUnit.MICROS).truncatedTo(ChronoUnit.SECONDS).toString();
    }

    static String human(Duration d) {
        long secs = d.toSeconds();
        if (secs < 60) {
            return "%.1f s".formatted(d.toMillis() / 1000.0);
        }
        if (secs < 3600) {
            return "%d min %d s".formatted(secs / 60, secs % 60);
        }
        return "%d h %d min".formatted(secs / 3600, secs % 3600 / 60);
    }
}
