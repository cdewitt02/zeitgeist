package headlinepulse.pipeline;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import headlinepulse.common.BskyEvent;
import headlinepulse.common.Delete;
import headlinepulse.common.Post;
import headlinepulse.common.PostParser;

/**
 * Raw Jetstream JSON in, English {@link Post}s and delete tombstones out, keyed by post URI.
 *
 * <p>Drops an event if the same one was seen within {@link #DEDUPE_WINDOW} of it, which covers the
 * poller's 5-second rewind on restart. No repartition is needed first: {@code bsky.raw} is keyed by
 * DID, and a URI contains its DID, so every copy of an event lands in the same task.
 */
final class Normalizer implements Processor<String, String, String, Post> {
    private static final Logger log = LoggerFactory.getLogger(Normalizer.class);

    static final String DEDUPE_STORE = "dedupe";
    static final Duration DEDUPE_WINDOW = Duration.ofMinutes(10);
    /** Log this many unparseable events in full, then only every 1000th. */
    private static final long LOG_FIRST = 10;

    private final Stats stats;
    private ProcessorContext<String, Post> context;
    private WindowStore<String, Long> seen;

    Normalizer(Stats stats) {
        this.stats = stats;
    }

    @Override
    public void init(ProcessorContext<String, Post> context) {
        this.context = context;
        this.seen = context.getStateStore(DEDUPE_STORE);
    }

    @Override
    public void process(Record<String, String> raw) {
        Optional<BskyEvent> parsed;
        try {
            if (raw.value() == null) {
                throw new IllegalArgumentException("Null value");
            }
            parsed = PostParser.parse(raw.value());
        } catch (Exception e) {
            stats.unparseable.increment();
            logUnparseable(e);
            return;
        }
        if (parsed.isEmpty()) {
            stats.ignored.increment();
            return;
        }

        switch (parsed.get()) {
            case Post post -> {
                stats.posts.increment();
                if (!isEnglish(post.langs())) {
                    return;
                }
                stats.english.increment();
                if (firstSighting(post.uri(), raw.timestamp())) {
                    context.forward(raw.withKey(post.uri()).withValue(post));
                }
            }
            case Delete delete -> {
                stats.deletes.increment();
                // Kept apart from the post's own entry, so a delete is never mistaken for a repeat of the create.
                if (firstSighting("delete|" + delete.uri(), raw.timestamp())) {
                    context.forward(raw.withKey(delete.uri()).withValue(null));
                }
            }
        }
    }

    /** True if any declared language is English ({@code en}, {@code en-US}, ...). Undeclared isn't English. */
    static boolean isEnglish(List<String> langs) {
        for (String lang : langs) {
            if (lang.equalsIgnoreCase("en") || lang.regionMatches(true, 0, "en-", 0, 3)) {
                return true;
            }
        }
        return false;
    }

    /** Records {@code key} at {@code timestamp}; false if it was already seen within the window. */
    private boolean firstSighting(String key, long timestamp) {
        long window = DEDUPE_WINDOW.toMillis();
        try (WindowStoreIterator<Long> it = seen.fetch(key, timestamp - window, timestamp + window)) {
            if (it.hasNext()) {
                stats.duplicates.increment();
                return false;
            }
        }
        seen.put(key, timestamp, timestamp);
        return true;
    }

    private void logUnparseable(Exception e) {
        long n = stats.unparseable.sum();
        if (n <= LOG_FIRST || n % 1000 == 0) {
            String where = context.recordMetadata()
                    .map(m -> m.topic() + "-" + m.partition() + "@" + m.offset())
                    .orElse("unknown offset");
            log.warn("Unparseable event #{} at {}: {}", n, where, e.toString());
        }
    }
}
