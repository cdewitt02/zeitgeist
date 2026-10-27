package headlinepulse.pipeline;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import headlinepulse.common.KafkaSettings;

public final class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    /** Change this (or run kafka-streams-application-reset) to reprocess bsky.raw from the start. */
    static final String APPLICATION_ID = "headline-pulse-pipeline";
    private static final Duration STATS_INTERVAL = Duration.ofSeconds(10);
    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(30);

    public static void main(String[] args) throws InterruptedException {
        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaSettings.bootstrapServers());
        // The dedupe store and the output commit together, so a crash can't let a duplicate through.
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        // A fresh application.id starts from the oldest raw event, so a reset replays everything.
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
        // Not /tmp, which the OS may clear, forcing a full restore of the dedupe store from its changelog.
        props.put(StreamsConfig.STATE_DIR_CONFIG, System.getenv().getOrDefault("PIPELINE_STATE_DIR",
                Path.of(System.getProperty("user.home"), ".headline-pulse", "kafka-streams").toString()));

        var stats = new Stats();
        var streams = new KafkaStreams(NormalizeTopology.build(stats), props);
        var stopped = new CountDownLatch(1);
        streams.setUncaughtExceptionHandler(e -> {
            log.error("Stream thread failed; shutting down", e);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });
        streams.setStateListener((now, before) -> {
            log.info("Streams {} -> {}", before, now);
            if (now == KafkaStreams.State.NOT_RUNNING || now == KafkaStreams.State.ERROR) {
                stopped.countDown();
            }
        });

        ScheduledExecutorService ticker = Executors.newSingleThreadScheduledExecutor(r -> {
            var t = new Thread(r, "stats");
            t.setDaemon(true);
            return t;
        });
        ticker.scheduleAtFixedRate(stats::log, STATS_INTERVAL.toMillis(), STATS_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down");
            streams.close(SHUTDOWN_TIMEOUT);
        }, "shutdown"));

        log.info("Pipeline starting: {} -> Kafka at {}", APPLICATION_ID, KafkaSettings.bootstrapServers());
        streams.start();
        stopped.await();
        ticker.shutdownNow();
        log.info("Pipeline stopped");
    }

    private Main() {}
}
