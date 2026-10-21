package headlinepulse.poller;

import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import headlinepulse.common.KafkaSettings;

public final class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private static final String DEFAULT_JETSTREAM_URL =
            "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post";
    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(20);

    public static void main(String[] args) throws InterruptedException {
        String jetstreamUrl = System.getenv().getOrDefault("JETSTREAM_URL", DEFAULT_JETSTREAM_URL);
        String bootstrapServers = KafkaSettings.bootstrapServers();
        log.info("Poller starting: {} -> Kafka at {}", jetstreamUrl, bootstrapServers);

        var poller = new Poller(jetstreamUrl, bootstrapServers);
        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down: committing the current batch");
            poller.stop();
            try {
                mainThread.join(SHUTDOWN_TIMEOUT.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "shutdown"));

        poller.run();
        log.info("Poller stopped");
    }
}
