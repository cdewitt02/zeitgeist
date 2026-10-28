package headlinepulse.headlines;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import headlinepulse.common.Headline;
import headlinepulse.common.JsonSerde;
import headlinepulse.common.KafkaSettings;
import headlinepulse.common.NewsDomains;
import headlinepulse.common.Topics;

public final class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private static final Duration POLL_INTERVAL = Duration.ofMinutes(3);
    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(30);

    public static void main(String[] args) throws IOException, InterruptedException {
        Path feedsFile = Path.of(System.getenv().getOrDefault("FEEDS", "config/feeds.yaml"));
        List<Feed> feeds = Feed.load(feedsFile);
        NewsDomains newsDomains = NewsDomains.load(Path.of(System.getenv().getOrDefault("NEWS_DOMAINS", "config/news-domains.yaml")));
        String bootstrapServers = KafkaSettings.bootstrapServers();
        log.info("Headlines starting: {} feeds from {} -> Kafka at {}", feeds.size(), feedsFile, bootstrapServers);

        Set<String> seen = loadSeen(bootstrapServers);
        log.info("{} headlines already in {}", seen.size(), Topics.NEWS_HEADLINES);

        var producer = new KafkaProducer<>(Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ProducerConfig.ACKS_CONFIG, "all",
                ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true),
                new StringSerializer(), new JsonSerde<>(Headline.class).serializer());
        var poller = new HeadlinePoller(feeds, new FeedFetcher(), newsDomains, producer, seen);

        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "poll"));
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                poller.pollAll();
            } catch (RuntimeException e) {
                // An exception escaping here would cancel every later poll.
                log.error("Poll failed", e);
            }
        }, 0, POLL_INTERVAL.toMillis(), TimeUnit.MILLISECONDS);

        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down");
            scheduler.shutdownNow();
            try {
                mainThread.join(SHUTDOWN_TIMEOUT.toMillis());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "shutdown"));

        while (!scheduler.awaitTermination(1, TimeUnit.DAYS)) {
            // Runs until the shutdown hook stops the scheduler.
        }
        producer.close(SHUTDOWN_TIMEOUT);
        log.info("Headlines stopped");
    }

    /** Every article URL already in the compacted topic, so a restart doesn't send them again. */
    private static Set<String> loadSeen(String bootstrapServers) {
        Set<String> seen = new HashSet<>();
        try (var consumer = new KafkaConsumer<>(Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false),
                new StringDeserializer(), new ByteArrayDeserializer())) {
            List<TopicPartition> partitions = consumer.partitionsFor(Topics.NEWS_HEADLINES).stream()
                    .map(p -> new TopicPartition(p.topic(), p.partition()))
                    .toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            Map<TopicPartition, Long> ends = consumer.endOffsets(partitions);
            while (partitions.stream().anyMatch(tp -> consumer.position(tp) < ends.get(tp))) {
                for (ConsumerRecord<String, byte[]> r : consumer.poll(Duration.ofSeconds(1))) {
                    // A tombstone removes a headline, so it can be sent again.
                    if (r.value() == null) {
                        seen.remove(r.key());
                    } else {
                        seen.add(r.key());
                    }
                }
            }
        }
        return seen;
    }

    private Main() {}
}
