package headlinepulse.poller;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import headlinepulse.common.Topics;

/**
 * The Jetstream cursor in {@code bsky.cursor}. The poller writes it in the same transaction as the
 * events it covers, so the last committed cursor is never ahead of the data in {@code bsky.raw}.
 */
final class CursorStore {
    private static final String KEY = "poller";
    private static final TopicPartition PARTITION = new TopicPartition(Topics.BSKY_CURSOR, 0);
    /** Each commit uses two offsets (the cursor and the commit marker), so this covers many commits. */
    private static final int TAIL = 1_000;
    private static final Duration READ_TIMEOUT = Duration.ofSeconds(30);

    static ProducerRecord<String, String> record(long timeUs) {
        return new ProducerRecord<>(Topics.BSKY_CURSOR, KEY, Long.toString(timeUs));
    }

    /**
     * Reads the last committed cursor. Call after the producer's {@code initTransactions()}, which aborts
     * any transaction left open by a killed poller; until then, committed reads can't get past it.
     */
    static OptionalLong readLast(String bootstrapServers) {
        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        try (var consumer = new KafkaConsumer<String, String>(props)) {
            if (consumer.partitionsFor(Topics.BSKY_CURSOR).isEmpty()) {
                throw new IllegalStateException(
                        "Topic " + Topics.BSKY_CURSOR + " doesn't exist. Run ./gradlew :tools:createTopics");
            }
            consumer.assign(List.of(PARTITION));
            long begin = consumer.beginningOffsets(List.of(PARTITION)).get(PARTITION);
            long end = consumer.endOffsets(List.of(PARTITION)).get(PARTITION);

            // Read the tail first; fall back to the whole partition if the tail held no committed cursor.
            long from = Math.max(begin, end - TAIL);
            OptionalLong last = readRange(consumer, from, end);
            if (last.isEmpty() && from > begin) {
                last = readRange(consumer, begin, end);
            }
            return last;
        }
    }

    private static OptionalLong readRange(KafkaConsumer<String, String> consumer, long from, long end) {
        consumer.seek(PARTITION, from);
        long deadline = System.nanoTime() + READ_TIMEOUT.toNanos();
        OptionalLong last = OptionalLong.empty();
        while (consumer.position(PARTITION) < end) {
            if (System.nanoTime() > deadline) {
                throw new IllegalStateException("Timed out reading " + Topics.BSKY_CURSOR);
            }
            for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofMillis(500))) {
                if (r.offset() < end && KEY.equals(r.key()) && r.value() != null) {
                    last = OptionalLong.of(Long.parseLong(r.value()));
                }
            }
        }
        return last;
    }

    private CursorStore() {}
}
