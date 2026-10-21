package headlinepulse.tools;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import headlinepulse.common.KafkaSettings;
import headlinepulse.common.Topics;

/**
 * Checks {@code bsky.raw} for holes between two event times, for the poller's restart and kill tests.
 * Prints the largest gaps between consecutive event timestamps and the number of duplicate events.
 *
 * <p>Usage: {@code ./gradlew :tools:checkGaps --args="2026-10-20T01:00:00Z 2026-10-20T01:30:00Z"}
 */
public final class CheckGaps {
    private static final Logger log = LoggerFactory.getLogger(CheckGaps.class);
    private static final int TOP = 5;

    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Usage: checkGaps <from> <to>   (ISO instants, e.g. 2026-10-20T01:00:00Z)");
            System.exit(2);
        }
        long fromMs = Instant.parse(args[0]).toEpochMilli();
        long toMs = Instant.parse(args[1]).toEpochMilli();

        Map<String, Object> props = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaSettings.bootstrapServers(),
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5_000,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        List<Long> times = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        long duplicates = 0;

        try (var consumer = new KafkaConsumer<String, String>(props)) {
            List<TopicPartition> partitions = consumer.partitionsFor(Topics.BSKY_RAW).stream()
                    .map(p -> new TopicPartition(p.topic(), p.partition()))
                    .toList();
            consumer.assign(partitions);
            Map<TopicPartition, Long> ends = consumer.endOffsets(partitions);

            Map<TopicPartition, Long> query = new HashMap<>();
            partitions.forEach(tp -> query.put(tp, fromMs));
            Map<TopicPartition, OffsetAndTimestamp> starts = consumer.offsetsForTimes(query);

            Set<TopicPartition> open = new HashSet<>();
            for (TopicPartition tp : partitions) {
                OffsetAndTimestamp start = starts.get(tp);
                if (start != null) {
                    consumer.seek(tp, start.offset());
                    open.add(tp);
                }
            }

            // Events can be up to a few seconds out of order, so read a little past the end time.
            long stopMs = toMs + 60_000;
            while (!open.isEmpty()) {
                for (ConsumerRecord<String, String> r : consumer.poll(Duration.ofSeconds(1))) {
                    TopicPartition tp = new TopicPartition(r.topic(), r.partition());
                    if (r.timestamp() > stopMs) {
                        open.remove(tp);
                        consumer.pause(List.of(tp));
                        continue;
                    }
                    if (r.timestamp() < fromMs || r.timestamp() > toMs) {
                        continue;
                    }
                    times.add(r.timestamp());
                    if (!seen.add(r.key() + "|" + r.value().hashCode() + "|" + r.value().length())) {
                        duplicates++;
                    }
                }
                open.removeIf(tp -> !consumer.paused().contains(tp) && consumer.position(tp) >= ends.get(tp));
            }
        }

        long[] sorted = times.stream().mapToLong(Long::longValue).sorted().toArray();
        if (sorted.length < 2) {
            log.info("Only {} events between {} and {}", sorted.length, args[0], args[1]);
            return;
        }
        List<long[]> gaps = new ArrayList<>();
        for (int i = 1; i < sorted.length; i++) {
            gaps.add(new long[] {sorted[i] - sorted[i - 1], sorted[i - 1]});
        }
        gaps.sort(Comparator.comparingLong((long[] g) -> g[0]).reversed());

        double spanSecs = (sorted[sorted.length - 1] - sorted[0]) / 1000.0;
        log.info("{} events from {} to {} ({} unique, {} duplicates), {} events/s of event time",
                sorted.length, Instant.ofEpochMilli(sorted[0]), Instant.ofEpochMilli(sorted[sorted.length - 1]),
                sorted.length - duplicates, duplicates, "%.1f".formatted((sorted.length - duplicates) / spanSecs));
        log.info("Largest gaps between consecutive events:");
        gaps.stream().limit(TOP).forEach(g ->
                log.info("  {} ms after {}", g[0], Instant.ofEpochMilli(g[1])));
        long largest = gaps.getFirst()[0];
        log.info(largest > 5_000 ? "GAP FOUND: {} ms" : "No gap: the largest is {} ms", largest);
    }

    private CheckGaps() {}
}
