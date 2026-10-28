package headlinepulse.tools;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import headlinepulse.common.JsonSerde;
import headlinepulse.common.KafkaSettings;
import headlinepulse.common.Post;
import headlinepulse.common.TopicMatcher;
import headlinepulse.common.Topics;

/**
 * For checking topic precision by hand. Runs the current {@code config/topics.yaml} over the most
 * recent posts in {@code bsky.posts} and prints a random sample of one topic's matches, each with what
 * matched it. Edit the config and run it again to see the effect, with no pipeline replay.
 *
 * <p>With no topic, or {@code all}, prints how many posts match each topic instead.
 *
 * <p>Usage: {@code ./gradlew :tools:reviewTopic --args="tx-gov-abbott [count=50] [scan=2000000]"},
 * where {@code scan} is how many of the most recent posts to read.
 */
public final class ReviewTopic {
    private static final int TEXT_LIMIT = 280;
    /** Fixed, so a rerun after a config change shows mostly the same posts. */
    private static final long SEED = 1;

    private record Sampled(Post post, String reason) {}

    public static void main(String[] args) throws IOException {
        String topic = args.length > 0 && !args[0].equals("all") ? args[0] : null;
        int count = args.length > 1 ? Integer.parseInt(args[1]) : 50;
        long scan = args.length > 2 ? Long.parseLong(args[2]) : 2_000_000;

        TopicMatcher matcher = TopicMatcher.load(Path.of(System.getenv().getOrDefault("TOPICS", "config/topics.yaml")));
        if (topic != null && matcher.topic(topic).isEmpty()) {
            System.err.println("Unknown topic '" + topic + "'. Topics: "
                    + matcher.topics().stream().map(TopicMatcher.Topic::id).toList());
            System.exit(2);
        }

        Map<String, Long> counts = new HashMap<>();
        List<Sampled> sample = new ArrayList<>();
        var random = new Random(SEED);
        long scanned = 0;
        long matchedTopic = 0;
        Instant first = null;
        Instant last = null;

        try (var consumer = new KafkaConsumer<>(Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaSettings.bootstrapServers(),
                ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false,
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 5_000),
                new StringDeserializer(), new JsonSerde<>(Post.class).deserializer())) {
            List<TopicPartition> partitions = consumer.partitionsFor(Topics.BSKY_POSTS).stream()
                    .map(p -> new TopicPartition(p.topic(), p.partition()))
                    .toList();
            consumer.assign(partitions);
            Map<TopicPartition, Long> begins = consumer.beginningOffsets(partitions);
            Map<TopicPartition, Long> ends = consumer.endOffsets(partitions);
            long perPartition = scan / partitions.size();
            Set<TopicPartition> open = new HashSet<>();
            for (TopicPartition tp : partitions) {
                consumer.seek(tp, Math.max(begins.get(tp), ends.get(tp) - perPartition));
                if (consumer.position(tp) < ends.get(tp)) {
                    open.add(tp);
                }
            }

            while (!open.isEmpty()) {
                for (ConsumerRecord<String, Post> r : consumer.poll(Duration.ofSeconds(1))) {
                    Post post = r.value();
                    if (post == null) {
                        continue;
                    }
                    scanned++;
                    first = first == null || post.time().isBefore(first) ? post.time() : first;
                    last = last == null || post.time().isAfter(last) ? post.time() : last;

                    List<String> matched = matcher.match(post);
                    matched.forEach(id -> counts.merge(id, 1L, Long::sum));
                    if (topic != null && matched.contains(topic)) {
                        // Reservoir sampling: every match is equally likely to be shown.
                        matchedTopic++;
                        var s = new Sampled(post, reason(matcher, post, topic));
                        if (sample.size() < count) {
                            sample.add(s);
                        } else {
                            long j = (long) (random.nextDouble() * matchedTopic);
                            if (j < count) {
                                sample.set((int) j, s);
                            }
                        }
                    }
                }
                open.removeIf(tp -> consumer.position(tp) >= ends.get(tp));
            }
        }

        System.out.printf("Scanned %,d English posts from %s to %s%n%n", scanned, first, last);
        if (topic == null) {
            for (TopicMatcher.Topic t : matcher.topics()) {
                System.out.printf("%-22s %,9d  %s%n", t.id(), counts.getOrDefault(t.id(), 0L), t.label());
            }
            return;
        }

        sample.sort((a, b) -> a.post().time().compareTo(b.post().time()));
        for (int i = 0; i < sample.size(); i++) {
            Sampled s = sample.get(i);
            System.out.printf("%2d. [%s] %s%n", i + 1, s.reason(), oneLine(s.post().text()));
            if (s.post().card() != null && !s.post().card().title().isBlank()) {
                System.out.printf("    card: %s%n", oneLine(s.post().card().title()));
            }
        }
        System.out.printf("%n%,d of %,d posts match %s; showing %d.%n", matchedTopic, scanned, topic, sample.size());
        System.out.println("Mark each as on-topic or not. Precision = on-topic / shown.");
    }

    /** What matched the post directly, or which child topic brought it in. */
    private static String reason(TopicMatcher matcher, Post post, String topic) {
        Map<String, String> reasons = matcher.reasons(post);
        if (reasons.containsKey(topic)) {
            return reasons.get(topic);
        }
        return reasons.entrySet().stream()
                .filter(e -> isDescendant(matcher, e.getKey(), topic))
                .map(e -> e.getKey() + ": " + e.getValue())
                .findFirst()
                .orElse("?");
    }

    private static boolean isDescendant(TopicMatcher matcher, String id, String ancestor) {
        for (String p = matcher.topic(id).orElseThrow().parent(); p != null; p = matcher.topic(p).orElseThrow().parent()) {
            if (p.equals(ancestor)) {
                return true;
            }
        }
        return false;
    }

    private static String oneLine(String text) {
        String t = text.replaceAll("\\s+", " ").strip();
        return t.length() <= TEXT_LIMIT ? t : t.substring(0, TEXT_LIMIT) + "…";
    }

    private ReviewTopic() {}
}
