package headlinepulse.common;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.TopicConfig;

/** Every Kafka topic in the system. {@code tools:createTopics} creates them from {@link #ALL}. */
public final class Topics {
    /** Raw Jetstream JSON, keyed by DID, timestamped with the event's {@code time_us}. */
    public static final String BSKY_RAW = "bsky.raw";
    /** The poller's last committed Jetstream {@code time_us}, keyed by {@code poller}. */
    public static final String BSKY_CURSOR = "bsky.cursor";
    /** Parsed, deduplicated English {@link Post}s as JSON, keyed by post URI. Deletes are tombstones. */
    public static final String BSKY_POSTS = "bsky.posts";
    /** {@link MatchedPost}s as JSON, keyed by post URI. Deletes of matched posts are tombstones. */
    public static final String POSTS_MATCHED = "posts.matched";

    public record Spec(String name, int partitions, Map<String, String> configs) {}

    public static final List<Spec> ALL = List.of(
            new Spec(BSKY_RAW, 3, Map.of(
                    TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE,
                    TopicConfig.RETENTION_MS_CONFIG, millis(Duration.ofDays(21)),
                    TopicConfig.COMPRESSION_TYPE_CONFIG, "zstd")),
            new Spec(BSKY_CURSOR, 1, Map.of(
                    TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT,
                    // Compaction skips the active segment, so roll it often.
                    TopicConfig.SEGMENT_MS_CONFIG, millis(Duration.ofHours(1)))),
            // A debugging aid: lets you inspect what the pipeline parsed.
            new Spec(BSKY_POSTS, 3, Map.of(
                    TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE,
                    TopicConfig.RETENTION_MS_CONFIG, millis(Duration.ofDays(7)),
                    TopicConfig.COMPRESSION_TYPE_CONFIG, "zstd")),
            new Spec(POSTS_MATCHED, 3, Map.of(
                    TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_DELETE,
                    TopicConfig.RETENTION_MS_CONFIG, millis(Duration.ofDays(7)),
                    TopicConfig.COMPRESSION_TYPE_CONFIG, "zstd")));

    private static String millis(Duration d) {
        return Long.toString(d.toMillis());
    }

    private Topics() {}
}
