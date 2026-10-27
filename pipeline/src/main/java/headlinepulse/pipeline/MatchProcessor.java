package headlinepulse.pipeline;

import java.time.Duration;
import java.util.Optional;

import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import headlinepulse.common.MatchedPost;
import headlinepulse.common.Post;

/**
 * Posts and tombstones from {@link Normalizer} in, matched posts out. A tombstone passes only if its
 * post matched within {@link #REMEMBER}: nearly every delete is of a post that never matched, and
 * passing those on would bury the matches.
 *
 * <p>Like {@link Normalizer}, it needs no repartition: input is keyed by URI, and every event for a
 * URI comes from the same {@code bsky.raw} partition.
 */
final class MatchProcessor implements Processor<String, Post, String, MatchedPost> {
    static final String MATCHED_STORE = "matched-uris";
    /** As long as {@code posts.matched} keeps a post. */
    static final Duration REMEMBER = Duration.ofDays(7);

    private final Stats stats;
    private final PostMatcher matcher;
    private ProcessorContext<String, MatchedPost> context;
    private WindowStore<String, Long> matchedUris;

    MatchProcessor(Stats stats, PostMatcher matcher) {
        this.stats = stats;
        this.matcher = matcher;
    }

    @Override
    public void init(ProcessorContext<String, MatchedPost> context) {
        this.context = context;
        this.matchedUris = context.getStateStore(MATCHED_STORE);
    }

    @Override
    public void process(Record<String, Post> record) {
        if (record.value() == null) {
            if (wasMatched(record.key(), record.timestamp())) {
                stats.matchedDeletes.increment();
                context.forward(record.withValue(null));
            }
            return;
        }
        Optional<MatchedPost> matched = matcher.match(record.value());
        if (matched.isPresent()) {
            stats.matched.increment();
            matchedUris.put(record.key(), record.timestamp(), record.timestamp());
            context.forward(record.withValue(matched.get()));
        }
    }

    private boolean wasMatched(String uri, long timestamp) {
        try (WindowStoreIterator<Long> it = matchedUris.fetch(uri, timestamp - REMEMBER.toMillis(), timestamp)) {
            return it.hasNext();
        }
    }
}
