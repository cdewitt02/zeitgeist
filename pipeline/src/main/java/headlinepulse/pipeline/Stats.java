package headlinepulse.pipeline;

import java.util.concurrent.atomic.LongAdder;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import headlinepulse.common.MatchedPost;

/** Counts what the normalize step does with each raw event. Safe to update from any stream thread. */
final class Stats {
    private static final Logger log = LoggerFactory.getLogger(Stats.class);

    final LongAdder posts = new LongAdder();
    final LongAdder english = new LongAdder();
    final LongAdder matched = new LongAdder();
    final LongAdder urlMatched = new LongAdder();
    final LongAdder topicMatched = new LongAdder();
    final LongAdder sampled = new LongAdder();
    final LongAdder matchedDeletes = new LongAdder();
    final LongAdder deletes = new LongAdder();
    final LongAdder duplicates = new LongAdder();
    final LongAdder ignored = new LongAdder();
    final LongAdder unparseable = new LongAdder();

    private long lastPosts;
    private long lastEnglish;
    private long lastNanos = System.nanoTime();

    /** Counts a post written to posts.matched, once in total and once per way it matched. */
    void count(MatchedPost m) {
        matched.increment();
        if (!m.urls().isEmpty()) {
            urlMatched.increment();
        }
        if (!m.topics().isEmpty()) {
            topicMatched.increment();
        }
        if (m.sample()) {
            sampled.increment();
        }
    }

    /** Logs totals, plus the post rate and English share since the last call. Call from one thread. */
    void log() {
        long now = System.nanoTime();
        long p = posts.sum();
        long en = english.sum();
        double secs = (now - lastNanos) / 1e9;
        long dp = p - lastPosts;
        long den = en - lastEnglish;
        log.info("{} posts/s, {} English | totals: {} posts, {} English ({}), {} matched ({} URL, {} topic, {} sample), {} deletes ({} of matched posts), {} duplicates, {} ignored, {} unparseable",
                "%.1f".formatted(dp / secs), percent(den, dp),
                p, en, percent(en, p), matched.sum(), urlMatched.sum(), topicMatched.sum(), sampled.sum(), deletes.sum(), matchedDeletes.sum(), duplicates.sum(), ignored.sum(), unparseable.sum());
        lastPosts = p;
        lastEnglish = en;
        lastNanos = now;
    }

    private static String percent(long part, long whole) {
        return whole == 0 ? "-" : "%.1f%%".formatted(100.0 * part / whole);
    }
}
