package headlinepulse.pipeline;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.zip.CRC32;

import headlinepulse.common.MatchedPost;
import headlinepulse.common.NewsDomains;
import headlinepulse.common.Post;
import headlinepulse.common.TopicMatcher;

/** Decides whether a post is worth tracking, and why. */
final class PostMatcher {
    /** Share of all English posts kept as the baseline, matched or not. */
    static final int SAMPLE_PERCENT = 10;

    private final NewsDomains newsDomains;
    private final TopicMatcher topicMatcher;
    private final int samplePercent;

    PostMatcher(NewsDomains newsDomains, TopicMatcher topicMatcher, int samplePercent) {
        this.newsDomains = newsDomains;
        this.topicMatcher = topicMatcher;
        this.samplePercent = samplePercent;
    }

    /**
     * The post with every allowlisted article it links and every topic it matches, or empty if it has
     * neither and isn't in the baseline sample.
     */
    Optional<MatchedPost> match(Post post) {
        Set<String> urls = new LinkedHashSet<>();
        for (String link : post.links()) {
            newsDomains.articleUrl(link).ifPresent(urls::add);
        }
        List<String> topics = topicMatcher.match(post);
        boolean sample = inSample(post.uri(), samplePercent);
        if (urls.isEmpty() && topics.isEmpty() && !sample) {
            return Optional.empty();
        }
        return Optional.of(new MatchedPost(post, List.copyOf(urls), topics, sample));
    }

    /** Picks by a hash of the URI, so a post is in or out of the sample the same way on every replay. */
    static boolean inSample(String uri, int percent) {
        var crc = new CRC32();
        crc.update(uri.getBytes(StandardCharsets.UTF_8));
        return crc.getValue() % 100 < percent;
    }
}
