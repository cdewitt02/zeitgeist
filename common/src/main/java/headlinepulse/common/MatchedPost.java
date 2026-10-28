package headlinepulse.common;

import java.util.List;

/**
 * A post worth scoring: it links a tracked article, matches a topic, or is in the baseline sample.
 *
 * @param urls   canonical URLs of the articles it links on allowlisted news domains, in link order
 * @param topics ids of the topics it matches, including the parents of matched topics, in config order
 * @param sample whether it's in the baseline sample of all English posts, matched or not
 */
public record MatchedPost(Post post, List<String> urls, List<String> topics, boolean sample) {
    public MatchedPost {
        urls = List.copyOf(urls);
        topics = List.copyOf(topics);
    }
}
