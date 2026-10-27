package headlinepulse.common;

import java.util.List;

/**
 * A post that matched something worth tracking.
 *
 * @param urls canonical URLs of the articles it links on allowlisted news domains, in link order
 */
public record MatchedPost(Post post, List<String> urls) {
    public MatchedPost {
        urls = List.copyOf(urls);
    }
}
