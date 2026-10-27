package headlinepulse.pipeline;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import headlinepulse.common.MatchedPost;
import headlinepulse.common.NewsDomains;
import headlinepulse.common.Post;
import headlinepulse.common.UrlCanonicalizer;

/** Decides whether a post is worth tracking, and why. */
final class PostMatcher {
    private final NewsDomains newsDomains;

    PostMatcher(NewsDomains newsDomains) {
        this.newsDomains = newsDomains;
    }

    /** The post with every allowlisted article it links, or empty if it links none. */
    Optional<MatchedPost> match(Post post) {
        Set<String> urls = new LinkedHashSet<>();
        for (String link : post.links()) {
            UrlCanonicalizer.canonicalize(link)
                    .filter(PostMatcher::isArticle)
                    .filter(url -> newsDomains.outlet(url).isPresent())
                    .map(PostMatcher::withoutQuery)
                    .ifPresent(urls::add);
        }
        return urls.isEmpty() ? Optional.empty() : Optional.of(new MatchedPost(post, List.copyOf(urls)));
    }

    /**
     * The allowlisted outlets identify articles by path, so whatever query remains is share and
     * newsletter tracking ({@code ?hash=...&rid=...&sharecount=...}) that would split one article's
     * count across many keys.
     */
    private static String withoutQuery(String canonicalUrl) {
        int q = canonicalUrl.indexOf('?');
        return q < 0 ? canonicalUrl : canonicalUrl.substring(0, q);
    }

    /** A link to an outlet's home page says nothing about a story, so it needs a path. */
    private static boolean isArticle(String canonicalUrl) {
        return canonicalUrl.indexOf('/', "https://".length()) >= 0;
    }
}
