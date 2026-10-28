package headlinepulse.common;

import java.time.Instant;

/**
 * An article from an outlet's RSS feed.
 *
 * @param url       the article key from {@link NewsDomains#articleUrl}, the same one posts linking it get
 * @param outlet    the outlet's name, from {@code config/feeds.yaml}
 * @param published when the feed says it was published, or {@code null} if it doesn't say
 * @param seen      when the headlines app first saw it in a feed
 * @param feed      the feed it came from
 */
public record Headline(String url, String outlet, String title, Instant published, Instant seen, String feed) {}
