package headlinepulse.common;

import java.time.Instant;
import java.util.List;

/**
 * A new Bluesky post, reduced to what matching and scoring need.
 *
 * @param langs     the author's declared languages, as given (often empty)
 * @param links     distinct link URLs from link facets, then the link card, in that order. Read from
 *                  the record, not the visible text, which is often truncated.
 * @param linkSpans where each link facet sits in {@link #text()}, as UTF-16 {@code char} indices,
 *                  so the scorer can replace the visible link text
 * @param card      the external embed (link card), or {@code null}
 * @param hashtags  distinct lowercase tags without {@code #}, from tag facets and the post's
 *                  {@code tags} field
 */
public record Post(
        String uri,
        String did,
        Instant time,
        String text,
        List<String> langs,
        List<String> links,
        List<Span> linkSpans,
        LinkCard card,
        List<String> hashtags) implements BskyEvent {

    public Post {
        langs = List.copyOf(langs);
        links = List.copyOf(links);
        linkSpans = List.copyOf(linkSpans);
        hashtags = List.copyOf(hashtags);
    }

    /** A link card. {@code title} and {@code description} are empty when the card has none. */
    public record LinkCard(String url, String title, String description) {}

    /** A half-open range {@code [start, end)} of {@code char} indices into the post text. */
    public record Span(int start, int end) {}
}
