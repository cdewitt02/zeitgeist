package headlinepulse.common;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Turns raw Jetstream JSON from {@code bsky.raw} into a {@link Post} or {@link Delete}.
 *
 * <p>Events that aren't a post create or delete (identity, account, updates, other collections) come
 * back empty. Malformed events throw. A single bad facet is skipped rather than failing the post.
 */
public final class PostParser {
    static final String COLLECTION = "app.bsky.feed.post";

    private static final ObjectMapper JSON = new ObjectMapper();

    public static Optional<BskyEvent> parse(String json) throws IOException {
        JsonNode root = JSON.readTree(json);
        if (root == null || !root.isObject()) {
            throw new IOException("Not a JSON object");
        }
        String did = root.path("did").asText("");
        long timeUs = root.path("time_us").asLong(-1);
        if (did.isEmpty() || timeUs <= 0) {
            throw new IOException("Missing did or time_us");
        }

        JsonNode commit = root.path("commit");
        if (!"commit".equals(root.path("kind").asText()) || !COLLECTION.equals(commit.path("collection").asText())) {
            return Optional.empty();
        }
        String rkey = commit.path("rkey").asText("");
        if (rkey.isEmpty()) {
            throw new IOException("Missing rkey");
        }
        String uri = "at://" + did + "/" + COLLECTION + "/" + rkey;
        Instant time = Instant.EPOCH.plusNanos(Math.multiplyExact(timeUs, 1000L));

        return switch (commit.path("operation").asText()) {
            case "create" -> Optional.of(parsePost(uri, did, time, commit.path("record")));
            case "delete" -> Optional.of(new Delete(uri, did, time));
            default -> Optional.empty();
        };
    }

    private static Post parsePost(String uri, String did, Instant time, JsonNode record) throws IOException {
        if (!record.isObject()) {
            throw new IOException("Create without a record");
        }
        String text = record.path("text").asText("");

        List<String> langs = new ArrayList<>();
        for (JsonNode lang : record.path("langs")) {
            if (lang.isTextual()) {
                langs.add(lang.asText());
            }
        }

        Set<String> links = new LinkedHashSet<>();
        List<Post.Span> linkSpans = new ArrayList<>();
        Set<String> hashtags = new LinkedHashSet<>();
        for (JsonNode facet : record.path("facets")) {
            for (JsonNode feature : facet.path("features")) {
                switch (feature.path("$type").asText()) {
                    case "app.bsky.richtext.facet#link" -> {
                        String link = feature.path("uri").asText("");
                        if (!link.isBlank()) {
                            links.add(link);
                            Post.Span span = span(text, facet.path("index"));
                            if (span != null) {
                                linkSpans.add(span);
                            }
                        }
                    }
                    case "app.bsky.richtext.facet#tag" -> addTag(hashtags, feature.path("tag").asText(""));
                    default -> {}
                }
            }
        }
        for (JsonNode tag : record.path("tags")) {
            addTag(hashtags, tag.asText(""));
        }

        Post.LinkCard card = linkCard(record.path("embed"));
        if (card != null) {
            links.add(card.url());
        }

        return new Post(uri, did, time, text, langs, List.copyOf(links), linkSpans, card, List.copyOf(hashtags));
    }

    /** The external embed, either on its own or as the media half of a quote post. */
    private static Post.LinkCard linkCard(JsonNode embed) {
        JsonNode external = switch (embed.path("$type").asText()) {
            case "app.bsky.embed.external" -> embed.path("external");
            case "app.bsky.embed.recordWithMedia" -> embed.path("media").path("external");
            default -> null;
        };
        if (external == null) {
            return null;
        }
        String url = external.path("uri").asText("");
        if (url.isBlank()) {
            return null;
        }
        return new Post.LinkCard(url, external.path("title").asText(""), external.path("description").asText(""));
    }

    private static void addTag(Set<String> hashtags, String tag) {
        String t = tag.strip();
        if (t.startsWith("#")) {
            t = t.substring(1);
        }
        if (!t.isEmpty()) {
            hashtags.add(t.toLowerCase(Locale.ROOT));
        }
    }

    /** Converts a facet's UTF-8 byte range to a {@code char} range, or {@code null} if it's invalid. */
    private static Post.Span span(String text, JsonNode index) {
        if (!index.path("byteStart").canConvertToInt() || !index.path("byteEnd").canConvertToInt()) {
            return null;
        }
        int start = charIndex(text, index.path("byteStart").asInt());
        int end = charIndex(text, index.path("byteEnd").asInt());
        return start >= 0 && end > start ? new Post.Span(start, end) : null;
    }

    /**
     * The {@code char} index in {@code text} at UTF-8 byte offset {@code byteOffset}, or −1 if the
     * offset is out of range or falls inside a character.
     */
    static int charIndex(String text, int byteOffset) {
        if (byteOffset < 0) {
            return -1;
        }
        int bytes = 0;
        int i = 0;
        while (bytes < byteOffset && i < text.length()) {
            int cp = text.codePointAt(i);
            bytes += utf8Length(cp);
            i += Character.charCount(cp);
        }
        return bytes == byteOffset ? i : -1;
    }

    private static int utf8Length(int cp) {
        if (cp < 0x80) return 1;
        if (cp < 0x800) return 2;
        if (cp < 0x10000) return 3;
        return 4;
    }

    private PostParser() {}
}
