package headlinepulse.common;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.yaml.snakeyaml.Yaml;

/**
 * Matches posts to the races and issues in {@code config/topics.yaml}.
 *
 * <p>A topic matches if the post has one of its hashtags, or one of its phrases appears in the post
 * text or the link card's title or description. Matching ignores case and needs whole words, though
 * a plural {@code s} or {@code es} is allowed: {@code paxton} matches "Paxton's" but not
 * "Paxtonville", and {@code school voucher} matches "school vouchers". A phrase can also require that one of its
 * {@code requireAny} words appears anywhere in the post, and can be cancelled by an {@code exclude}
 * phrase that contains it ("abbott elementary" cancels that "abbott", not one elsewhere in the post).
 * A match on a topic also counts for its {@code parent}, and the parent's parent.
 *
 * <p>Each post is checked against every phrase in a plain loop, which is fast enough at Bluesky's
 * volume for the few hundred phrases this needs.
 */
public final class TopicMatcher {
    private record Config(List<String> context, List<TopicConfig> topics) {}

    private record TopicConfig(String id, String label, String parent, List<String> hashtags, List<PhraseConfig> phrases) {}

    private record PhraseConfig(String text, List<String> requireAny, List<String> exclude) {}

    public record Topic(String id, String label, String parent, Set<String> hashtags, List<Phrase> phrases) {}

    /** A phrase as matched: normalized like post text. */
    public record Phrase(String text, List<String> requireAny, List<String> exclude) {}

    /** Separates the parts of a post, so a phrase can't run from the end of one into the next. */
    private static final String PART_SEPARATOR = " | ";
    private static final Pattern WHITESPACE = Pattern.compile("\\s+");
    private static final Pattern HASHTAG = Pattern.compile("[\\p{L}\\p{N}_]+");

    private final Map<String, Topic> topics;

    private TopicMatcher(Map<String, Topic> topics) {
        this.topics = topics;
    }

    public static TopicMatcher load(Path file) throws IOException {
        try {
            return parse(Files.readString(file));
        } catch (IllegalArgumentException e) {
            throw new IOException(file + ": " + e.getMessage(), e);
        }
    }

    /** Parses YAML in the {@code topics.yaml} format. Throws {@link IllegalArgumentException} if it's invalid. */
    public static TopicMatcher parse(String yaml) {
        Object tree = new Yaml().load(yaml);
        Config config = new ObjectMapper().convertValue(tree, Config.class);
        if (config == null || config.topics() == null || config.topics().isEmpty()) {
            throw new IllegalArgumentException("No topics");
        }

        Map<String, Topic> topics = new LinkedHashMap<>();
        for (TopicConfig t : config.topics()) {
            if (t.id() == null || t.id().isBlank()) {
                throw new IllegalArgumentException("A topic has no id");
            }
            Set<String> hashtags = new LinkedHashSet<>();
            for (String tag : orEmpty(t.hashtags())) {
                if (!HASHTAG.matcher(tag).matches() || !tag.equals(tag.toLowerCase(Locale.ROOT))) {
                    throw new IllegalArgumentException(t.id() + ": hashtag '" + tag + "' must be lowercase, without #");
                }
                hashtags.add(tag);
            }
            List<Phrase> phrases = new ArrayList<>();
            for (PhraseConfig p : orEmpty(t.phrases())) {
                if (p.text() == null || normalize(p.text()).isBlank()) {
                    throw new IllegalArgumentException(t.id() + ": a phrase has no text");
                }
                phrases.add(new Phrase(normalize(p.text()),
                        orEmpty(p.requireAny()).stream().map(TopicMatcher::normalize).toList(),
                        orEmpty(p.exclude()).stream().map(TopicMatcher::normalize).toList()));
            }
            if (hashtags.isEmpty() && phrases.isEmpty()) {
                throw new IllegalArgumentException(t.id() + ": needs hashtags or phrases");
            }
            var topic = new Topic(t.id(), t.label() == null ? t.id() : t.label(), t.parent(),
                    Set.copyOf(hashtags), List.copyOf(phrases));
            if (topics.putIfAbsent(t.id(), topic) != null) {
                throw new IllegalArgumentException("Topic '" + t.id() + "' is listed twice");
            }
        }

        for (Topic topic : topics.values()) {
            Set<String> seen = new HashSet<>();
            for (Topic t = topic; t.parent() != null; t = topics.get(t.parent())) {
                if (!topics.containsKey(t.parent())) {
                    throw new IllegalArgumentException(t.id() + ": unknown parent '" + t.parent() + "'");
                }
                if (!seen.add(t.id())) {
                    throw new IllegalArgumentException(topic.id() + ": parents form a cycle");
                }
            }
        }
        // Not Map.copyOf, which would lose config order.
        return new TopicMatcher(Collections.unmodifiableMap(topics));
    }

    public List<Topic> topics() {
        return List.copyOf(topics.values());
    }

    public Optional<Topic> topic(String id) {
        return Optional.ofNullable(topics.get(id));
    }

    /** Ids of every topic the post matches, with their parents, in config order. */
    public List<String> match(Post post) {
        Set<String> matched = new HashSet<>();
        for (String id : reasons(post).keySet()) {
            for (Topic t = topics.get(id); t != null; t = t.parent() == null ? null : topics.get(t.parent())) {
                matched.add(t.id());
            }
        }
        return topics.keySet().stream().filter(matched::contains).toList();
    }

    /**
     * For each topic the post matches directly (not through a child), what matched it: {@code #tag} or
     * the phrase text. For reviewing matches by hand.
     */
    public Map<String, String> reasons(Post post) {
        String haystack = haystack(post);
        Map<String, String> reasons = new LinkedHashMap<>();
        for (Topic topic : topics.values()) {
            String reason = reason(topic, post.hashtags(), haystack);
            if (reason != null) {
                reasons.put(topic.id(), reason);
            }
        }
        return reasons;
    }

    private static String reason(Topic topic, List<String> postTags, String haystack) {
        for (String tag : postTags) {
            if (topic.hashtags().contains(tag)) {
                return "#" + tag;
            }
        }
        for (Phrase phrase : topic.phrases()) {
            if (matches(phrase, haystack)) {
                return phrase.text();
            }
        }
        return null;
    }

    private static boolean matches(Phrase phrase, String haystack) {
        boolean found = false;
        for (int start : occurrences(haystack, phrase.text())) {
            if (!excluded(phrase, haystack, start)) {
                found = true;
                break;
            }
        }
        if (!found) {
            return false;
        }
        if (phrase.requireAny().isEmpty()) {
            return true;
        }
        for (String word : phrase.requireAny()) {
            if (!occurrences(haystack, word).isEmpty()) {
                return true;
            }
        }
        return false;
    }

    /** True if the occurrence of the phrase at {@code start} sits inside one of its exclude phrases. */
    private static boolean excluded(Phrase phrase, String haystack, int start) {
        int end = start + phrase.text().length();
        for (String exclude : phrase.exclude()) {
            for (int s : occurrences(haystack, exclude)) {
                if (s <= start && end <= s + exclude.length()) {
                    return true;
                }
            }
        }
        return false;
    }

    /** True if a word ends at {@code end}, allowing a plural {@code s} or {@code es}. */
    private static boolean endsWord(String s, int end) {
        return isBoundary(s, end, false)
                || (s.startsWith("s", end) && isBoundary(s, end + 1, false))
                || (s.startsWith("es", end) && isBoundary(s, end + 2, false));
    }

    /** Start indices of {@code word} in {@code haystack} as a whole word or phrase, or its plural. */
    static List<Integer> occurrences(String haystack, String word) {
        List<Integer> starts = new ArrayList<>();
        for (int i = haystack.indexOf(word); i >= 0; i = haystack.indexOf(word, i + 1)) {
            int end = i + word.length();
            if (isBoundary(haystack, i - 1, true) && endsWord(haystack, end)) {
                starts.add(i);
            }
        }
        return starts;
    }

    /** True if the character at {@code i} (before or after a match) doesn't continue a word. */
    private static boolean isBoundary(String s, int i, boolean before) {
        if (i < 0 || i >= s.length()) {
            return true;
        }
        int cp = before ? s.codePointBefore(i + 1) : s.codePointAt(i);
        return !Character.isLetterOrDigit(cp);
    }

    /** Post text, link card title and description, and hashtags, normalized for matching. */
    private static String haystack(Post post) {
        var parts = new ArrayList<String>();
        parts.add(post.text());
        if (post.card() != null) {
            parts.add(post.card().title());
            parts.add(post.card().description());
        }
        // Tags from the post's tags field aren't in the visible text, but they're still context.
        parts.add(String.join(" ", post.hashtags()));
        return normalize(String.join(PART_SEPARATOR, parts));
    }

    /** Lowercase, straight quotes, single spaces. */
    static String normalize(String s) {
        String t = s.toLowerCase(Locale.ROOT)
                .replace('’', '\'').replace('‘', '\'')
                .replace('“', '"').replace('”', '"');
        return WHITESPACE.matcher(t).replaceAll(" ").strip();
    }

    private static <T> List<T> orEmpty(List<T> list) {
        return list == null ? List.of() : list;
    }
}
