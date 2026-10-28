package headlinepulse.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import headlinepulse.common.JsonSerde;
import headlinepulse.common.MatchedPost;
import headlinepulse.common.NewsDomains;
import headlinepulse.common.Post;
import headlinepulse.common.TopicMatcher;
import headlinepulse.common.Topics;

// All fixtures are synthetic.
class PipelineTopologyTest {
    private static final String DID = "did:plc:test123";
    private static final long TIME_US = 1792436000123456L;

    private final Stats stats = new Stats();
    private TopologyTestDriver driver;
    private TestInputTopic<String, String> raw;
    private TestOutputTopic<String, Post> posts;
    private TestOutputTopic<String, MatchedPost> matched;

    private static final TopicMatcher TOPICS = TopicMatcher.parse("""
            topics:
              - { id: race, hashtags: [synthrace], phrases: [{ text: "synthetic race" }] }
              - { id: race-candidate, parent: race, phrases: [{ text: "zorblax" }] }
            """);

    @BeforeEach
    void setUp() {
        start(0);
    }

    private void start(int samplePercent) {
        var matcher = new PostMatcher(NewsDomains.of(Map.of("example-news.com", "Example News")), TOPICS, samplePercent);
        driver = new TopologyTestDriver(PipelineTopology.build(stats, matcher));
        raw = driver.createInputTopic(Topics.BSKY_RAW, new StringSerializer(), new StringSerializer());
        posts = driver.createOutputTopic(Topics.BSKY_POSTS, new StringDeserializer(),
                new JsonSerde<>(Post.class).deserializer());
        matched = driver.createOutputTopic(Topics.POSTS_MATCHED, new StringDeserializer(),
                new JsonSerde<>(MatchedPost.class).deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    private static String uri(String rkey) {
        return "at://" + DID + "/app.bsky.feed.post/" + rkey;
    }

    private static String create(String rkey, long timeUs, String langs) {
        return """
                {"did":"%s","time_us":%d,"kind":"commit","commit":{"rev":"r1","operation":"create",\
                "collection":"app.bsky.feed.post","rkey":"%s",\
                "record":{"text":"Synthetic post %s","langs":%s},"cid":"c1"}}"""
                .formatted(DID, timeUs, rkey, rkey, langs);
    }

    private static String createWithText(String rkey, long timeUs, String text) {
        return """
                {"did":"%s","time_us":%d,"kind":"commit","commit":{"rev":"r1","operation":"create",\
                "collection":"app.bsky.feed.post","rkey":"%s",\
                "record":{"text":"%s","langs":["en"]},"cid":"c1"}}"""
                .formatted(DID, timeUs, rkey, text);
    }

    /** An English post whose text links {@code facetUrl} and whose link card is {@code cardUrl}. */
    private static String createWithLinks(String rkey, long timeUs, String facetUrl, String cardUrl) {
        return """
                {"did":"%s","time_us":%d,"kind":"commit","commit":{"rev":"r1","operation":"create",\
                "collection":"app.bsky.feed.post","rkey":"%s","record":{"text":"Synthetic link post","langs":["en"],\
                "facets":[{"index":{"byteStart":0,"byteEnd":9},\
                "features":[{"$type":"app.bsky.richtext.facet#link","uri":"%s"}]}],\
                "embed":{"$type":"app.bsky.embed.external","external":{"uri":"%s",\
                "title":"Synthetic headline","description":"Synthetic summary"}}},"cid":"c1"}}"""
                .formatted(DID, timeUs, rkey, facetUrl, cardUrl);
    }

    private static String delete(String rkey, long timeUs) {
        return """
                {"did":"%s","time_us":%d,"kind":"commit","commit":{"rev":"r2","operation":"delete",\
                "collection":"app.bsky.feed.post","rkey":"%s"}}""".formatted(DID, timeUs, rkey);
    }

    /** Sends like the poller does: keyed by DID, timestamped with {@code time_us}. */
    private void send(String json, long timeUs) {
        raw.pipeInput(DID, json, Instant.EPOCH.plus(Duration.ofNanos(timeUs * 1000)));
    }

    @Test
    void englishPostIsKeyedByUriWithItsEventTime() {
        send(create("3kaaa", TIME_US, "[\"en\"]"), TIME_US);

        TestRecord<String, Post> out = posts.readRecord();
        assertEquals(uri("3kaaa"), out.key());
        assertEquals("Synthetic post 3kaaa", out.value().text());
        assertEquals(Instant.parse("2026-10-19T18:53:20.123456Z"), out.value().time());
        assertEquals(Instant.ofEpochMilli(TIME_US / 1000), out.getRecordTime());
        assertTrue(posts.isEmpty());
    }

    @Test
    void keepsOnlyDeclaredEnglish() {
        send(create("3kaaa", TIME_US, "[\"en-US\"]"), TIME_US);
        send(create("3kbbb", TIME_US + 1, "[\"ja\",\"EN\"]"), TIME_US + 1);
        send(create("3kccc", TIME_US + 2, "[\"es\"]"), TIME_US + 2);
        send(create("3kddd", TIME_US + 3, "[]"), TIME_US + 3);
        send(create("3keee", TIME_US + 4, "[\"eng\"]"), TIME_US + 4);

        assertEquals(List.of(uri("3kaaa"), uri("3kbbb")), posts.readKeyValuesToList().stream().map(kv -> kv.key).toList());
        assertEquals(5, stats.posts.sum());
        assertEquals(2, stats.english.sum());
    }

    @Test
    void dropsDuplicatesFromACursorRewind() {
        // The poller restarts and replays the last 5 seconds: same events, same time_us.
        send(create("3kaaa", TIME_US, "[\"en\"]"), TIME_US);
        send(create("3kbbb", TIME_US + 1_000_000, "[\"en\"]"), TIME_US + 1_000_000);
        send(create("3kaaa", TIME_US, "[\"en\"]"), TIME_US);
        send(create("3kbbb", TIME_US + 1_000_000, "[\"en\"]"), TIME_US + 1_000_000);
        send(create("3kccc", TIME_US + 2_000_000, "[\"en\"]"), TIME_US + 2_000_000);

        assertEquals(List.of(uri("3kaaa"), uri("3kbbb"), uri("3kccc")),
                posts.readKeyValuesToList().stream().map(kv -> kv.key).toList());
        assertEquals(2, stats.duplicates.sum());
    }

    @Test
    void sameUriOutsideTheWindowPassesAgain() {
        long later = TIME_US + Normalizer.DEDUPE_WINDOW.plusMinutes(1).toNanos() / 1000;
        send(create("3kaaa", TIME_US, "[\"en\"]"), TIME_US);
        send(create("3kaaa", later, "[\"en\"]"), later);

        assertEquals(2, posts.readKeyValuesToList().size());
        assertEquals(0, stats.duplicates.sum());
    }

    @Test
    void deleteBecomesATombstone() {
        send(create("3kaaa", TIME_US, "[\"en\"]"), TIME_US);
        send(delete("3kaaa", TIME_US + 5_000_000), TIME_US + 5_000_000);
        send(delete("3kaaa", TIME_US + 5_000_000), TIME_US + 5_000_000);

        var out = posts.readKeyValuesToList();
        assertEquals(2, out.size());
        assertEquals(uri("3kaaa"), out.get(0).key);
        assertEquals(uri("3kaaa"), out.get(1).key);
        assertNull(out.get(1).value);
        assertEquals(1, stats.duplicates.sum());
    }

    @Test
    void unparseableAndIrrelevantEventsAreCountedNotFatal() {
        send("not json", TIME_US);
        send("{\"kind\":\"commit\"}", TIME_US);
        send("{\"did\":\"%s\",\"time_us\":%d,\"kind\":\"identity\"}".formatted(DID, TIME_US), TIME_US);
        raw.pipeInput(DID, (String) null);
        send(create("3kaaa", TIME_US, "[\"en\"]"), TIME_US);

        assertEquals(List.of(uri("3kaaa")), posts.readKeyValuesToList().stream().map(kv -> kv.key).toList());
        assertEquals(3, stats.unparseable.sum());
        assertEquals(1, stats.ignored.sum());
    }

    @Test
    void postLinkingAnAllowlistedArticleIsMatchedWithCanonicalUrls() {
        send(createWithLinks("3kaaa", TIME_US, "https://www.example-news.com/politics/story-one/?utm_source=bsky",
                "https://politics.example-news.com/amp/story-two#top"), TIME_US);

        TestRecord<String, MatchedPost> out = matched.readRecord();
        assertEquals(uri("3kaaa"), out.key());
        assertEquals(List.of("https://example-news.com/politics/story-one", "https://politics.example-news.com/story-two"),
                out.value().urls());
        assertEquals("Synthetic link post", out.value().post().text());
        assertEquals(List.of(), out.value().topics());
        assertFalse(out.value().sample());
        assertEquals("Synthetic headline", out.value().post().card().title());
        assertEquals(Instant.ofEpochMilli(TIME_US / 1000), out.getRecordTime());
        assertTrue(matched.isEmpty());
        assertEquals(1, stats.matched.sum());
    }

    @Test
    void sameArticleLinkedTwiceIsListedOnce() {
        send(createWithLinks("3kaaa", TIME_US, "https://example-news.com/story?fbclid=1",
                "https://m.example-news.com/story/"), TIME_US);

        assertEquals(List.of("https://example-news.com/story"), matched.readValue().urls());
    }

    @Test
    void shareTrackingQueryIsDroppedFromNewsUrls() {
        send(createWithLinks("3kaaa", TIME_US, "https://example-news.com/article/story-123.php?hash=abc&rid=def&sharecount=2",
                "https://example-news.com/article/story-123.php?id=9"), TIME_US);

        assertEquals(List.of("https://example-news.com/article/story-123.php"), matched.readValue().urls());
    }

    @Test
    void postsWithoutAnAllowlistedArticleAreNotMatched() {
        send(create("3kaaa", TIME_US, "[\"en\"]"), TIME_US);
        send(createWithLinks("3kbbb", TIME_US + 1, "https://other.example/story", "https://notexample-news.com/story"),
                TIME_US + 1);
        // A link to the outlet's home page isn't a story.
        send(createWithLinks("3kccc", TIME_US + 2, "https://www.example-news.com/", "https://example-news.com"),
                TIME_US + 2);

        assertTrue(matched.isEmpty());
        assertEquals(3, posts.readKeyValuesToList().size());
        assertEquals(0, stats.matched.sum());
    }

    @Test
    void onlyDeletesOfMatchedPostsAreTombstonesInPostsMatched() {
        send(createWithLinks("3kaaa", TIME_US, "https://example-news.com/story", "https://example-news.com/story"),
                TIME_US);
        send(create("3kbbb", TIME_US + 1, "[\"en\"]"), TIME_US + 1);
        send(delete("3kaaa", TIME_US + 5_000_000), TIME_US + 5_000_000);
        send(delete("3kbbb", TIME_US + 6_000_000), TIME_US + 6_000_000);
        send(delete("3kzzz", TIME_US + 7_000_000), TIME_US + 7_000_000);

        var out = matched.readKeyValuesToList();
        assertEquals(List.of(uri("3kaaa"), uri("3kaaa")), out.stream().map(kv -> kv.key).toList());
        assertNull(out.get(1).value);
        assertEquals(1, stats.matchedDeletes.sum());
        // bsky.posts still gets every delete.
        assertEquals(3, posts.readKeyValuesToList().stream().filter(kv -> kv.value == null).count());
    }

    @Test
    void deleteOfAPostMatchedLongAgoIsNotPassedOn() {
        long later = TIME_US + MatchProcessor.REMEMBER.plusHours(1).toNanos() / 1000;
        send(createWithLinks("3kaaa", TIME_US, "https://example-news.com/story", "https://example-news.com/story"),
                TIME_US);
        send(delete("3kaaa", later), later);

        assertEquals(1, matched.readKeyValuesToList().size());
    }

    @Test
    void needsNoRepartitionTopic() {
        String description = PipelineTopology.build(new Stats(), new PostMatcher(NewsDomains.of(Map.of()), TOPICS, 10)).describe().toString();
        assertFalse(description.contains("repartition"), description);
    }

    @Test
    void topicMatchesIncludeTheParent() {
        send(createWithText("3kaaa", TIME_US, "Zorblax gave a speech"), TIME_US);

        MatchedPost out = matched.readValue();
        assertEquals(List.of("race", "race-candidate"), out.topics());
        assertEquals(List.of(), out.urls());
        assertEquals(1, stats.topicMatched.sum());
        assertEquals(0, stats.urlMatched.sum());
    }

    @Test
    void sampledPostsAreWrittenEvenWithoutAMatch() {
        driver.close();
        start(100);
        send(createWithText("3kaaa", TIME_US, "Nothing to see here"), TIME_US);
        send(delete("3kaaa", TIME_US + 1_000_000), TIME_US + 1_000_000);

        var out = matched.readKeyValuesToList();
        assertEquals(2, out.size());
        assertTrue(out.get(0).value.sample());
        assertEquals(List.of(), out.get(0).value.topics());
        assertNull(out.get(1).value);
        assertEquals(1, stats.sampled.sum());
    }
}
