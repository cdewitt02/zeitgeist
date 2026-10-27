package headlinepulse.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

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
import headlinepulse.common.Post;
import headlinepulse.common.Topics;

// All fixtures are synthetic.
class NormalizeTopologyTest {
    private static final String DID = "did:plc:test123";
    private static final long TIME_US = 1792436000123456L;

    private final Stats stats = new Stats();
    private TopologyTestDriver driver;
    private TestInputTopic<String, String> raw;
    private TestOutputTopic<String, Post> posts;

    @BeforeEach
    void setUp() {
        driver = new TopologyTestDriver(NormalizeTopology.build(stats));
        raw = driver.createInputTopic(Topics.BSKY_RAW, new StringSerializer(), new StringSerializer());
        posts = driver.createOutputTopic(Topics.BSKY_POSTS, new StringDeserializer(),
                new JsonSerde<>(Post.class).deserializer());
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
}
