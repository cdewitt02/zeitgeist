package headlinepulse.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;

import org.junit.jupiter.api.Test;

// All fixtures are synthetic.
class PostParserTest {
    private static final String DID = "did:plc:test123";
    private static final String URI = "at://did:plc:test123/app.bsky.feed.post/3kabc";

    /** A Jetstream create event wrapping {@code record}. */
    private static String create(String record) {
        return """
                {"did":"%s","time_us":1792436000123456,"kind":"commit",\
                "commit":{"rev":"r1","operation":"create","collection":"app.bsky.feed.post","rkey":"3kabc",\
                "record":%s,"cid":"c1"}}""".formatted(DID, record);
    }

    private static Post parsePost(String json) throws IOException {
        return assertInstanceOf(Post.class, PostParser.parse(json).orElseThrow());
    }

    @Test
    void plainPost() throws IOException {
        Post p = parsePost(create("""
                {"$type":"app.bsky.feed.post","text":"Synthetic post about nothing","langs":["en"],\
                "createdAt":"2026-10-19T19:33:20.000Z"}"""));

        assertEquals(URI, p.uri());
        assertEquals(DID, p.did());
        assertEquals(Instant.parse("2026-10-19T18:53:20.123456Z"), p.time());
        assertEquals("Synthetic post about nothing", p.text());
        assertEquals(List.of("en"), p.langs());
        assertEquals(List.of(), p.links());
        assertEquals(List.of(), p.linkSpans());
        assertNull(p.card());
        assertEquals(List.of(), p.hashtags());
    }

    @Test
    void linkFacetAndLinkCard() throws IOException {
        // The visible text is truncated; the facet has the full URL.
        Post p = parsePost(create("""
                {"text":"Read this: example.com/news/stor...","langs":["en"],\
                "facets":[{"index":{"byteStart":11,"byteEnd":35},\
                "features":[{"$type":"app.bsky.richtext.facet#link","uri":"https://example.com/news/story-one?utm_source=x"}]}],\
                "embed":{"$type":"app.bsky.embed.external","external":{"uri":"https://example.com/news/story-two",\
                "title":"Synthetic headline","description":"Synthetic summary"}}}"""));

        assertEquals(List.of("https://example.com/news/story-one?utm_source=x", "https://example.com/news/story-two"),
                p.links());
        assertEquals(List.of(new Post.Span(11, 35)), p.linkSpans());
        assertEquals("example.com/news/stor...", p.text().substring(11, 35));
        assertEquals(new Post.LinkCard("https://example.com/news/story-two", "Synthetic headline", "Synthetic summary"),
                p.card());
    }

    @Test
    void sameUrlInFacetAndCardListedOnce() throws IOException {
        Post p = parsePost(create("""
                {"text":"example.com/a","facets":[{"index":{"byteStart":0,"byteEnd":13},\
                "features":[{"$type":"app.bsky.richtext.facet#link","uri":"https://example.com/a"}]}],\
                "embed":{"$type":"app.bsky.embed.external","external":{"uri":"https://example.com/a"}}}"""));

        assertEquals(List.of("https://example.com/a"), p.links());
        assertEquals(new Post.LinkCard("https://example.com/a", "", ""), p.card());
    }

    @Test
    void linkCardInsideQuotePost() throws IOException {
        Post p = parsePost(create("""
                {"text":"quoting","embed":{"$type":"app.bsky.embed.recordWithMedia",\
                "record":{"$type":"app.bsky.embed.record","record":{"uri":"at://did:plc:other/app.bsky.feed.post/1"}},\
                "media":{"$type":"app.bsky.embed.external","external":{"uri":"https://example.com/b","title":"T"}}}}"""));

        assertEquals(List.of("https://example.com/b"), p.links());
        assertEquals("T", p.card().title());
    }

    @Test
    void hashtagsFromFacetsAndTagsFieldLowercasedAndDistinct() throws IOException {
        Post p = parsePost(create("""
                {"text":"Go vote #TXSen #txlege","langs":["en"],\
                "facets":[\
                {"index":{"byteStart":8,"byteEnd":14},"features":[{"$type":"app.bsky.richtext.facet#tag","tag":"TXSen"}]},\
                {"index":{"byteStart":15,"byteEnd":22},"features":[{"$type":"app.bsky.richtext.facet#tag","tag":"txlege"}]}],\
                "tags":["txsen","#TxVotes"]}"""));

        assertEquals(List.of("txsen", "txlege", "txvotes"), p.hashtags());
        assertEquals(List.of(), p.links());
    }

    @Test
    void deleteEvent() throws IOException {
        BskyEvent e = PostParser.parse("""
                {"did":"%s","time_us":1792436000123456,"kind":"commit",\
                "commit":{"rev":"r2","operation":"delete","collection":"app.bsky.feed.post","rkey":"3kabc"}}"""
                .formatted(DID)).orElseThrow();

        assertEquals(new Delete(URI, DID, Instant.parse("2026-10-19T18:53:20.123456Z")), e);
    }

    @Test
    void emojiBeforeLinkUsesByteOffsets() throws IOException {
        // 🗳️ is U+1F5F3 (4 bytes, 2 chars) and U+FE0F (3 bytes, 1 char); é is 2 bytes, 1 char.
        String text = "🗳️ Café vote: example.com/vote";
        int byteStart = text.substring(0, text.indexOf("example")).getBytes(StandardCharsets.UTF_8).length;
        int byteEnd = text.getBytes(StandardCharsets.UTF_8).length;
        assertEquals(20, byteStart);

        Post p = parsePost(create("""
                {"text":"%s","facets":[{"index":{"byteStart":%d,"byteEnd":%d},\
                "features":[{"$type":"app.bsky.richtext.facet#link","uri":"https://example.com/vote"}]}]}"""
                .formatted(text, byteStart, byteEnd)));

        Post.Span span = p.linkSpans().get(0);
        assertEquals("example.com/vote", p.text().substring(span.start(), span.end()));
        assertEquals(new Post.Span(15, 31), span);
    }

    @Test
    void badFacetOffsetsDropTheSpanButKeepTheLink() throws IOException {
        // byteEnd 2 falls inside the 4-byte emoji; byteEnd 99 is past the end.
        Post p = parsePost(create("""
                {"text":"🎉 x","facets":[\
                {"index":{"byteStart":0,"byteEnd":2},"features":[{"$type":"app.bsky.richtext.facet#link","uri":"https://a.example"}]},\
                {"index":{"byteStart":5,"byteEnd":99},"features":[{"$type":"app.bsky.richtext.facet#link","uri":"https://b.example"}]},\
                {"features":[{"$type":"app.bsky.richtext.facet#link","uri":"https://c.example"}]}]}"""));

        assertEquals(List.of("https://a.example", "https://b.example", "https://c.example"), p.links());
        assertEquals(List.of(), p.linkSpans());
    }

    @Test
    void charIndexConvertsUtf8Offsets() {
        String text = "a🎉é";  // bytes: a=1, 🎉=4, é=2
        assertEquals(0, PostParser.charIndex(text, 0));
        assertEquals(1, PostParser.charIndex(text, 1));
        assertEquals(3, PostParser.charIndex(text, 5));
        assertEquals(4, PostParser.charIndex(text, 7));
        assertEquals(-1, PostParser.charIndex(text, 3));
        assertEquals(-1, PostParser.charIndex(text, 6));
        assertEquals(-1, PostParser.charIndex(text, 8));
        assertEquals(-1, PostParser.charIndex(text, -1));
    }

    @Test
    void ignoresEventsThatAreNotPostCreatesOrDeletes() throws IOException {
        assertTrue(PostParser.parse("""
                {"did":"did:plc:x","time_us":1792436000000002,"kind":"identity","identity":{"handle":"a.test"}}""")
                .isEmpty());
        assertTrue(PostParser.parse("""
                {"did":"did:plc:x","time_us":1792436000000002,"kind":"commit",\
                "commit":{"operation":"create","collection":"app.bsky.feed.like","rkey":"k","record":{}}}""")
                .isEmpty());
        assertTrue(PostParser.parse("""
                {"did":"did:plc:x","time_us":1792436000000002,"kind":"commit",\
                "commit":{"operation":"update","collection":"app.bsky.feed.post","rkey":"k","record":{"text":"x"}}}""")
                .isEmpty());
    }

    @Test
    void rejectsMalformedEvents() {
        assertThrows(IOException.class, () -> PostParser.parse("{\"did\":"));
        assertThrows(IOException.class, () -> PostParser.parse("[1,2]"));
        assertThrows(IOException.class, () -> PostParser.parse("{\"time_us\":1792436000000000}"));
        assertThrows(IOException.class, () -> PostParser.parse("""
                {"did":"did:plc:x","time_us":1792436000000002,"kind":"commit",\
                "commit":{"operation":"create","collection":"app.bsky.feed.post","rkey":"k"}}"""));
        assertThrows(IOException.class, () -> PostParser.parse("""
                {"did":"did:plc:x","time_us":1792436000000002,"kind":"commit",\
                "commit":{"operation":"delete","collection":"app.bsky.feed.post"}}"""));
    }
}
