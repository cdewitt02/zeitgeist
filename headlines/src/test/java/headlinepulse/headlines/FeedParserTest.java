package headlinepulse.headlines;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import com.rometools.rome.io.FeedException;
import org.junit.jupiter.api.Test;

import headlinepulse.common.Headline;
import headlinepulse.common.NewsDomains;

// All fixtures are synthetic.
class FeedParserTest {
    private static final NewsDomains DOMAINS = NewsDomains.of(Map.of("example-news.com", "Example News"));
    private static final Feed FEED = new Feed("Example News", "https://example-news.com/feed/");
    private static final Instant NOW = Instant.parse("2026-10-27T12:00:00Z");

    private static FeedParser.Parsed parse(String xml) throws Exception {
        return FeedParser.parse(xml.getBytes(StandardCharsets.UTF_8), FEED, DOMAINS, NOW);
    }

    @Test
    void rssItemsBecomeHeadlinesKeyedLikePostLinks() throws Exception {
        FeedParser.Parsed parsed = parse("""
                <?xml version="1.0" encoding="UTF-8"?>
                <rss version="2.0"><channel><title>Example News</title><link>https://example-news.com/</link>
                  <item><title>  Synthetic   headline &amp; more </title>
                    <link>https://www.example-news.com/politics/story-one/?utm_source=rss</link>
                    <pubDate>Tue, 27 Oct 2026 10:30:00 GMT</pubDate></item>
                  <item><title>Second synthetic headline</title>
                    <link>https://example-news.com/politics/story-two</link></item>
                </channel></rss>""");

        assertEquals(List.of(
                new Headline("https://example-news.com/politics/story-one", "Example News", "Synthetic headline & more",
                        Instant.parse("2026-10-27T10:30:00Z"), NOW, FEED.url()),
                new Headline("https://example-news.com/politics/story-two", "Example News", "Second synthetic headline",
                        null, NOW, FEED.url())),
                parsed.headlines());
        assertEquals(0, parsed.skipped());
    }

    @Test
    void skipsHomePagesOtherDomainsUntitledItemsAndRepeats() throws Exception {
        FeedParser.Parsed parsed = parse("""
                <rss version="2.0"><channel><title>Example News</title><link>https://example-news.com/</link>
                  <item><title>Kept</title><link>https://example-news.com/story</link></item>
                  <item><title>Same story, tracked link</title><link>https://example-news.com/story?fbclid=1</link></item>
                  <item><title>Home page</title><link>https://example-news.com/</link></item>
                  <item><title>Elsewhere</title><link>https://other.example/story</link></item>
                  <item><title></title><link>https://example-news.com/untitled</link></item>
                  <item><title>No link</title></item>
                </channel></rss>""");

        assertEquals(List.of("https://example-news.com/story"), parsed.headlines().stream().map(Headline::url).toList());
        assertEquals("Kept", parsed.headlines().getFirst().title());
        assertEquals(4, parsed.skipped());
    }

    @Test
    void atomFeedsWork() throws Exception {
        FeedParser.Parsed parsed = parse("""
                <?xml version="1.0" encoding="utf-8"?>
                <feed xmlns="http://www.w3.org/2005/Atom"><title>Example News</title>
                  <id>urn:example</id><updated>2026-10-27T11:00:00Z</updated>
                  <entry><title>Synthetic atom headline</title><id>urn:example:1</id>
                    <link href="https://example-news.com/atom-story"/>
                    <updated>2026-10-27T11:00:00Z</updated></entry>
                </feed>""");

        Headline h = parsed.headlines().getFirst();
        assertEquals("https://example-news.com/atom-story", h.url());
        assertEquals(Instant.parse("2026-10-27T11:00:00Z"), h.published());
    }

    @Test
    void malformedFeedThrows() {
        assertThrows(FeedException.class, () -> parse("<rss><channel><item>"));
    }

    @Test
    void refusesDoctypes() {
        // Guards against XML entity tricks in a feed.
        assertThrows(FeedException.class, () -> parse("""
                <?xml version="1.0"?><!DOCTYPE rss [<!ENTITY x "boom">]>
                <rss version="2.0"><channel><title>&x;</title></channel></rss>"""));
    }

    @Test
    void publishedIsNullWhenTheFeedHasNoDate() throws Exception {
        assertNull(parse("""
                <rss version="2.0"><channel><title>t</title><link>https://example-news.com/</link>
                  <item><title>Undated</title><link>https://example-news.com/undated</link></item>
                </channel></rss>""").headlines().getFirst().published());
    }
}
