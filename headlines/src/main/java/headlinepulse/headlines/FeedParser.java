package headlinepulse.headlines;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.rometools.rome.feed.synd.SyndEntry;
import com.rometools.rome.feed.synd.SyndFeed;
import com.rometools.rome.io.FeedException;
import com.rometools.rome.io.SyndFeedInput;
import com.rometools.rome.io.XmlReader;

import headlinepulse.common.Headline;
import headlinepulse.common.NewsDomains;

/** Turns an RSS or Atom feed into headlines, keyed the same way as the article links in posts. */
final class FeedParser {
    /** @param skipped items with no title, or whose link isn't an article on an allowlisted domain */
    record Parsed(List<Headline> headlines, int skipped) {}

    static Parsed parse(byte[] body, Feed feed, NewsDomains newsDomains, Instant now) throws IOException, FeedException {
        SyndFeed syndFeed = new SyndFeedInput().build(new XmlReader(new ByteArrayInputStream(body)));

        Map<String, Headline> byUrl = new LinkedHashMap<>();
        int skipped = 0;
        for (SyndEntry entry : syndFeed.getEntries()) {
            String link = entry.getLink() != null ? entry.getLink() : entry.getUri();
            String title = entry.getTitle() == null ? "" : entry.getTitle().replaceAll("\\s+", " ").strip();
            Optional<String> url = link == null ? Optional.empty() : newsDomains.articleUrl(link);
            if (url.isEmpty() || title.isEmpty()) {
                skipped++;
                continue;
            }
            Date date = entry.getPublishedDate() != null ? entry.getPublishedDate() : entry.getUpdatedDate();
            byUrl.putIfAbsent(url.get(), new Headline(url.get(), feed.outlet(), title,
                    date == null ? null : date.toInstant(), now, feed.url()));
        }
        return new Parsed(List.copyOf(byUrl.values()), skipped);
    }

    private FeedParser() {}
}
