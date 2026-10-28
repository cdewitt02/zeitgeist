package headlinepulse.headlines;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import headlinepulse.common.Headline;
import headlinepulse.common.NewsDomains;
import headlinepulse.common.Topics;

/**
 * Polls every feed once per {@link #pollAll} and sends articles not seen before to
 * {@code news.headlines}. One feed failing doesn't stop the others.
 */
final class HeadlinePoller {
    private static final Logger log = LoggerFactory.getLogger(HeadlinePoller.class);

    private final List<Feed> feeds;
    private final FeedFetcher fetcher;
    private final NewsDomains newsDomains;
    private final Producer<String, Headline> producer;
    /** Article URLs already in news.headlines. Updated from the producer's thread when a send fails. */
    private final Set<String> seen;
    private final Map<String, Integer> failuresInARow = new ConcurrentHashMap<>();

    HeadlinePoller(List<Feed> feeds, FeedFetcher fetcher, NewsDomains newsDomains,
                   Producer<String, Headline> producer, Set<String> seen) {
        this.feeds = feeds;
        this.fetcher = fetcher;
        this.newsDomains = newsDomains;
        this.producer = producer;
        this.seen = ConcurrentHashMap.newKeySet();
        this.seen.addAll(seen);
    }

    void pollAll() {
        int updated = 0;
        int unchanged = 0;
        int failed = 0;
        int fresh = 0;
        for (Feed feed : feeds) {
            try {
                Optional<FeedFetcher.Response> response = fetcher.fetch(feed.url());
                if (response.isEmpty()) {
                    unchanged++;
                } else {
                    fresh += process(feed, response.get());
                    fetcher.remember(feed.url(), response.get());
                    updated++;
                }
                Integer failures = failuresInARow.remove(feed.url());
                if (failures != null) {
                    log.info("{} is back after {} failed polls", feed.url(), failures);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                failed++;
                int failures = failuresInARow.merge(feed.url(), 1, Integer::sum);
                log.warn("{} failed ({} in a row): {}", feed.url(), failures, e.toString());
            }
        }
        producer.flush();
        log.info("Polled {} feeds: {} updated, {} unchanged, {} failed | {} new headlines, {} known",
                feeds.size(), updated, unchanged, failed, fresh, seen.size());
    }

    private int process(Feed feed, FeedFetcher.Response response) throws Exception {
        FeedParser.Parsed parsed = FeedParser.parse(response.body(), feed, newsDomains, Instant.now());
        if (parsed.headlines().isEmpty() && parsed.skipped() > 0) {
            log.warn("{}: none of its {} items link an allowlisted article", feed.url(), parsed.skipped());
        }
        int fresh = 0;
        for (Headline h : parsed.headlines()) {
            if (seen.add(h.url())) {
                fresh++;
                producer.send(new ProducerRecord<>(Topics.NEWS_HEADLINES, null, h.seen().toEpochMilli(), h.url(), h),
                        (metadata, e) -> {
                            if (e != null) {
                                // Let the next poll send it again.
                                seen.remove(h.url());
                                log.warn("Couldn't send {}: {}", h.url(), e.toString());
                            }
                        });
            }
        }
        if (fresh > 0) {
            log.info("{}: {} new", feed.outlet(), fresh);
        }
        return fresh;
    }
}
