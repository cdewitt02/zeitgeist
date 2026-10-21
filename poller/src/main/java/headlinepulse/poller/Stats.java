package headlinepulse.poller;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Logs event rates every 10 seconds. Used only from the poller's sending thread. */
final class Stats {
    private static final Logger log = LoggerFactory.getLogger(Stats.class);
    private static final Duration INTERVAL = Duration.ofSeconds(10);

    private long posts;
    private long deletes;
    private long other;
    private long intervalStart = System.nanoTime();

    void count(RawEvent e) {
        if (e.isCreate()) {
            posts++;
        } else if (e.isDelete()) {
            deletes++;
        } else {
            other++;
        }
    }

    void maybeLog(int queued, long committedUs, long malformed) {
        long now = System.nanoTime();
        double secs = (now - intervalStart) / 1e9;
        if (secs < INTERVAL.toSeconds()) {
            return;
        }

        String cursor = committedUs > 0
                ? "%s, %s behind".formatted(Poller.time(committedUs),
                        Poller.human(Duration.between(Instant.EPOCH.plus(committedUs, ChronoUnit.MICROS), Instant.now())))
                : "none yet";
        log.info("{} events/s ({} posts, {} deletes, {} other) | queue {} | committed {}{}",
                rate(posts + deletes + other, secs), rate(posts, secs), rate(deletes, secs), rate(other, secs),
                queued, cursor, malformed > 0 ? " | " + malformed + " malformed" : "");

        posts = deletes = other = 0;
        intervalStart = now;
    }

    private static String rate(long count, double secs) {
        return "%.1f".formatted(count / secs);
    }
}
