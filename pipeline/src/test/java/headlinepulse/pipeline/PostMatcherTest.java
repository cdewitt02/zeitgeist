package headlinepulse.pipeline;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class PostMatcherTest {
    private static String uri(int i) {
        return "at://did:plc:synthetic" + (i % 97) + "/app.bsky.feed.post/3k" + Integer.toString(i, 32);
    }

    @Test
    void sampleIsAboutTenPercentAndStable() {
        int n = 100_000;
        int in = 0;
        for (int i = 0; i < n; i++) {
            boolean first = PostMatcher.inSample(uri(i), PostMatcher.SAMPLE_PERCENT);
            assertEquals(first, PostMatcher.inSample(uri(i), PostMatcher.SAMPLE_PERCENT));
            if (first) {
                in++;
            }
        }
        double share = (double) in / n;
        assertTrue(share > 0.095 && share < 0.105, "share " + share);
    }

    @Test
    void zeroAndHundredPercent() {
        for (int i = 0; i < 1000; i++) {
            assertEquals(false, PostMatcher.inSample(uri(i), 0));
            assertEquals(true, PostMatcher.inSample(uri(i), 100));
        }
    }
}
