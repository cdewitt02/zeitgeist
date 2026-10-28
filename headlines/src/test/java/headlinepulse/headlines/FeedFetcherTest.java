package headlinepulse.headlines;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class FeedFetcherTest {
    private static final byte[] BODY = "<rss/>".getBytes(StandardCharsets.UTF_8);

    private HttpServer server;
    private String base;
    private final List<String> seenEtags = new ArrayList<>();
    private final List<String> seenAgents = new ArrayList<>();
    private final AtomicInteger status = new AtomicInteger(200);

    @BeforeEach
    void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/feed", exchange -> {
            String etag = exchange.getRequestHeaders().getFirst("If-None-Match");
            seenEtags.add(etag);
            seenAgents.add(exchange.getRequestHeaders().getFirst("User-Agent"));
            if ("\"v1\"".equals(etag)) {
                exchange.sendResponseHeaders(304, -1);
            } else if (status.get() != 200) {
                exchange.sendResponseHeaders(status.get(), -1);
            } else {
                exchange.getResponseHeaders().add("ETag", "\"v1\"");
                exchange.sendResponseHeaders(200, BODY.length);
                exchange.getResponseBody().write(BODY);
            }
            exchange.close();
        });
        server.start();
        base = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    @Test
    void unchangedFeedIsA304AfterItsValidatorsAreRemembered() throws Exception {
        var fetcher = new FeedFetcher();
        FeedFetcher.Response first = fetcher.fetch(base + "/feed").orElseThrow();
        assertArrayEquals(BODY, first.body());

        // Not remembered yet (say, it failed to parse): fetched in full again.
        assertTrue(fetcher.fetch(base + "/feed").isPresent());

        fetcher.remember(base + "/feed", first);
        assertTrue(fetcher.fetch(base + "/feed").isEmpty());
        assertEquals(Arrays.asList(null, null, "\"v1\""), seenEtags);
        assertEquals(FeedFetcher.USER_AGENT, seenAgents.getFirst());
    }

    @Test
    void errorStatusThrows() {
        status.set(503);
        assertThrows(IOException.class, () -> new FeedFetcher().fetch(base + "/feed"));
    }
}
