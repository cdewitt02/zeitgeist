package headlinepulse.headlines;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Fetches feeds with conditional GET. After a feed is processed, {@link #remember} keeps its
 * {@code ETag} and {@code Last-Modified}; the next fetch sends them back, so an unchanged feed costs a
 * 304 with no body. Validators are kept only after processing, so a feed that fails to parse is
 * fetched in full again next time instead of being skipped as unchanged.
 */
final class FeedFetcher {
    /** Identifies the app honestly; feeds that refuse it are left out rather than worked around. */
    static final String USER_AGENT = "headline-pulse/0.1 (RSS reader)";
    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    record Response(byte[] body, String etag, String lastModified) {}

    private final HttpClient http = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NORMAL)
            .connectTimeout(TIMEOUT)
            .build();
    private final Map<String, Response> validators = new ConcurrentHashMap<>();

    /** The feed, or empty if it hasn't changed since the last {@link #remember}ed fetch. */
    Optional<Response> fetch(String url) throws IOException, InterruptedException {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(url))
                .timeout(TIMEOUT)
                .header("User-Agent", USER_AGENT)
                .header("Accept", "application/rss+xml, application/atom+xml, application/xml;q=0.9, */*;q=0.8");
        Response last = validators.get(url);
        if (last != null) {
            if (last.etag() != null) {
                request.header("If-None-Match", last.etag());
            }
            if (last.lastModified() != null) {
                request.header("If-Modified-Since", last.lastModified());
            }
        }

        HttpResponse<byte[]> response = http.send(request.GET().build(), HttpResponse.BodyHandlers.ofByteArray());
        if (response.statusCode() == 304) {
            return Optional.empty();
        }
        if (response.statusCode() / 100 != 2) {
            throw new IOException("HTTP " + response.statusCode());
        }
        return Optional.of(new Response(response.body(),
                response.headers().firstValue("ETag").orElse(null),
                response.headers().firstValue("Last-Modified").orElse(null)));
    }

    /** Sends this response's validators with the next fetch of {@code url}. */
    void remember(String url, Response response) {
        if (response.etag() != null || response.lastModified() != null) {
            // The body isn't needed again.
            validators.put(url, new Response(new byte[0], response.etag(), response.lastModified()));
        }
    }
}
