package headlinepulse.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NewsDomainsTest {
    private final NewsDomains domains = NewsDomains.of(Map.of("example.com", "Example News"));

    @Test
    void matchesTheDomainAndItsSubdomains() {
        assertEquals(Optional.of("Example News"), domains.outlet("https://example.com/story"));
        assertEquals(Optional.of("Example News"), domains.outlet("https://politics.example.com/story"));
        assertEquals(Optional.of("Example News"), domains.outlet("https://example.com:8443/story"));
    }

    @Test
    void doesNotMatchLookalikes() {
        assertEquals(Optional.empty(), domains.outlet("https://notexample.com/story"));
        assertEquals(Optional.empty(), domains.outlet("https://example.com.attacker.net/story"));
        assertEquals(Optional.empty(), domains.outlet("https://example.org/example.com"));
    }

    @Test
    void articleUrlIsTheCanonicalUrlWithoutQuery() {
        assertEquals(Optional.of("https://example.com/news/story"),
                domains.articleUrl("http://www.example.com/news/story/?hash=abc&id=2#top"));
        assertEquals(Optional.of("https://politics.example.com/story"),
                domains.articleUrl("https://politics.example.com/amp/story"));
        assertEquals(Optional.empty(), domains.articleUrl("https://www.example.com/"));
        assertEquals(Optional.empty(), domains.articleUrl("https://example.com/?p=123"));
        assertEquals(Optional.empty(), domains.articleUrl("https://other.example/story"));
        assertEquals(Optional.empty(), domains.articleUrl("not a url"));
    }

    @Test
    void rejectsNonCanonicalOrDuplicateDomains() {
        assertThrows(IllegalArgumentException.class, () -> NewsDomains.of(Map.of("www.example.com", "X")));
        assertThrows(IllegalArgumentException.class, () -> NewsDomains.of(Map.of("Example.com", "X")));
    }

    @Test
    void loadsYaml(@TempDir Path dir) throws IOException {
        Path file = dir.resolve("news-domains.yaml");
        Files.writeString(file, """
                outlets:
                  - name: Example News
                    domains: [example.com, example-news.org]
                  - name: Other Outlet
                    domains: [other.example.net]
                """);
        NewsDomains loaded = NewsDomains.load(file);
        assertEquals(3, loaded.size());
        assertEquals(Optional.of("Example News"), loaded.outlet("https://example-news.org/a"));

        Files.writeString(file, """
                outlets:
                  - name: Example News
                    domains: [example.com]
                  - name: Same Domain Again
                    domains: [example.com]
                """);
        assertThrows(IllegalArgumentException.class, () -> NewsDomains.load(file));
    }

    @Test
    void shippedConfigLoads() throws IOException {
        NewsDomains shipped = NewsDomains.load(Path.of("../config/news-domains.yaml"));
        assertTrue(shipped.size() >= 10);
        assertEquals(Optional.of("The Texas Tribune"), shipped.outlet("https://texastribune.org/2026/10/20/story"));
    }
}
