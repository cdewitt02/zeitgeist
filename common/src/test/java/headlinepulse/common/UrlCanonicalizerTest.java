package headlinepulse.common;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Optional;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class UrlCanonicalizerTest {
    @ParameterizedTest(name = "{0}")
    @CsvSource(delimiter = '|', textBlock = """
            # Host
            https://example.com/news/story                    | https://example.com/news/story
            HTTPS://WWW.Example.COM/News/Story                | https://example.com/News/Story
            http://example.com/news/story                     | https://example.com/news/story
            https://m.example.com/news/story                  | https://example.com/news/story
            https://amp.example.com/news/story                | https://example.com/news/story
            https://www.m.example.com/news/story              | https://example.com/news/story
            https://politics.example.com/story                | https://politics.example.com/story
            https://amp.dev/docs                              | https://amp.dev/docs
            https://example.com./story                        | https://example.com/story
            https://reader@example.com/story                  | https://example.com/story
            https://example.com:443/story                     | https://example.com/story
            http://example.com:80/story                       | https://example.com/story
            https://example.com:8443/story                    | https://example.com:8443/story
            # Path
            https://example.com/news/story/                   | https://example.com/news/story
            https://www.example.com/                          | https://example.com
            https://example.com                               | https://example.com
            https://example.com//news//story                  | https://example.com/news/story
            https://example.com/amp/news/story                | https://example.com/news/story
            https://example.com/news/story/amp/               | https://example.com/news/story
            https://example.com/news/story.amp.html           | https://example.com/news/story.html
            https://example.com/news/story.amp                | https://example.com/news/story
            https://example.com/news/camp-story               | https://example.com/news/camp-story
            # Query and fragment
            https://example.com/story#comments                | https://example.com/story
            https://example.com/story?utm_source=bsky&utm_medium=social | https://example.com/story
            https://example.com/story?id=5&utm_campaign=x&fbclid=abc    | https://example.com/story?id=5
            https://example.com/story?b=2&a=1                 | https://example.com/story?a=1&b=2
            https://example.com/story?UTM_Source=x&gclid=1&ref=bsky     | https://example.com/story
            https://example.com/story?_bhlid=f00d&id=3        | https://example.com/story?id=3
            https://example.com/story?outputType=amp&id=7     | https://example.com/story?id=7
            https://example.com/story?outputType=print        | https://example.com/story?outputType=print
            https://example.com/story?amp                     | https://example.com/story
            https://example.com/story?amp=1                   | https://example.com/story
            https://example.com/story?&&id=5&                 | https://example.com/story?id=5
            https://example.com?utm_source=x                  | https://example.com
            # Text real links contain that java.net.URI rejects
            https://example.com/news/año electoral            | https://example.com/news/año electoral
            '  https://example.com/story  '                   | https://example.com/story
            """)
    void canonicalizes(String in, String expected) {
        assertEquals(Optional.of(expected), UrlCanonicalizer.canonicalize(in));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "", "not a url", "example.com/story", "ftp://example.com/file", "mailto:someone@example.com",
            "https://", "https:///story", "https://localhost/story", "https://[::1]/story", "https://exa mple.com/"})
    void rejects(String in) {
        assertEquals(Optional.empty(), UrlCanonicalizer.canonicalize(in));
    }

    @ParameterizedTest
    @CsvSource(delimiter = '|', textBlock = """
            https://example.com                  | example.com
            https://example.com/news?id=1        | example.com
            https://example.com?id=1             | example.com
            https://example.com:8443/story       | example.com
            """)
    void hostOf(String canonical, String host) {
        assertEquals(host, UrlCanonicalizer.hostOf(canonical));
    }
}
