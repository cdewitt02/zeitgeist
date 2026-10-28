package headlinepulse.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

// All post text is synthetic.
class TopicMatcherTest {
    private static Post post(String text) {
        return post(text, null, List.of());
    }

    private static Post post(String text, Post.LinkCard card, List<String> hashtags) {
        return new Post("at://did:plc:test/app.bsky.feed.post/3kabc", "did:plc:test", Instant.EPOCH, text,
                List.of("en"), List.of(), List.of(), card, hashtags);
    }

    /** The name problem, against the real config. */
    @Nested
    class ShippedConfig {
        private static TopicMatcher topics;

        @BeforeAll
        static void load() throws IOException {
            topics = TopicMatcher.load(Path.of("../config/topics.yaml"));
        }

        private boolean matches(String text, String topic) {
            return topics.match(post(text)).contains(topic);
        }

        @ParameterizedTest
        @ValueSource(strings = {
                "Abbott Elementary is the best show on TV",
                "Abbott Elementary season finale: vote for your favorite episode",
                "Bought a glucose monitor from Abbott Laboratories",
                "Bud Abbott and Lou Costello were a comedy duo"})
        void abbottWithoutTexasContextDoesNotMatch(String text) {
            assertFalse(matches(text, "tx-gov-abbott"), text);
        }

        @ParameterizedTest
        @ValueSource(strings = {
                "Abbott signs bill in Austin",
                "Greg Abbott held a press conference",
                "Gov. Abbott said the state will appeal",
                "Abbott Elementary is great, but Abbott's new Texas border plan isn't"})
        void abbottInTexasPoliticsMatches(String text) {
            assertTrue(matches(text, "tx-gov-abbott"), text);
            assertTrue(matches(text, "tx-gov"), "counts toward the race: " + text);
        }

        @ParameterizedTest
        @ValueSource(strings = {"Dan Patrick", "dan patrick spoke today", "What did DAN PATRICK mean?"})
        void danPatrickAlwaysMatches(String text) {
            assertTrue(matches(text, "tx-ltgov-patrick"), text);
            assertTrue(matches(text, "tx-ltgov"), text);
        }

        @ParameterizedTest
        @ValueSource(strings = {
                "Patrick", "Patrick Mahomes threw four touchdowns", "Happy St. Patrick's Day, Texas!",
                "Patrick said he'll vote early in Texas"})
        void patrickAloneNeverMatches(String text) {
            assertFalse(matches(text, "tx-ltgov-patrick"), text);
        }

        @Test
        void distinctiveSurnamesMatchAlone() {
            assertTrue(matches("Paxton's lawsuit was dismissed", "tx-senate-paxton"));
            assertTrue(matches("Talarico is touring the state", "tx-senate-talarico"));
            assertTrue(matches("Talarico is touring the state", "tx-senate"));
        }

        @Test
        void hashtagsMatch() {
            assertEquals(List.of("tx-senate"), topics.match(post("Tonight's debate", null, List.of("txsen"))));
        }

        @ParameterizedTest
        @CsvSource(delimiter = '|', textBlock = """
                Texas grocery prices are up again            | issue-prices
                Inflation is crushing families in TX         | issue-prices
                Corruption in Texas politics runs deep       | issue-corruption
                They took bribes, Texas deserves better      | issue-corruption
                Texas leads the nation in uninsured people   | issue-health
                Medicaid expansion would help Texas          | issue-health
                """)
        void addedIssuesMatchInTexasContext(String text, String topic) {
            assertTrue(matches(text, topic), text);
        }

        @ParameterizedTest
        @ValueSource(strings = {
                "Inflation came in at 3% this month",
                "The corruption in that movie was unbelievable",
                "Health care costs keep going up"})
        void addedIssuesNeedTexasContext(String text) {
            List<String> issues = List.of("issue-prices", "issue-corruption", "issue-health");
            assertTrue(topics.match(post(text)).stream().noneMatch(issues::contains), text);
        }

        @Test
        void pluralsMatch() {
            assertTrue(matches("Texas school vouchers pass the House", "issue-schools"));
            assertTrue(matches("Texas property taxes went up again", "issue-property-tax"));
        }
    }

    @Nested
    class Rules {
        private final TopicMatcher topics = TopicMatcher.parse("""
                context: &ctx [state, vote]
                topics:
                  - id: race
                    hashtags: [synthrace]
                    phrases: [{ text: "synthetic race" }]
                  - id: candidate
                    parent: race
                    phrases:
                      - { text: "jane zorblax" }
                      - { text: "zorblax", requireAny: *ctx, exclude: ["zorblax cereal"] }
                  - id: issue
                    phrases: [{ text: "lt. gov", requireAny: [state] }]
                """);

        @Test
        void caseInsensitiveWholeWords() {
            assertEquals(List.of("race", "candidate"), topics.match(post("JANE ZORBLAX spoke")));
            assertEquals(List.of(), topics.match(post("Jane Zorblaxian spoke")));
            assertEquals(List.of(), topics.match(post("Jane MegaZorblax spoke")));
            assertEquals(List.of("race", "candidate"), topics.match(post("Jane Zorblax's speech")));
            assertEquals(List.of("race", "candidate"), topics.match(post("jane\n  zorblax")));
        }

        @Test
        void requireAnyNeedsAContextWordAnywhereInThePost() {
            assertEquals(List.of(), topics.match(post("Zorblax spoke")));
            assertEquals(List.of("race", "candidate"), topics.match(post("Zorblax spoke. Go vote!")));
            assertEquals(List.of(), topics.match(post("Zorblax spoke. Go voter!")));
        }

        @Test
        void excludeCancelsOnlyTheOccurrenceItContains() {
            assertEquals(List.of(), topics.match(post("Zorblax Cereal is on sale, vote with your wallet")));
            assertEquals(List.of("race", "candidate"),
                    topics.match(post("Zorblax cereal is fine, but Zorblax the candidate wants your vote")));
        }

        @Test
        void searchesTheLinkCardAndHashtags() {
            var card = new Post.LinkCard("https://example.com/a", "Jane Zorblax announces", "");
            assertEquals(List.of("race", "candidate"), topics.match(post("Look at this", card, List.of())));
            assertEquals(List.of("race"), topics.match(post("Tonight!", null, List.of("synthrace"))));
            // A tag from the post's tags field counts as context.
            assertEquals(List.of("race", "candidate"), topics.match(post("Zorblax spoke", null, List.of("vote"))));
        }

        @Test
        void aPhraseCanNotSpanTextAndCard() {
            var card = new Post.LinkCard("https://example.com/a", "Zorblax announces", "");
            assertEquals(List.of(), topics.match(post("Read about Jane", card, List.of())));
        }

        @Test
        void curlyApostrophesAndPunctuationInPhrases() {
            assertEquals(List.of("issue"), topics.match(post("The state’s Lt. Gov. spoke")));
        }

        @Test
        void reasonsNameWhatMatchedDirectly() {
            assertEquals(Map.of("candidate", "jane zorblax"), topics.reasons(post("Jane Zorblax spoke")));
            assertEquals(Map.of("race", "#synthrace"), topics.reasons(post("x", null, List.of("synthrace"))));
        }
    }

    @Nested
    class Validation {
        @ParameterizedTest
        @ValueSource(strings = {
                "topics: []",
                "topics: [{ id: a }]",
                "topics: [{ id: a, phrases: [{ text: x }] }, { id: a, phrases: [{ text: y }] }]",
                "topics: [{ id: a, parent: nope, phrases: [{ text: x }] }]",
                "topics: [{ id: a, parent: b, phrases: [{ text: x }] }, { id: b, parent: a, phrases: [{ text: y }] }]",
                "topics: [{ id: a, hashtags: ['#tag'] }]",
                "topics: [{ id: a, hashtags: [TxSen] }]",
                "topics: [{ id: a, phrases: [{ text: '  ' }] }]"})
        void rejectsInvalidConfig(String yaml) {
            assertThrows(IllegalArgumentException.class, () -> TopicMatcher.parse(yaml));
        }

        @Test
        void rejectsMisspelledFields() {
            assertThrows(IllegalArgumentException.class,
                    () -> TopicMatcher.parse("topics: [{ id: a, phrases: [{ text: x, requireany: [y] }] }]"));
        }
    }
}
