package headlinepulse.pipeline;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

import headlinepulse.common.JsonSerde;
import headlinepulse.common.MatchedPost;
import headlinepulse.common.Post;
import headlinepulse.common.Topics;

/**
 * {@code bsky.raw} → parse → keep English → dedupe → {@code bsky.posts}, then → match →
 * {@code posts.matched}. Both outputs carry tombstones: every delete in {@code bsky.posts}, and only
 * deletes of matched posts in {@code posts.matched}.
 */
final class PipelineTopology {
    static Topology build(Stats stats, PostMatcher matcher) {
        var builder = new StreamsBuilder();
        builder.addStateStore(Stores.windowStoreBuilder(
                Stores.persistentWindowStore(Normalizer.DEDUPE_STORE,
                        Normalizer.DEDUPE_WINDOW, Normalizer.DEDUPE_WINDOW, false),
                Serdes.String(), Serdes.Long()));
        builder.addStateStore(Stores.windowStoreBuilder(
                Stores.persistentWindowStore(MatchProcessor.MATCHED_STORE,
                        MatchProcessor.REMEMBER, MatchProcessor.REMEMBER, false),
                Serdes.String(), Serdes.Long()));

        KStream<String, Post> posts = builder.stream(Topics.BSKY_RAW, Consumed.with(Serdes.String(), Serdes.String()))
                .process(() -> new Normalizer(stats), Normalizer.DEDUPE_STORE);
        posts.to(Topics.BSKY_POSTS, Produced.with(Serdes.String(), new JsonSerde<>(Post.class)));

        posts.process(() -> new MatchProcessor(stats, matcher), MatchProcessor.MATCHED_STORE)
                .to(Topics.POSTS_MATCHED, Produced.with(Serdes.String(), new JsonSerde<>(MatchedPost.class)));
        return builder.build();
    }

    private PipelineTopology() {}
}
