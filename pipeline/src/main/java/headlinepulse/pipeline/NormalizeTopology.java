package headlinepulse.pipeline;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;

import headlinepulse.common.JsonSerde;
import headlinepulse.common.Post;
import headlinepulse.common.Topics;

/** {@code bsky.raw} → parse → keep English → dedupe → {@code bsky.posts}. */
final class NormalizeTopology {
    static Topology build(Stats stats) {
        var builder = new StreamsBuilder();
        builder.addStateStore(Stores.windowStoreBuilder(
                Stores.persistentWindowStore(Normalizer.DEDUPE_STORE,
                        Normalizer.DEDUPE_WINDOW, Normalizer.DEDUPE_WINDOW, false),
                Serdes.String(), Serdes.Long()));

        builder.stream(Topics.BSKY_RAW, Consumed.with(Serdes.String(), Serdes.String()))
                .process(() -> new Normalizer(stats), Normalizer.DEDUPE_STORE)
                .to(Topics.BSKY_POSTS, Produced.with(Serdes.String(), new JsonSerde<>(Post.class)));
        return builder.build();
    }

    private NormalizeTopology() {}
}
