package headlinepulse.common;

import java.time.Instant;

/** A Jetstream event the pipeline cares about: a new post, or a deleted one. */
public sealed interface BskyEvent permits Post, Delete {
    /** {@code at://<did>/app.bsky.feed.post/<rkey>}. */
    String uri();

    /** The author's DID. */
    String did();

    /** When Jetstream saw the event ({@code time_us}), not the client's {@code createdAt}. */
    Instant time();
}
