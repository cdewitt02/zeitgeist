package headlinepulse.common;

import java.time.Instant;

/** A post was deleted. The pipeline turns this into a tombstone for {@link #uri()}. */
public record Delete(String uri, String did, Instant time) implements BskyEvent {}
