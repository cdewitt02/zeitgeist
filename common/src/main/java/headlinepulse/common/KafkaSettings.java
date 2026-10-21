package headlinepulse.common;

/** Connection settings shared by every app. */
public final class KafkaSettings {
    /** {@code KAFKA_BOOTSTRAP_SERVERS}, or the broker from docker-compose.yml. */
    public static String bootstrapServers() {
        String env = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        return env == null || env.isBlank() ? "localhost:9092" : env;
    }

    private KafkaSettings() {}
}
