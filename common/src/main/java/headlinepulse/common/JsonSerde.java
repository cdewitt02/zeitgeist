package headlinepulse.common;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/** A Kafka serde that writes a record as JSON, with times as ISO-8601 strings. {@code null} stays {@code null}. */
public final class JsonSerde<T> implements Serde<T> {
    static final ObjectMapper JSON = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final Class<T> type;

    public JsonSerde(Class<T> type) {
        this.type = type;
    }

    @Override
    public Serializer<T> serializer() {
        return (topic, value) -> {
            if (value == null) {
                return null;
            }
            try {
                return JSON.writeValueAsBytes(value);
            } catch (IOException e) {
                throw new SerializationException("Can't write " + type.getSimpleName() + " to " + topic, e);
            }
        };
    }

    @Override
    public Deserializer<T> deserializer() {
        return (topic, data) -> {
            if (data == null) {
                return null;
            }
            try {
                return JSON.readValue(data, type);
            } catch (IOException e) {
                throw new SerializationException("Can't read " + type.getSimpleName() + " from " + topic, e);
            }
        };
    }
}
