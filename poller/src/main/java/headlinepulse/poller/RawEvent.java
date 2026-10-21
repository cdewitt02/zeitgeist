package headlinepulse.poller;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

/**
 * One Jetstream message, kept as the original JSON. Only the fields the poller needs are pulled out:
 * {@code did} and {@code time_us} for the Kafka key and timestamp, and {@code kind} and
 * {@code commit.operation} for the rate log.
 */
record RawEvent(String did, long timeUs, String kind, String operation, String json) {
    private static final JsonFactory JSON = new JsonFactory();

    static RawEvent parse(String json) throws IOException {
        String did = null;
        long timeUs = -1;
        String kind = null;
        String operation = null;

        try (JsonParser p = JSON.createParser(json)) {
            if (p.nextToken() != JsonToken.START_OBJECT) {
                throw new IOException("Not a JSON object");
            }
            while (p.nextToken() == JsonToken.FIELD_NAME) {
                String field = p.currentName();
                JsonToken value = p.nextToken();
                switch (field) {
                    case "did" -> did = p.getValueAsString();
                    case "time_us" -> timeUs = p.getValueAsLong(-1);
                    case "kind" -> kind = p.getValueAsString();
                    case "commit" -> {
                        if (value == JsonToken.START_OBJECT) {
                            operation = readOperation(p);
                        } else {
                            p.skipChildren();
                        }
                    }
                    default -> p.skipChildren();
                }
            }
        }

        if (did == null || did.isEmpty() || timeUs <= 0) {
            throw new IOException("Missing did or time_us");
        }
        return new RawEvent(did, timeUs, kind, operation, json);
    }

    boolean isCreate() {
        return "commit".equals(kind) && "create".equals(operation);
    }

    boolean isDelete() {
        return "commit".equals(kind) && "delete".equals(operation);
    }

    /** Reads {@code operation} from the commit object and leaves the parser at its end. */
    private static String readOperation(JsonParser p) throws IOException {
        String operation = null;
        while (p.nextToken() == JsonToken.FIELD_NAME) {
            String field = p.currentName();
            p.nextToken();
            if (field.equals("operation")) {
                operation = p.getValueAsString();
            } else {
                p.skipChildren();
            }
        }
        return operation;
    }
}
