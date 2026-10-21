package headlinepulse.poller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.junit.jupiter.api.Test;

class RawEventTest {
    @Test
    void readsHeaderFieldsAndKeepsJsonUnchanged() throws IOException {
        String json = """
                {"did":"did:plc:test123","time_us":1760900000123456,"kind":"commit",\
                "commit":{"rev":"r1","operation":"create","collection":"app.bsky.feed.post","rkey":"k1",\
                "record":{"$type":"app.bsky.feed.post","text":"Synthetic post 🎉","langs":["en"]},"cid":"c1"}}""";

        RawEvent e = RawEvent.parse(json);

        assertEquals("did:plc:test123", e.did());
        assertEquals(1760900000123456L, e.timeUs());
        assertTrue(e.isCreate());
        assertFalse(e.isDelete());
        assertEquals(json, e.json());
    }

    @Test
    void findsFieldsInAnyOrder() throws IOException {
        RawEvent e = RawEvent.parse("""
                {"commit":{"record":{"nested":{"time_us":1}},"operation":"delete"},"kind":"commit",\
                "time_us":1760900000000001,"did":"did:plc:x"}""");

        assertEquals("did:plc:x", e.did());
        assertEquals(1760900000000001L, e.timeUs());
        assertTrue(e.isDelete());
    }

    @Test
    void acceptsEventsWithoutCommit() throws IOException {
        RawEvent e = RawEvent.parse("""
                {"did":"did:plc:x","time_us":1760900000000002,"kind":"identity","identity":{"handle":"a.test"}}""");

        assertEquals("identity", e.kind());
        assertNull(e.operation());
        assertFalse(e.isCreate());
    }

    @Test
    void rejectsEventsMissingDidOrTime() {
        assertThrows(IOException.class, () -> RawEvent.parse("{\"time_us\":1760900000000000}"));
        assertThrows(IOException.class, () -> RawEvent.parse("{\"did\":\"did:plc:x\"}"));
        assertThrows(IOException.class, () -> RawEvent.parse("[1,2]"));
        assertThrows(IOException.class, () -> RawEvent.parse("{\"did\":"));
    }
}
