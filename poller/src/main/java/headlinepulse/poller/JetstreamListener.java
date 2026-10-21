package headlinepulse.poller;

import java.io.IOException;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Turns WebSocket frames into {@link RawEvent}s on a bounded queue. When the queue is full the listener
 * waits before asking for the next message, so the poller stops reading until Kafka catches up.
 */
final class JetstreamListener implements WebSocket.Listener {
    private static final Logger log = LoggerFactory.getLogger(JetstreamListener.class);

    private final BlockingQueue<RawEvent> queue;
    private final StringBuilder message = new StringBuilder();
    private final AtomicLong malformed = new AtomicLong();

    private volatile WebSocket webSocket;
    private volatile boolean closed;
    private volatile boolean waitingForQueue;
    private volatile long lastMessageNanos = System.nanoTime();

    JetstreamListener(BlockingQueue<RawEvent> queue) {
        this.queue = queue;
    }

    @Override
    public void onOpen(WebSocket webSocket) {
        this.webSocket = webSocket;
        if (closed) {
            // Aborted while still connecting.
            webSocket.abort();
            return;
        }
        lastMessageNanos = System.nanoTime();
        webSocket.request(1);
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
        message.append(data);
        if (last) {
            lastMessageNanos = System.nanoTime();
            String json = message.toString();
            message.setLength(0);
            try {
                enqueue(RawEvent.parse(json));
            } catch (IOException e) {
                if (malformed.getAndIncrement() == 0) {
                    log.warn("Skipping malformed event ({}): {}", e.getMessage(), abbreviate(json));
                }
            }
        }
        if (!closed) {
            webSocket.request(1);
        }
        return null;
    }

    @Override
    public CompletionStage<?> onBinary(WebSocket webSocket, ByteBuffer data, boolean last) {
        // Jetstream only sends binary frames when compression is requested, which it isn't.
        webSocket.request(1);
        return null;
    }

    @Override
    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
        log.warn("Jetstream closed the connection: {} {}", statusCode, reason);
        closed = true;
        return null;
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
        log.warn("WebSocket error: {}", error.toString());
        closed = true;
    }

    /** Drops the connection. Events not yet on the queue are dropped too; they replay from the cursor. */
    void abort() {
        closed = true;
        WebSocket ws = webSocket;
        if (ws != null) {
            ws.abort();
        }
    }

    boolean isClosed() {
        return closed;
    }

    /** True if nothing has arrived for {@code timeout}, not counting time spent waiting for the queue. */
    boolean isStalled(Duration timeout) {
        return !waitingForQueue && System.nanoTime() - lastMessageNanos > timeout.toNanos();
    }

    long malformedCount() {
        return malformed.get();
    }

    private void enqueue(RawEvent event) {
        if (queue.offer(event)) {
            return;
        }
        waitingForQueue = true;
        try {
            while (!closed) {
                if (queue.offer(event, 100, TimeUnit.MILLISECONDS)) {
                    return;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            lastMessageNanos = System.nanoTime();
            waitingForQueue = false;
        }
    }

    private static String abbreviate(String s) {
        return s.length() <= 300 ? s : s.substring(0, 300) + "...";
    }
}
