# Headline Pulse: Texas 2026

Real-time Bluesky reaction to Texas midterm news, built with Java and Apache Kafka.

## Requirements

- JDK 21 or newer to launch Gradle (`sudo apt install openjdk-21-jdk`). The build compiles and runs on Java 21 either way.
- Docker with Compose.

## Run the poller

```bash
docker compose up -d                 # Kafka, plus Kafka UI at http://localhost:8081
./gradlew :tools:createTopics        # once; safe to rerun
./gradlew :poller:installDist
poller/build/install/poller/bin/poller   # Ctrl+C to stop; resumes on the next run
```

Run the installed script rather than `./gradlew :poller:run` for real collection. Ctrl+C then reaches the poller directly, so you see it commit and exit, and rebuilding the project can't restart it mid-collection. Starting a second poller makes the first one exit, so you can hand over between terminals without stopping.

To check a stretch of collected data for holes (for example, around a restart):

```bash
./gradlew :tools:checkGaps --args="2026-10-19T13:30:00Z 2026-10-19T14:00:00Z"
```

The poller copies every Jetstream post event into `bsky.raw` and commits its cursor to `bsky.cursor` in the same Kafka transaction every 500 ms. On restart it resumes 5 seconds before the last committed cursor, so stopping or killing it causes duplicates but no gaps, as long as it's back within about a day (Jetstream's history). With no saved cursor, it starts from the oldest event Jetstream still has.

Every 10 seconds it logs the event rate, the queue depth, and how far behind live the committed cursor is. A `GAP:` warning means posts were lost; note it.

| Variable | Default |
|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `JETSTREAM_URL` | `wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post` |

Kafka UI is on port 8081 rather than 8080 because Open WebUI uses 8080 on this machine.
