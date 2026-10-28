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

## Run the pipeline

```bash
./gradlew :tools:createTopics        # adds bsky.posts and posts.matched if they're missing
./gradlew :pipeline:installDist
pipeline/build/install/pipeline/bin/pipeline
```

The pipeline is a Kafka Streams app that reads `bsky.raw`, parses each event, keeps posts with a declared English language (`en`, `en-US`, ...), drops repeats seen within 10 minutes (the poller's restart overlap), and writes them to `bsky.posts` as JSON keyed by post URI. A delete becomes a tombstone for its URI.

Posts worth scoring also go to `posts.matched`, with the reasons they matched:

- **`urls`:** articles they link on a Texas outlet in `config/news-domains.yaml`, canonicalized: `https`, lowercase host without `www.`/`m.`/`amp.`, no AMP path, trailing slash, query, or fragment.
- **`topics`:** races and issues from `config/topics.yaml` whose hashtags or phrases appear in the post text or link card, plus the parents of those topics.
- **`sample`:** 10% of all English posts, picked by a hash of the URI, as the baseline. Most of `posts.matched` is the sample.

A delete of a post in `posts.matched` becomes a tombstone there too. Deletes of other posts are left out, since they're nearly all of them.

## Run the headlines app

```bash
./gradlew :tools:createTopics        # adds news.headlines if it's missing
./gradlew :headlines:installDist
headlines/build/install/headlines/bin/headlines
```

Every 3 minutes it polls the RSS feeds in `config/feeds.yaml` and writes each article it hasn't seen to `news.headlines` (compacted), keyed by the same canonical article URL that posts linking it get in `posts.matched`, so the two join. It uses conditional GET, so an unchanged feed costs a 304. On startup it reloads the keys already in the topic, so a restart sends nothing twice.

Each poll logs how many feeds were updated, unchanged, or failed, and how many headlines were new. A feed that fails is retried on the next poll, and its failures in a row are logged. Not every outlet in `news-domains.yaml` has a usable feed; `feeds.yaml` notes which and why. Links to those outlets still count as URL matches.

| Variable | Default |
|---|---|
| `FEEDS` | `config/feeds.yaml` |
| `NEWS_DOMAINS` | `config/news-domains.yaml` |

## Review topic matches

Before relying on a topic, read a sample of its matches and fix the phrases that pull in noise. This runs the current `config/topics.yaml` over recent `bsky.posts`, so you can edit the file and rerun without replaying the pipeline:

```bash
./gradlew :tools:reviewTopic                              # match counts for every topic
./gradlew :tools:reviewTopic --args="tx-gov-abbott 50"    # 50 random matches, with what matched each
```

A third argument sets how many recent posts to scan (default 2,000,000). The sample uses a fixed seed, so a rerun shows mostly the same posts. Events that can't be parsed are counted and logged, never fatal. It runs exactly-once, so the dedupe store and the output can't disagree after a crash.

Every 10 seconds it logs the post rate, the share kept as English, and totals for matches (by URL, topic, and sample), deletes, duplicates, and unparseable events. On its first run it starts from the oldest event in `bsky.raw`. To reprocess from the start, stop it and run `kafka-streams-application-reset --application-id headline-pulse-pipeline --input-topics bsky.raw`.

| Variable | Default |
|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` |
| `NEWS_DOMAINS` | `config/news-domains.yaml` |
| `TOPICS` | `config/topics.yaml` |
| `PIPELINE_STATE_DIR` | `~/.headline-pulse/kafka-streams` |

Kafka UI is on port 8081 rather than 8080 because Open WebUI uses 8080 on this machine.
