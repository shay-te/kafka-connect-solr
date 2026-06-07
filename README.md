# kafka-connect-solr

A production-grade Apache Solr sink connector for Kafka Connect.
The shape and configuration surface mirror the Confluent
[Elasticsearch sink connector](https://docs.confluent.io/kafka-connectors/elasticsearch/current/overview.html)
so moving a sink config from Elasticsearch to Solr is a drop-in change.

Performance: the connector uses SolrJ's `Http2SolrClient` so multiple
in-flight requests share a single connection, and an internal executor
pool drives `max.in.flight.requests` (default 8 vs Confluent ES default 5)
concurrent batches per task. This typically beats the Elasticsearch
connector on the same hardware on commit-bound workloads.

Package: `com.una.kafka.connect.solr`

## Architecture

```
+--------------------+      +----------------------+      +--------+
|  SolrSinkConnector |----->|     SolrSinkTask     |----->|  Solr  |
+--------------------+      +----------+-----------+      +--------+
                                       |
                                       v
                            +----------+-----------+
                            |     SolrWriter       |
                            +----------+-----------+
                                       |
        +---------------+---------------+----------------+
        v               v               v                v
SolrRecordConverter  SolrBulkProcessor  SolrSchemaManager  CollectionResolver
```

Helpers: `SolrClientFactory`, `SolrVersionDetector`, `RetryUtil`,
`transforms.WKBToLatLon` (PostGIS WKB -> Solr `LatLonPointSpatialField`).

## Configuration (mirrors the Elasticsearch sink)

| Key                          | Default     | Notes                                                              |
|------------------------------|-------------|--------------------------------------------------------------------|
| `solr.url`                   | _required*_ | Comma-separated base URLs (`http://host:8983/solr`)                |
| `solr.zk.host`               | _required*_ | Zookeeper string for SolrCloud (mutually exclusive with solr.url)  |
| `solr.collection`            | `""`        | Default collection / core. Falls back to topic if empty            |
| `connection.username`        | `""`        | Basic-auth user                                                    |
| `connection.password`        | `""`        | Basic-auth password                                                |
| `connection.timeout.ms`      | `5000`      | HTTP connect timeout                                               |
| `read.timeout.ms`            | `60000`     | HTTP read timeout                                                  |
| `key.ignore`                 | `false`     |                                                                    |
| `schema.ignore`              | `false`     |                                                                    |
| `behavior.on.null.values`    | `ignore`    | `ignore` / `delete` / `fail`                                       |
| `behavior.on.malformed.documents` | `fail` | `ignore` / `warn` / `fail`                                         |
| `batch.size`                 | `2000`      | Records per bulk update                                            |
| `linger.ms`                  | `50`        | Time to wait while filling a batch                                 |
| `flush.timeout.ms`           | `30000`     | Max wait when flushing                                             |
| `max.in.flight.requests`     | `8`         | Concurrent Solr requests per task                                  |
| `max.buffered.records`       | `20000`     | Back-pressure threshold                                            |
| `max.retries`                | `5`         | Max attempts on retryable failures                                 |
| `retry.backoff.ms`           | `200`       | Initial backoff, doubles up to 30s                                 |
| `write.method`               | `INDEX`     | `INDEX` / `UPSERT` / `ATOMIC_UPDATE`                               |
| `id.strategy`                | `KAFKA_KEY` | `KAFKA_KEY` / `RECORD_FIELD` / `TOPIC_PARTITION_OFFSET` / `UUID`   |
| `id.field`                   | `id`        | Dot-path for `RECORD_FIELD`                                        |
| `collection.naming.strategy` | `TOPIC`     | `TOPIC` / `STATIC` / `TOPIC_REGEX`                                 |
| `schema.auto.create`         | `false`     | Try to create the collection on SolrCloud                          |
| `schema.auto.evolve`         | `false`     | Auto-add new fields to managed schema                              |
| `commit.within.ms`           | `1000`      | Solr `commitWithin` sent with every batch                          |
| `streaming.enabled`          | `false`     | Wrap the client in `ConcurrentUpdateHttp2SolrClient` (HTTP/2 streaming). Standalone Solr only; forces sync offsets |
| `streaming.queue.size`       | `10000`     | Internal queue when `streaming.enabled=true`                       |
| `streaming.threads`          | `4`         | Streaming worker threads when `streaming.enabled=true`             |
| `partition.fanout.enabled`   | `false`     | One BulkProcessor per Kafka partition (use when tasks.max < partitions) |
| `dry.run`                    | `false`     | Log intended Solr actions without writing                          |
| `mapping.version`            | `""`        | If set, stamp every doc with `_mapping_version=<value>`            |

\* exactly one of `solr.url` / `solr.zk.host` must be set.

## Delivery guarantees

- At-least-once delivery, with idempotent writes when the unique key is
  stable (`id.strategy` != `UUID`).
- `429` and `5xx` are retried with exponential backoff (max 30 s).
- `4xx` is reported to Kafka Connect so the record lands in the DLQ.
- Leader election / collection unavailable on SolrCloud retries
  transparently via the CloudHttp2SolrClient.

## Ordering: when `max.in.flight.requests > 1` is safe

This connector defaults to `max.in.flight.requests=8` for throughput. Kafka
guarantees order *within a partition*, so as long as a given document's
writes all land on the same partition (the Debezium default — row PK is
the Kafka key) they arrive at the connector in the right order.

What the connector cannot guarantee on its own: two **different batches**
that each contain a write for the same key can be acknowledged by Solr in
either order under concurrent in-flight requests. Worst case, an older
write lands after a newer one and you lose the latest state.

You have three ways to make `max.in.flight.requests > 1` correct:

1. **Enable external versioning (recommended).** Tell Debezium to expose
   the Kafka offset as a header and point the connector at it:

   ```properties
   external.version.header=__kafka_offset
   ```

   Solr's `_version_` semantics will reject any write whose version is
   older than the doc's current version. Reordering becomes safe — the
   stale batch is just dropped.

2. **Use atomic updates for partial mutations.** When only some fields
   change, set `write.method=ATOMIC_UPDATE`. Solr merges field-by-field
   so reordering is benign at the field level.

3. **Drop to `max.in.flight.requests=1`.** Slower but bulletproof — every
   batch is acked before the next is sent. Matches the
   kafka-connect-elasticsearch default.

For a typical Debezium CDC pipeline where each row updates at human
speed (seconds apart), races for the same key are rare in practice
and (1) is the production-safe default.

## Recommended Debezium config

For a `Debezium → Kafka → kafka-connect-solr → Solr` pipeline:

```properties
# Connection
solr.zk.host=zk1:2181,zk2:2181/solr     # SolrCloud — direct shard routing
solr.collection=users

# Id: Debezium ships the row primary key as the Kafka key.
id.strategy=KAFKA_KEY

# Tombstones become deletes (Debezium emits them when ExtractNewRecordState SMT
# is configured with default delete handling).
behavior.on.null.values=delete

# Order safety: pair external versioning with high in-flight concurrency.
external.version.header=__kafka_offset
max.in.flight.requests=8

# DLQ
errors.tolerance=all
errors.deadletterqueue.topic.name=solr-sink-dlq
errors.deadletterqueue.context.headers.enable=true

# Throughput
batch.size=2000
bulk.size.bytes=5242880
linger.ms=50
commit.within.ms=5000

# SMTs (required on the upstream Debezium connector, not here):
#   transforms=unwrap
#   transforms.unwrap.type=io.debezium.transforms.ExtractNewRecordState
```

## Quick start (standalone Solr)

```bash
mvn -DskipTests package
docker compose up -d
curl -s -XPOST -H "Content-Type: application/json" \
    --data @config/quickstart-solr.properties \
    http://localhost:8084/connectors
```

## Building

### Prerequisites

| Tool | Version | Why |
|---|---|---|
| **JDK** | **11** (LTS) | Source/target is Java 11 for broad worker compatibility. Build also runs cleanly on JDK 17. Avoid JDK 24+ — Mockito 5.5 (used by the test suite) doesn't yet support that bytecode level and unit tests will fail to load. |
| **Maven** | 3.6+ | The wrapper isn't checked in; install via Homebrew (`brew install maven`), SDKMAN, or your package manager. |
| **Docker** | any recent version | **Only required for integration tests** (`*IT.java` — they spin up a real Solr 9.4 container via Testcontainers). Skip with `-DskipITs` if you don't have Docker. |

Pin the JDK explicitly before building if you have multiple installed:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 11)    # macOS
export PATH="$JAVA_HOME/bin:$PATH"
java -version    # should print 11.x
```

### The one command that runs everything

```bash
mvn -B clean verify
```

This is the canonical build script. It:

1. Compiles `src/main` (Java 11 source + target)
2. Runs all unit tests (`*Test.java` via Surefire, ~400 tests, ~30 s)
3. Runs all Testcontainers integration tests (`*IT.java` via Failsafe — starts ephemeral Solr 9.4 containers, ~30 s)
4. Merges JaCoCo coverage exec files (UT + IT) and writes `target/site/jacoco/index.html`
5. Enforces coverage gates: **≥ 95 % instruction, ≥ 85 % branch** — build fails if either drops
6. Builds the deployable artifact: `target/kafka-connect-solr-<version>-package.zip` (the connector + all runtime dependencies, ready to drop into a Connect worker's `plugins/` directory)
7. Builds the plain JAR: `target/kafka-connect-solr-<version>.jar`

End-to-end: ~1–2 minutes on a 4-core laptop with Docker already running.

### Faster feedback loops

```bash
# Compile + unit tests only (skip Docker-backed ITs and coverage gate)
mvn -B test -DskipITs

# Single test class
mvn -B test -Dtest=SolrRecordConverterNumericIdTest

# Single test method
mvn -B test -Dtest=SolrRecordConverterNumericIdTest#coerceFalseKafkaKeyLongPassesThrough

# Skip tests entirely (rebuilding the deployable zip after a code-only change)
mvn -B clean package -DskipTests
```

### Performance / stress benchmarks (opt-in)

```bash
# Head-to-head perf vs Elasticsearch (spins up both containers; tagged @perf so the
# default suite excludes them).
mvn -B test -Pperf
```

### What you get

| Path | What it is |
|---|---|
| `target/kafka-connect-solr-<version>.jar` | The connector classes only (no dependencies). Use this if your worker already has SolrJ on its classpath. |
| `target/kafka-connect-solr-<version>-package.zip` | The shipping artifact — connector + SolrJ + Jackson + JTS + SLF4J API. **This is what you copy to the Connect worker's `plugin.path` directory.** |
| `target/site/jacoco/index.html` | Coverage report, view in a browser. |
| `target/surefire-reports/` and `target/failsafe-reports/` | Per-test XML/TXT reports. |

### Deploying the connector

1. Run `mvn -B clean verify`.
2. Unzip `target/kafka-connect-solr-<version>-package.zip` under your Connect worker's `plugin.path` (e.g. `/opt/connect/plugins/kafka-connect-solr/`).
3. Restart the Connect worker.
4. `POST` a connector config to `http://<worker>:8083/connectors` (see [Quick start](#quick-start-standalone-solr) above).

## Tuning for throughput

The connector itself is allocation-conscious in the hot path (per-collection
buffers reused across flushes, hot config cached in final fields, javabin
wire format on by default via SolrJ's HTTP/2 client). To finish the job,
tune the Kafka Connect worker JVM the connector runs in.

**Pick the JDK first.** The connector compiles to Java 11 for broad
compatibility, but the worker JVM running it should be **Java 21 LTS**
when possible. Java 21 ships a more modern G1, generational ZGC, and
significantly better escape analysis in C2 — typically 5–15% more
throughput on the same code with no other changes.

Recommended worker `KAFKA_HEAP_OPTS` / `KAFKA_JVM_PERFORMANCE_OPTS` for a
high-throughput Solr sink:

```bash
KAFKA_HEAP_OPTS="-Xms4g -Xmx4g"                    # same min/max avoids resize pauses
KAFKA_JVM_PERFORMANCE_OPTS="\
  -server \
  -XX:+UseG1GC \
  -XX:MaxGCPauseMillis=50 \
  -XX:InitiatingHeapOccupancyPercent=35 \
  -XX:+ParallelRefProcEnabled \
  -XX:+ExplicitGCInvokesConcurrent \
  -Djava.awt.headless=true \
  -Dsolr.http2.maxConnectionsPerDestination=64"
```

For low-latency profiles where occasional 10ms GC pauses are unacceptable,
swap G1 for ZGC on JDK 17+:

```bash
-XX:+UseZGC -XX:+ZGenerational
```

Connector-side knobs that move the most throughput, in priority order:

| Setting | Default | When to raise |
|---|---|---|
| `max.in.flight.requests` | 8 | Solr CPU has headroom and your network RTT > 5 ms |
| `batch.size` | 2000 | Documents are small (< 1 KB) and Solr indexing is the bottleneck |
| `bulk.size.bytes` | 5 MiB | Documents are large; prevents oversized requests stalling a slow shard |
| `commit.within.ms` | 1000 | Raise for write-heavy loads where searcher freshness can lag |
| `linger.ms` | 50 | Lower under burst; raise (200–500) for steady high-rate streams |
| `tasks.max` (Kafka Connect) | depends | Set to the topic's partition count — each task gets its own HTTP/2 connection |

On the Connect worker, the Kafka consumer settings that move the most data
through the connector (set them in `connect-distributed.properties` or in
the connector config under the `consumer.` prefix):

```properties
consumer.max.poll.records=1000        # match batch.size to your records-per-batch target
consumer.fetch.min.bytes=1048576      # 1 MiB - waits for a fuller fetch
consumer.fetch.max.wait.ms=500        # cap the wait so latency stays bounded
consumer.max.partition.fetch.bytes=4194304  # 4 MiB per partition per poll
```

For deep-dive perf model + architectural options still on the table
(streaming via `ConcurrentUpdateHttp2SolrClient`, per-partition fanout,
direct javabin emission), see [`docs/performance.md`](docs/performance.md).
