# kafka-connect-solr

A production-grade Apache Solr sink connector for Kafka Connect.
The shape and configuration surface mirror the Confluent
[Elasticsearch sink connector](https://docs.confluent.io/kafka-connectors/elasticsearch/current/overview.html)
so moving a sink config from Elasticsearch to Solr is a drop-in change.

Performance: the connector uses SolrJ's `Http2SolrClient` so multiple
in-flight requests share a single connection, and an internal executor
pool drives `max.in.flight.requests` concurrent batches per task. It
defaults to `1` (strict apply order); raise it once the ordering guard
below is in place. Measured against the Elasticsearch connector on the
same hardware (`docs/benchmark.md`, 2026-08-23, median of 3 runs): 2.9x on
bulk insert, 2.4x on update, 9.2x on delete, ~1.5x on filter/sort, ~1.2x on
the query suite.

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

Helpers: `SolrClientFactory`, `RetryUtil`.

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
| `max.in.flight.requests`     | `1`         | Concurrent Solr requests per task; >1 needs `kafka.offset.version.field` |
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

This connector defaults to `max.in.flight.requests=1`, which is always safe.
Raising it is the single biggest throughput lever, but only with the guard
below — `Validator` rejects `> 1` without it. Kafka
guarantees order *within a partition*, so as long as a given document's
writes all land on the same partition (the Debezium default — row PK is
the Kafka key) they arrive at the connector in the right order.

What the connector cannot guarantee on its own: two **different batches**
that each contain a write for the same key can be acknowledged by Solr in
either order under concurrent in-flight requests. Worst case, an older
write lands after a newer one and you lose the latest state.

You have three ways to make `max.in.flight.requests > 1` correct:

1. **Stamp the Kafka offset as a document version (recommended, and the
   only option the validator accepts).** Add a numeric field plus a
   `DocBasedVersionConstraints` update processor with
   `ignoreOldUpdates=true` on it to the collection's solrconfig, then:

   ```properties
   kafka.offset.version.field=_offset_ver
   ```

   Solr then silently drops any write whose stamped offset is older than
   the one already indexed, so reordering is safe.

   Note this is *not* the same as `external.version.header`, which writes
   Solr's reserved `_version_` field. `_version_` is **optimistic
   concurrency** ("apply only if the doc is still at exactly this
   version"), not "ignore older" — feeding it Kafka offsets makes almost
   every update fail with a 409 conflict. Use it only when the header
   really does carry the document's current `_version_`.

2. **Use atomic updates for partial mutations.** When only some fields
   change, set `write.method=ATOMIC_UPDATE`. Solr merges field-by-field
   so reordering is benign at the field level.

3. **Stay at `max.in.flight.requests=1` (the default).** Every batch is
   acked before the next is sent, so *cross-batch* reordering disappears.
   Matches the kafka-connect-elasticsearch default.

Independently of the setting, upserts and deletes buffered in the same
flush interval are always applied in Kafka-offset order: each collection
keeps a single ordered op buffer that is flushed as consecutive same-type
runs (`add,add,del,add` → add-batch, delete-batch, add-batch) sent
sequentially. A `delete` followed by a re-`create` of the same key can
therefore never be applied as create-then-delete.

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

# Order safety: pair the offset version guard with high in-flight concurrency.
# Requires _offset_ver + a DocBasedVersionConstraints processor in the configset.
kafka.offset.version.field=_offset_ver
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
| **JDK** | **OpenJDK 11 (LTS)** or **OpenJDK 17 (LTS)** | Source/target is Java 11. Both 11 and 17 are tested in CI. **JDK 22+ will fail** — JaCoCo 0.8.11 (our coverage tool) only understands class files up to Java 21; Mockito 5.5 also breaks on JDK 24+. **Use JDK 11 or 17.** |
| **Maven** | 3.6+ | The wrapper isn't checked in; install via Homebrew (`brew install maven`), SDKMAN, or your package manager. |
| **Docker** | any recent version | **Only required for integration tests** (`*IT.java` — they spin up a real Solr 9.4 container via Testcontainers). Skip with `-DskipITs` if you don't have Docker. |

#### Which JDK distribution?

**Any OpenJDK 11 or 17 build works.** The connector doesn't depend on any vendor-specific JDK features. In recommendation order:

1. **Eclipse Temurin** (formerly AdoptOpenJDK) — most widely used OpenJDK build, free for any use, maintained by the Eclipse Foundation. [adoptium.net](https://adoptium.net) or `brew install --cask temurin@11`.
2. **Amazon Corretto** — AWS's OpenJDK build, free, well-tested for production. `brew install --cask corretto@11`.
3. **Azul Zulu** — another widely-used free OpenJDK distribution.

**Oracle JDK** also works technically, but its license requires a paid Oracle Java SE Subscription for any commercial / production use as of JDK 17+. **Stick to OpenJDK builds unless you have a specific Oracle support contract.**

You do **not** need GraalVM, OpenJ9, or any other JVM variant for the build — vanilla HotSpot is correct.

#### Pin the JDK explicitly

`mvn` picks up whatever `JAVA_HOME` points to, and on macOS that often defaults to the newest JDK you have installed. Verify before each build:

```bash
# macOS (with /usr/libexec/java_home)
export JAVA_HOME=$(/usr/libexec/java_home -v 11)
export PATH="$JAVA_HOME/bin:$PATH"
java -version    # MUST print 11.x (or 17.x). 22+ will not work.

# Linux with SDKMAN (recommended)
sdk install java 11.0.21-tem    # Temurin 11
sdk use java 11.0.21-tem
java -version

# Linux with system package manager
sudo apt install temurin-11-jdk    # Debian/Ubuntu with Adoptium repo
sudo dnf install java-11-openjdk-devel   # RHEL/Fedora
```

If you can't get the right JDK on the PATH globally, prefix the build command:

```bash
JAVA_HOME=/path/to/jdk-11 PATH=/path/to/jdk-11/bin:$PATH mvn -B clean verify
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

The perf run prints a per-phase metrics table (CPU vs wall, allocated MB/s, GC count + pause
ms) for both Solr and Elasticsearch, and writes JFR captures to `target/perf-jfr/` for
offline flamegraph analysis. See [`docs/benchmark.md`](docs/benchmark.md) for the
methodology, how to read the table, and how to attach async-profiler for live CPU /
allocation flamegraphs.

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

### Troubleshooting the build

| Symptom | Fix |
|---|---|
| `Mockito couldn't self-attach to current VM` / `IllegalStateException: Could not self-attach` | The pom already passes `-Djdk.attach.allowAttachSelf=true` via the Surefire/Failsafe `argLine`. If you've **overridden** `argLine` (e.g. in a profile or via `-DargLine=`), make sure you keep that property, otherwise the JDK refuses self-attach and every Mockito `mock(...)` call fails. |
| `Unsupported class file major version N` from JaCoCo | You're building on JDK 22+. JaCoCo 0.8.11 (pinned in `pom.xml`) only understands up to Java 21 bytecode. Switch to JDK 11 or 17, or bump the JaCoCo plugin to 0.8.13+ in `pom.xml`. |
| Failsafe ITs hang or fail with "Cannot connect to Docker daemon" | The integration tests spin up real Solr containers via Testcontainers. Either start Docker Desktop / colima / podman first, or run unit tests only: `mvn -B test -DskipITs`. |
| `Rule violated for bundle … instructions covered ratio is 0.94, expected minimum is 0.95` | The coverage gate is failing. JaCoCo's HTML report at `target/site/jacoco/index.html` shows which class regressed. Either add tests for the uncovered lines or, if the gate itself needs adjusting, modify `pom.xml`'s `check-coverage` execution. |
| `Tests run: N, Failures: 0, Errors: 0` but the build still fails | Something in the `verify` phase after tests is failing — typically the coverage gate (see above) or the assembly plugin. Scroll back in the log for `[ERROR]` lines.|

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
| `max.in.flight.requests` | 1 | Solr CPU has headroom and RTT > 5 ms — **and** `kafka.offset.version.field` is configured |
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
