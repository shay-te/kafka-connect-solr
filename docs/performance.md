# Performance model and tuning

This doc explains where time goes in a Kafka → Solr sink, what we've
already done at each layer, and what's still on the table if you need
more throughput. It is intentionally honest about diminishing returns:
once layer 4 (Solr indexing itself) dominates, no amount of connector-
side cleverness will move the needle.

## Cost model

| Layer | What happens | Typical share of wall-time | We own it? |
|---|---|---|---|
| 1. Converter (CPU) | `SinkRecord` → `SolrInputDocument` | 5–10% | yes |
| 2. Bulk assembly | Batch, offset tracking | 1–2% | yes |
| 3. Wire | HTTP framing + serialization + network | 25–40% | yes |
| 4. Solr server | Index, commit, replication | 50–65% | no (config only) |

## What we already do at each layer

### Layer 1 — Converter

- Config knobs (`idStrategy`, `writeMethod`, `compactMapEntries`,
  `keyIgnore`, `idField`) cached in `final` fields at construction —
  no per-record reflection on the config.
- `id.field` dot-path pre-split once at construction.
- `Base64.getEncoder()` cached as a static.
- `TOPIC_PARTITION_OFFSET` id built via a sized `StringBuilder` (no
  `String.format`).
- Struct fields iterated by index (no iterator object).
- KAFKA_KEY / RECORD_FIELD id strategies use an `instanceof String`
  fast-path before falling through to `String.valueOf`.
- **Numeric id pass-through** — `id.coerce.to.string=false` lets numeric
  Kafka keys (`Long` / `Integer`) flow into the SolrInputDocument unchanged.
  Saves a `String.valueOf(...)` allocation per record when the Solr schema's
  id field is declared as `plong` / `pint`. Default `true` keeps backward
  compat.
- **`LinkedHashMap` capacity hint** — `convert()` allocates the backing
  map at `hint × 4/3 + 1`, accounting for the 0.75 load factor. Avoids the
  resize that `new LinkedHashMap<>(hint)` would otherwise trigger on records
  with > 24 fields.
- `SolrRecordConverter` is `final` so the JIT can devirtualise.

### Layer 2 — Bulk

- Per-collection buffers reused across flushes (no `new ArrayList<>(batchSize)`
  per flush).
- Per-buffer byte counter is a **plain `long`** field (`UpsertBuffer.bytes`),
  not `LongAdder` — single-task-thread access by the Kafka Connect contract, so
  the LongAdder cell array was overkill. Saves ~5 ns/record on the
  `bulk.size.bytes>0` path.
- **Byte estimate produced during conversion, not via a second walk.**
  `SolrRecordConverter` accumulates per-field byte contributions through its
  internal `addField(doc, name, value)` helper and exposes the total via
  `lastConversionByteEstimate()`. `SolrBulkProcessor.upsert(...)` accepts the
  pre-computed value, so the standalone `estimateBytes(doc)` walk only runs as
  a fallback when the caller doesn't supply one.
- Hot config cached in `final` fields.
- Sync mode uses a static sentinel `OffsetState` — zero allocation per record.
- Async mode uses `volatile boolean` for ack flag, not `AtomicBoolean` — one
  less wrapper object per record.
- `AsyncOffsetTracker` caches the last-seen `TopicPartition` + its deque so
  consecutive records from the same partition skip the `new TopicPartition()`
  allocation and the HashMap lookup.
- `checkGlobalThresholds` orders its checks LongAdder-first / `nanoTime` second
  — at high throughput the cheap buffer-depth check trips, and we skip the
  more expensive `System.nanoTime()` call entirely.

### Layer 3 — Wire

This is where most of our advantage over the ES connector comes from:

| Property | ES connector | This connector |
|---|---|---|
| Transport | HTTP/1.1 | **HTTP/2 (multiplexed)** |
| Wire format | JSON | **javabin (binary, SolrJ default)** |
| Concurrency model | N TCP connections | **1 connection, N concurrent streams** |
| Per-stream blocking | head-of-line on connection | none (HTTP/2 frames interleave) |
| Default `max.in.flight.requests` | 5 | **8** |
| Response compression | gzip optional | **gzip + zstd opt-in** |

Concretely: at 8 in-flight requests with 1 KB documents and 5 ms RTT,
HTTP/1.1 with 5 connections needs 5 TLS handshakes and serializes the
6th–8th requests behind the first three completing. HTTP/2 with 1
connection multiplexes all 8 simultaneously. On a 5 ms RTT link that
alone is roughly the difference between 200 batches/sec and 1600
batches/sec per task before saturating the connection.

### Layer 4 — Solr

What you control via the connector:

- `commit.within.ms` — soft-commit cadence. The lower it is, the more
  the indexer pauses to make recent writes visible. For ingestion-heavy
  loads, raise this (5000–10000 ms).
- `write.method=ATOMIC_UPDATE` — Solr-only; partial field updates
  without re-shipping the whole doc.
- `solr.zk.host` over `solr.url` — `CloudHttp2SolrClient` hashes each
  doc id and sends straight to the owning shard leader. ES has a
  coordinator hop here.

What you control on the Solr server (out of scope here, but matters):

- `autoCommit` / `autoSoftCommit` settings in `solrconfig.xml`.
- Number of shards / replicas vs ingestion rate.
- JVM heap on the Solr nodes.
- SSD for the index data dir.

## Still on the table

Items below ranked by ROI for a 100k records/sec target. Each lists the
real implementation cost and the offset-tracking implication, because
some of them break the per-record OffsetState contract.

### 1. `ConcurrentUpdateHttp2SolrClient` (streaming updates) — ★★★ — IMPLEMENTED

Opt-in via `streaming.enabled=true` (plus `streaming.queue.size` and
`streaming.threads` to tune the internal buffering). The connector
wraps the base `Http2SolrClient` in CUHTTP2 so docs flow through a
continuously-open HTTP/2 stream instead of one bulk request per batch.
The server pipeline-indexes them.

Win: 20–40% extra throughput when RTT > 1 ms (the bigger the RTT, the
bigger the win — we never wait for a batch round-trip).

Trade-off taken: CUHTTP2 owns the batching, so per-record async offsets
aren't meaningful. When `streaming.enabled=true`, the task forces the
sync offset tracker and `preCommit` blocks on `blockUntilFinished()`
before returning. This guarantees at-least-once: we only commit offsets
once Solr has fully acked everything queued.

When to enable: WAN deploys where the per-batch round-trip dominates.
Standalone Solr only — `solr.zk.host` (SolrCloud) falls back to
`CloudHttp2SolrClient` with a warning, because CUHTTP2 doesn't know
how to do ZK-based shard routing.

### 2. Per-Kafka-partition fanout — ★★ — IMPLEMENTED

Opt-in via `partition.fanout.enabled=true`. Each assigned Kafka
partition gets its own dedicated `BulkProcessor` with independent
buffers + inflight pool. Partitions never wait for each other's batches.

Win: real concurrency multiplied by `assignedPartitions` per task
(2–10× depending on assignment).

Cost: extra thread pool per partition (each pool sized to
`max.in.flight.requests`). Memory: one buffer map per partition. For
typical CDC deploys with 2–8 partitions per task and `max.in.flight=8`,
that's 16–64 worker threads per task — well within reason.

When to enable: when `tasks.max` is smaller than your topic partition
count and one task ends up owning multiple partitions. When
`tasks.max == partitions`, every task already owns exactly one
partition and the shared processor is already optimal — leave fanout
off.

### 3. Request-side gzip — ★

Already done on the response side. Outbound (connector → Solr) gzip
helps WAN/cross-region deploys where bandwidth is the limit.

Win: 50–70% smaller request payload on text-heavy documents.

Cost: small (Jetty has the support; one config key).

Recommendation: opt-in via `connection.compression.requests=true`.
Off by default because it costs CPU when the link is fast.

### 4. Schema-aware encoder cache — ★ — IMPLEMENTED

Done. `SolrRecordConverter` keeps an identity-keyed
`Map<Schema, Map<String, FieldEncoder[]>>` plan cache. The top-level
plan is built once per schema; nested STRUCT plans are built **eagerly
at parent-build time and captured directly in the parent's encoder
closure**, so the per-record nested invocation reuses an array
reference without a runtime cache lookup. The two-level outer map only
serves the schemaless `addScalar`-with-Struct path and the rare
runtime-schema-substitution fallback.

Win realised: ~30 lambda allocations per record eliminated for typical
CDC nested schemas (1–3 levels of nesting), plus the per-record
`switch (schemaType) + instanceof` dispatch is now a `final FieldEncoder[]`
array lookup.

### 5. Direct javabin emission — ★

Skip `SolrInputDocument` entirely; emit javabin bytes straight from the
`SinkRecord` into an `OutputStream` mounted on an `UpdateRequest`'s
`ContentStream`.

Win: eliminates ~16 allocations per record (SolrInputDocument +
internal map + N SolrInputFields). Layer 1 only.

Cost: large refactor; we'd be re-implementing SolrJ's wire encoder.
Risk: subtle compatibility breaks across Solr versions.

Recommendation: don't. The current code is already past the point of
diminishing returns on layer 1. Only worth it if a future profile
shows allocation pressure on the SinkTask hot path.

### 6. WKBToLatLon SMT cost on every CDC record — ★

The `WKBToLatLon` Single-Message Transform (`transforms.WKBToLatLon` in
`scripts/debezium_setup.py`) converts PostGIS WKB byte arrays in the
`location` column into Solr's `lat,lon` string format. It runs **per
record** on every Kafka message that has a `location` payload — which
for this app is most records (users have geo coordinates).

Per-record cost (see `WKBToLatLon.java`):

- One `Map`/`Struct` copy (the SMT can't mutate the input record).
- WKB parse via `WKBReader` (`Geometry geom = WKB_READER.get().read(wkb)`).
- New `SourceRecord` allocation with the transformed value.

This is order-of-magnitude **larger** than any of the per-record
micro-optimisations we made inside the connector itself. If geo data
volume justifies it, the right fix is to move the conversion **upstream**:

- **Best:** have the source emit `lat,lon` directly. PostGIS can output
  `ST_X(location)`, `ST_Y(location)` as separate columns; expose those
  in the Debezium `column.include.list` instead of (or in addition to)
  the raw WKB.
- **Acceptable:** run the SMT in the Debezium source connector instead
  of the Solr sink connector. Same CPU cost but on a different worker
  pool, freeing the sink task for indexing throughput.
- **Last resort:** optimise the SMT itself — pool the `WKBReader`, reuse
  the output map, etc. ~80 LOC of careful refactor for a single-digit
  percent win.

Recommendation: **investigate before optimising.** Profile the kstreams
output with the SMT in vs out (`mvn test -Pperf` with and without
`transforms=WKBToLatLon`) to confirm whether it actually shows up as a
hot spot in your real workload. If it does, the upstream-emit fix is
the cleanest answer; if it doesn't, leave it alone.

## Tuning recipes

### Maximum throughput, LAN deploy

```properties
batch.size=5000
bulk.size.bytes=10485760      # 10 MiB
linger.ms=20
max.in.flight.requests=16
max.buffered.records=80000
commit.within.ms=5000
flush.synchronously=false      # use AsyncOffsetTracker
```

JVM (Connect worker):

```bash
KAFKA_HEAP_OPTS="-Xms8g -Xmx8g"
KAFKA_JVM_PERFORMANCE_OPTS="-server -XX:+UseG1GC -XX:MaxGCPauseMillis=50"
```

### Low latency, small docs

```properties
batch.size=200
bulk.size.bytes=524288         # 512 KiB
linger.ms=5
max.in.flight.requests=4
commit.within.ms=1000
flush.synchronously=true       # safest offsets
```

### WAN / cross-region

```properties
batch.size=2000
bulk.size.bytes=5242880
max.in.flight.requests=8
connection.compression=true
connection.compression.algorithm=ZSTD
# request-side compression when implemented:
# connection.compression.requests=true
read.timeout.ms=120000          # higher to absorb RTT spikes
```

### Bulk rebuild (blue/green reindex)

```properties
batch.size=10000
bulk.size.bytes=20971520        # 20 MiB
linger.ms=100
max.in.flight.requests=24
commit.within.ms=30000          # commit rarely while we backfill
flush.synchronously=false
```

## Runtime choices

### JDK

The connector compiles to Java 11 for compatibility, but the worker JVM
that runs it should be **Java 21 LTS** when possible:

- More aggressive escape analysis in C2 lets the JIT prove our per-batch
  small allocations (Pending wrappers, ArrayList<SolrInputDocument>
  copies) never escape and stack-allocate them.
- G1 in JDK 21 has shorter pauses on the same heap.
- Generational ZGC (JDK 21) is the right choice if you need sub-10ms
  pauses while sustaining > 100k records/sec.

We do not need virtual threads for the bulk path — the executor pool is
small (`max.in.flight.requests`) and well-tuned. Virtual threads could
help further when `streaming.enabled=true` because CUHTTP2's worker
threads spend most of their time blocked on HTTP/2 stream responses.

### GraalVM JDK

GraalVM Community Edition's JIT (Graal) is sometimes faster than C2 on
JSON-heavy workloads. Worth a benchmark run against your real Solr if
the connector is the bottleneck. Native image is not recommended — Kafka
Connect's classloading model fights it.

## How to measure

```bash
mvn test -Pperf
```

Runs the three perf tests:

- `SolrRecordConverterPerfTest` — pure CPU converter throughput
  (target: > 50k r/s on modern hardware).
- `SolrBulkProcessorStressTest` — concurrency vs simulated latency.
- `SolrVsElasticsearchPerfTest` — Testcontainers head-to-head against
  Elasticsearch on the same data, prints both wall-clocks, asserts
  Solr wins by ≥ 10%.

For real production tuning, two complementary tools:

**async-profiler** — allocation + CPU flame graphs, fast, runs in
production without slowing the worker noticeably:

```bash
asprof -d 60 -e alloc -f flame.html <pid>
asprof -d 60 -e cpu   -f cpu.html   <pid>
```

**Java Flight Recorder** — bundled with every JDK 11+, captures GC,
locks, thread state, and method profiles in one trace:

```bash
jcmd <pid> JFR.start name=solr duration=60s filename=solr.jfr
jcmd <pid> JFR.dump  name=solr filename=solr.jfr
# Open the resulting .jfr in JDK Mission Control.
```

If allocations land in:

- `SolrInputDocument$add` → tune `batch.size` down (fewer doc objects
  in flight) or implement (5) direct javabin emission.
- `LinkedHashMap.put` → likely the field map; (5) again.
- `org.eclipse.jetty.http2.*` → wire layer, your link is the bottleneck;
  enable request compression (3) or scale tasks.
