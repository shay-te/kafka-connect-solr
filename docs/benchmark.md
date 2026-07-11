# Benchmark & profiler runbook

Why this file exists: blind code-scanning for "more wins" has diminishing returns
once the obvious optimisations are in. The grown-up path from here is *measure*,
not *guess*. This document describes the harness we ship, what it captures, and how
to use it to find the next real bottleneck.

## What the harness measures

Each timed phase in `SolrVsElasticsearchPerfTest` is wrapped with two probes:

| Probe              | What it tells you                                            |
|--------------------|--------------------------------------------------------------|
| `BenchmarkMetrics` | CPU time (vs wall), allocated bytes, GC count + pause time   |
| `JfrRecorder`      | A `.jfr` file per phase under `target/perf-jfr/` for offline |
|                    | flame-graph / hot-method / allocation analysis               |

So a single run produces both an at-a-glance per-phase table and the raw data to
answer "where did the time go?" without re-running.

Reading the per-phase table:

```
solr          wall= 4831 ms | cpu= 9214 ms (1.91x wall) | alloc=  742.3 MB (153.7 MB/s) | gc=12 (43 ms)
elasticsearch wall= 7104 ms | cpu= 6802 ms (0.96x wall) | alloc= 1881.6 MB (264.8 MB/s) | gc=28 (118 ms)
```

- `cpu / wall > 1.0` → multi-core work; you are CPU-bound, not I/O-bound.
- `cpu / wall < 1.0` → you are waiting on network / disk / locks more than computing.
- `alloc MB/s` is the *real* GC-pressure signal. Two implementations with the same
  throughput but very different allocation rates will diverge under sustained load.
- `gc count / gc ms` is *added* JVM stop-the-world during the phase. If this dwarfs
  the wall-clock difference, the perf gap is a GC-tuning problem, not an algorithm
  problem.

## Running

Prereqs: Docker (for Testcontainers) and JDK 11+.

```bash
mvn -B -Pperf test -Dtest=SolrVsElasticsearchPerfTest
```

This starts Solr 9.4 and Elasticsearch 7.17 containers and drives the same record set
through **all five operations** head-to-head — **SEED** (bulk insert via the real
`SolrSinkTask` vs ES `_bulk`), **UPDATE** (re-write same ids), **FIND** (bool filter),
**SORT** (numeric desc, top-50), **DELETE** (delete-by-query all) — printing a
`[HEAD-TO-HEAD]` row per op with the winner and margin, and dropping per-phase
`.jfr` files under `target/perf-jfr/`.

Tunables (`-D`): `perf.records` (default 20000), `perf.queries` (200 read iters),
`perf.repeats` (best-of-N per phase, default 3).

Methodology notes:
- **Best-of-N.** Each phase runs `perf.repeats` times and keeps the *fastest* wall-clock.
  A single-shot micro-benchmark under fresh containers is dominated by GC/JIT/scheduler
  jitter (observed ~2x run-to-run swings, winner flipping); best-of-N cancels transient
  interference for *both* engines so the numbers reflect real engine speed. (DELETE runs
  once — a second delete-by-query on an empty index is a no-op.)
- **Fair SORT.** The throwaway `perf` core is schemaless `_default`, which would auto-map
  the numeric sort field as multiValued/no-docValues (Solr can't sort that efficiently — a
  test-core artifact, not a Solr trait). The test defines it single-valued + docValues, as
  the production `aggregated_user_data` configset does, so SORT is apples-to-apples.
- **Gates.** SEED + DELETE are structural Solr wins (batched connector; O(1) delete) →
  gated tightly (≤10% slower). UPDATE/FIND/SORT are near-ties between two mature Lucene
  engines → gated at "competitive" (≤30% slower) to catch real regressions without
  flaking. The printed head-to-head is the source of truth for actual margins.

Representative local result (20k records, best-of-3): Solr won every op — SEED/UPDATE
~1.7–2.8x, FIND ~1.8x, SORT ~1.2–1.4x, DELETE ~15–21x.

### In-flight scaling (`writeThroughputScalesWithInFlightRequests`)

A second `@Test` in the same class quantifies the migration's biggest write-throughput
lever: production runs the sink at `max.in.flight.requests=1` for delete/upsert ordering,
but with `kafka.offset.version.field` + a `DocBasedVersionConstraints` processor (ordering
proven by `SolrOrderingEmbeddedTest`) it can safely run higher. Seeding 20k docs (each
stamped `_offset_ver`) at in-flight 1 / 4 / 8:

```
inFlight=1: ~20k rec/s   1.00x
inFlight=4: ~44k rec/s   ~2.1x
inFlight=8: ~48k rec/s   ~2.3x
```

Wired to env in `debezium_setup.py`: `SOLR_MAX_IN_FLIGHT` (default 1) + `SOLR_OFFSET_VERSION_FIELD`
(default off). See `solr_wiring.md §8.1` to enable safely.

If JFR is not available in your JDK build, the test still runs — JFR captures are
best-effort, the assertion still works off wall-clock.

## Reading the JFR files

JDK Mission Control (free, Adoptium ships it as a separate download):

```bash
jmc -open target/perf-jfr/solr.jfr
```

Useful views inside JMC:

- *Method Profiling* → top hot methods for that phase
- *Memory* → allocation hot paths (look here when `alloc MB/s` is high)
- *GC* → STW pause breakdown
- *Socket I/O* → confirms network vs CPU balance

## Converting JFR → flamegraph

[`async-profiler`](https://github.com/async-profiler/async-profiler) ships a `jfrconv`
that turns a `.jfr` into an interactive flamegraph:

```bash
jfrconv --cpu target/perf-jfr/solr.jfr solr-cpu.html
jfrconv --alloc target/perf-jfr/solr.jfr solr-alloc.html
open solr-cpu.html
```

CPU flamegraph: tall bars = methods burning CPU.
Allocation flamegraph: tall bars = methods producing GC pressure.

## Attaching async-profiler live (more accurate than JFR)

JFR samples on JVM safepoints; async-profiler uses `perf_events` and catches
methods JFR can miss (native, tight loops). Use it when the JFR flamegraph looks
suspiciously flat.

1. Start the perf run in one shell:
   ```bash
   mvn -B -Pperf test -Dtest=SolrVsElasticsearchPerfTest
   ```
2. In another shell, find the surefire fork PID:
   ```bash
   jps -l | grep surefire
   ```
3. Attach a 30 s CPU profile to that PID:
   ```bash
   asprof -d 30 -f /tmp/solr-cpu.html <PID>
   ```
4. Or capture an allocation profile:
   ```bash
   asprof -e alloc -d 30 -f /tmp/solr-alloc.html <PID>
   ```

On macOS install with: `brew install async-profiler`.

## What to look for, dimension by dimension

Codex's seven dimensions, mapped onto what to check in the harness output:

| Dimension                  | Where it shows up                                          |
|----------------------------|------------------------------------------------------------|
| Connector CPU              | `cpu/wall` ratio in the metrics table; CPU flamegraph      |
| GC allocations             | `alloc MB/s` in the table; allocation flamegraph           |
| WKB transform              | Hot frames under `WKBToLatLon.apply` in CPU graph          |
| SolrJ serialization        | Hot frames under `JavaBinCodec` in CPU graph               |
| HTTP / network             | `cpu/wall < 1`; Socket I/O view in JMC                     |
| Solr indexing/commit/merge | Solr container logs (`autoCommit`, segment merges)         |
| Schema field settings      | Solr `coreNodeProps` / fieldCache size in Solr admin UI    |

If a dimension does not show up as the dominant contributor, do *not* spend time
optimising it. That's the whole point of running this instead of scanning code.

## Adding more phases

Wrap any block with the same idiom — the `measure(...)` helper in
`SolrVsElasticsearchPerfTest` returns `(wallMs, delta)` and writes a JFR file:

```java
PhaseResult result = measure("phase-name", () -> {
    long t0 = System.nanoTime();
    doWork();
    return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0);
});
System.out.println(result.delta.toTable("phase-name", result.wallMs));
```

JFR file lands at `target/perf-jfr/phase-name.jfr`.
