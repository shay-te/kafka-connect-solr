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

## 2026-08-14 — FIND phase made symmetric (fq), post-merge re-run

**Methodology fix.** The Solr FIND phase used to run `q=status:2 AND language:en` — a *scored*
main query — while the ES side ran the same terms in `bool.filter` (filter context: unscored,
cached bitsets). That was asymmetric against Solr and unfaithful to production, where the read
layer (`solr_query_builder.search_docs`) always sends `q=*:*` with every filter as `fq`. FIND
now runs `q=*:*&fq=status:2&fq=language:en&rows=20` — the exact prod shape and the ES filter
analog. Both engines still return full documents per hit (fair retrieval).

**Results (same run, 20k docs, best-of-N, containers on one laptop):**

| Phase  | Solr    | ES      | Ratio |
|--------|---------|---------|-------|
| SEED   | 525 ms  | 1115 ms | 2.1x  |
| UPDATE | 455 ms  | 1079 ms | 2.4x  |
| FIND   | 618 ms  | 902 ms  | 1.5x  |
| SORT   | 1235 ms | 1313 ms | 1.1x  |
| DELETE | 177 ms  | 1251 ms | 7.1x  |

Per-query FIND best-of-N went 3.55 ms → 3.09 ms with the fq shape. The *ratio* barely moved at
this index size: with 20k tiny docs, scoring two terms is sub-millisecond, so FIND is dominated
by HTTP round-trip + serializing 20 full documents — work identical for both engines. The fq
shape matters anyway: (a) it measures the code path production actually runs, and (b) unscored
cached filters scale with index size where scored queries do not. Cross-run absolute numbers
swing with container boots (SEED Solr 400↔525 ms across runs); compare ratios within a run,
never absolutes across runs.

**filterCache autowarm — proposed, measured, REJECTED.** The "reads go cold after every ~5s
soft commit" hypothesis suggested `filterCache autowarmCount 0 → 64`. A direct A/B on the
project SolrCloud (two identical collections from the real configset differing only in
autowarmCount, same 20k prod-shaped docs, `fl=_src` reads) said no:

| Experiment | autowarm=0 | autowarm=64 |
|---|---|---|
| 40s write churn, reader p50 | 7.6 ms | 8.3 ms |
| 40s write churn, reader p99 | **52 ms** | 71 ms |
| post-soft-commit 8-filter sweep, mean | **79 ms** | 94 ms |

Recomputing a filter bitset at this scale costs ~1 ms; re-executing up to 64 warming queries
on every new searcher costs more than it saves and lands as tail-latency spikes. The
configset stays at `autowarmCount="0"` (comment in `solrconfig.xml` points here). Revisit
only if production slow-logs/JFR show filter recompute as a dominant cost at real index size.
The same A/B re-measured the fq-vs-scored shape on one warm collection, interleaved: 5.91 vs
6.14 ms/query (~4%) — consistent with "fidelity fix, wins grow with index size", not a local
speedup.

### Follow-up: why ES won "top-level filter + sort" — found and fixed (schema, not engine)

Q1 (`fq=status:2`, `sort=created_at desc`, rows=20) was the one query where ES beat Solr
(0.78x). Investigation on a live SolrCloud A/B (same 50k docs, same timestamp indexed three
ways) showed the cause: `created_at` was not an explicit field, so it fell into the schema's
catch-all `*` dynamicField — **multiValued** — and every sort paid a per-doc SORTED_SET
selector. The other suite queries sorted properly-typed fields (`height` plong,
`*_sort_value` single-valued string), which is why only Q1 lagged. ES never had this handicap
(its mapping types `created_at` as `date`).

Fix: the prod schema (and its test mirror) now declares the scalar sort/range columns as
explicit single-valued docValues string fields (`created_at`, `updated_at`, `birthday`,
`conversation_id`, package dates, last_admin_* dates). Type stays string → range/sort
semantics unchanged; only the multiValued selector goes away. Validated: configset provisions
on real SolrCloud (8/8) and the read-path integration suite passes against a core built from
the updated schema (10/10).

Re-run (same harness, 50k docs, 300 iters):

| Query | before (Solr/ES ms) | after (Solr/ES ms) |
|---|---|---|
| 1. top-level filter + sort | 8.05 / 6.28 — **ES 1.3x** | 5.97 / 8.02 — **Solr 1.34x** |
| 2. custom-field filter + sort (flat vs nested) | 4.13 / 5.33 — Solr 1.29x | 4.95 / 5.68 — Solr 1.15x |
| 3. numeric range + sort | 3.00 / 4.94 — Solr 1.65x | 3.76 / 6.82 — Solr 1.81x |
| AVERAGE | Solr 1.09x | **Solr 1.40x** |

Absolute ms swing between container boots (both engines moved); the within-run ratios are the
signal. After the schema fix Solr wins every query in the suite. Deploy note: schema changes
ship via the blue/green flow — new versioned collection from the updated configset, re-stream,
flip the alias (README_Solr_BlueGreen_Deploy.md); an in-place RELOAD does not retype existing
docs.

## 2026-08-14 — systematic speed hunt (measured on real SolrCloud, 50k prod-shaped docs)

Three candidates surfaced by reading the whole read path; every one measured before touching:

| Candidate | Measurement | Verdict |
|---|---|---|
| `id` without docValues (it's the tiebreak on every custom-field sort → heap uninversion per searcher) | steady state equal; post-soft-commit first-sort tail max 50.6→32.2 ms, mean −13% with docValues | **APPLIED** — `docValues="true"` on `id` (small tail win + removes per-searcher heap uninversion; Solr 9 default schema does the same) |
| Funnel sort `field(funnel_lead_N_score,max)` (function over multiValued) vs plain single-valued numeric sort | 8.06 vs 7.84 ms/q | **REJECTED** — ~3%, not worth a streamer+schema+backend change |
| Dashboard recent-promises: fetch 2000 full `_src` blobs (~2.4 MB) to pick 20 promises | **120 ms → 6.5 ms (18.4x)** with the sorted top-K design | **APPLIED** — see below |

**Recent-promises redesign.** The streamer now emits `promises_latest_actionable_at` = max
`created_at` over the user's actionable (AT_RISK/PENDING/BROKEN) promises (absent when none).
The K most recent actionable promises can only belong to the K users with the highest value, so
the backend fetches `start_row+page_size` users sorted by it instead of scanning
`RECENT_PROMISES_USER_SCAN=2000` docs (the constant remains as the deep-paging cap, and the old
scan remains as an automatic fallback when the field is absent — rollback / mid-restream).
Correctness proven on real Solr: with unique timestamps the new page equals ground truth
computed over all 50k users / 75k promises exactly; with tied timestamps only tie-order can
differ (the old path additionally MISSED any promise outside its first-2000-docs window, and
its `recent_total` was capped at the scan size — the new `recent_total` is exact, taken from
the `promises_summary` facet). Ships with the schema change via blue/green re-stream.

## 2026-08-14 — final Solr vs ES comparison (post speed-hunt schema + dashboard redesign)

Query suite grew a 4th head-to-head: the dashboard recent-promises page — Solr's design
(top-K users by the streamer-derived `promises_latest_actionable_at`, `fl=_src`, promises
sliced client-side, that client work timed too) vs ES's native server-side `nested top_hits`
aggregation. All Solr queries now fetch `fl=_src` — the only retrieval shape production uses
(an un-`fl`'d query fetches every stored field, which since the promises seeding includes the
_src blob and skews all rows; that mistake produced one anomalous run, kept in the log below
as a warning).

Same run, 20k docs (ops) / 50k docs × 300 iters (queries), fresh containers:

| Operation | Solr | ES | Solr advantage |
|---|---|---|---|
| SEED 20k | 398 ms (50,251 r/s) | 1,799 ms | 4.5x |
| UPDATE 20k | 454 ms (44,053 r/s) | 1,878 ms | 4.1x |
| FIND (filter, 200x) | 705 ms | 1,301 ms | 1.9x |
| SORT (200x) | 718 ms | 1,458 ms | 2.0x |
| DELETE 20k | 191 ms (104,712 r/s) | 1,241 ms | 6.5x |
| Q1 top-level filter + sort | 4.59 ms | 6.11 ms | 1.33x |
| Q2 custom-field filter + sort (flat vs nested) | 3.57 ms | 6.56 ms | 1.84x |
| Q3 numeric range + sort | 2.96 ms | 4.67 ms | 1.58x |
| Q4 dashboard recent promises | 2.93 ms | 4.24 ms | 1.45x |
| Query average | 3.51 ms | 5.40 ms | **1.54x** |

Solr wins every operation and every query, including beating ES's own server-side nested
top_hits with the flat top-K + client-slice design. The suite gate (Solr avg ≤ ES avg) is
enforced on every -Dperf.query=true run.
