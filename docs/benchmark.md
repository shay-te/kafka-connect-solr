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

Representative result (20k records, best-of-3, median of 3 runs, clean 8 GiB Docker VM / 5 CPUs,
2026-08-23): Solr wins every op — SEED 2.9x, UPDATE 2.4x, DELETE 9.2x, FIND 1.5x, SORT 1.6x.
Margins move with how contended the host is, so read the printed `[HEAD-TO-HEAD]` rows rather than
quoting this line.

### In-flight scaling (`writeThroughputScalesWithInFlightRequests`)

A second `@Test` in the same class quantifies the migration's biggest write-throughput
lever: production runs the sink at `max.in.flight.requests=1` for delete/upsert ordering,
but with `kafka.offset.version.field` + a `DocBasedVersionConstraints` processor (ordering
proven by `SolrOrderingEmbeddedTest`) it can safely run higher. Seeding 20k docs (each
stamped `_offset_ver`) at in-flight 1 / 4 / 8:

```
inFlight=1:  892 ms  (22,422 rec/s)  1.00x
inFlight=4:  506 ms  (39,526 rec/s)  1.76x
inFlight=8:  435 ms  (45,977 rec/s)  2.05x
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

> Superseded — see the 2026-08-23 re-baseline below. Every margin there is smaller, and Q4 is a
> tie rather than a Solr win. Do not quote this table externally.

## 2026-08-23 — re-baseline on a clean host (3 runs), and a SEED measurement bug

Re-ran both suites on an 8 GiB Docker VM / 5 CPUs with no other containers resident.

**A benchmark bug had to be fixed first.** `solrIsFasterThanElasticsearch` and
`writeThroughputScalesWithInFlightRequests` share containers via `@BeforeAll`, and JUnit orders
`@Test` methods arbitrarily. When the in-flight test ran first it left 60k docs in the `perf`
core, so Solr's SEED inserted 20k docs into a ~62k-doc index while ES inserted into a ~2k-doc one.
That understated Solr's seed throughput by ~3x and made SEED read as a tie. Both engines are now
cleared after the warmup, so SEED measures an empty (but JIT-warm) index on both sides. Any future
phase added here must keep that symmetry.

Median of 3 runs (per-run range in brackets):

| Operation | Solr | ES | Solr advantage | vs 2026-08-14 |
|---|---|---|---|---|
| SEED 20k | 362 ms (55,249 r/s) | 1,082 ms (18,484 r/s) | **2.94x** [1.69–3.79] | was 4.5x |
| UPDATE 20k | 337 ms (59,347 r/s) | 815 ms (24,540 r/s) | **2.36x** [2.34–2.81] | was 4.1x |
| FIND (filter, 200x) | 688 ms | 962 ms | **1.47x** [1.30–1.70] | was 1.9x |
| SORT (200x) | 709 ms | 1,065 ms | **1.63x** [1.40–1.65] | was 2.0x |
| DELETE 20k | 50 ms (400,000 r/s) | 459 ms (43,573 r/s) | **9.18x** [8.00–10.00] | was 6.5x |
| Q1 top-level filter + sort | 3.82 ms | 4.81 ms | 1.26x | was 1.33x |
| Q2 custom-field filter + sort (flat vs nested) | 3.60 ms | 4.09 ms | 1.14x | was 1.84x |
| Q3 numeric range + sort | 2.66 ms | 4.05 ms | 1.53x | was 1.58x |
| Q4 dashboard recent promises | 3.20 ms | 3.17 ms | **0.99x — tie** | was 1.45x |
| Query average | 3.32 ms | 4.03 ms | **1.21x** | was 1.54x |

Solr wins every write/CRUD operation in all 3 runs — Solr's WORST run beats ES's BEST run on
every one of them, so the direction is not in doubt even though the margins move.

The four query rows are a **single run** (n=1) and carry no variance estimate. The average (1.21x)
is directionally consistent with the CRUD reads, but Q2 (1.14x) and Q4 (0.99x) are inside the
run-to-run noise seen elsewhere in this suite and should not be quoted as individual results until
the query suite is repeated. What is clear is that Q4 does not reproduce the 1.45x win claimed on
2026-08-14: the flat top-K + client-slice design is at best level with ES's server-side
`nested top_hits`.

**Why the 2026-08-14 margins were larger.** Solr's own timings barely moved (query average 3.32 vs
3.51 ms); ES got much faster (5.40 -> 4.03 ms). That run was taken on a host also carrying the dev
stack, and ES degrades more under memory pressure than Solr does — so the old margins were partly
measuring contention. The structural wins that hold up are DELETE (O(1) delete-by-id vs ES
delete-by-query) and UPDATE (full-doc replace vs read-modify-write).

### In-flight scaling is directionally real but NOT quantifiable on 5 cores

| Run | inFlight=1 | inFlight=4 | inFlight=8 |
|---|---|---|---|
| 1 | 1,069 ms | 734 ms (1.46x) | 348 ms (3.07x) |
| 2 | 1,337 ms | 1,405 ms (**0.95x**) | 1,122 ms (1.19x) |
| 3 | 857 ms | 604 ms (1.42x) | 413 ms (2.08x) |

Run 2 inverted and tripped the assertion. With two engine JVMs plus the test JVM on 5 shared
cores, the concurrency lever swings 1.19x–3.07x. Quote it as "~2x, re-measure on real hardware"
and never as a single figure. The assertion now gates on the BEST of the higher levels rather than
on in-flight=4 specifically, so it protects the claim ("concurrency helps") without flaking on
which level happens to win.

### Action items

1. **Quotable claim:** Solr is ~2.9x on bulk insert, ~2.4x on update, ~9x on delete, ~1.5x on
   filter/sort, ~1.2x on the production query suite. Do NOT claim a win on the dashboard
   recent-promises query — it is a tie.
2. **Run benchmarks on a clean host.** Both engines need ~1.3 GiB resident. Below ~6 GiB of Docker
   VM, Elasticsearch fails to START (it stalls committing its heap) rather than running slowly —
   which reads as a harness bug, not a resource problem. Prune containers first.
3. **The in-flight lever is the biggest unexploited win** and production runs at
   `SOLR_MAX_IN_FLIGHT=1`. Unlocking it needs `_offset_ver` + `DocBasedVersionConstraints` in the
   `aggregated_user_data` configset AND the `-_deleted:true` read filter (soft-delete tombstones
   go live once the version guard is on). See `solr_wiring.md §8.1`.
4. **Backfill-only ingest settings are untuned.** For a blue/green re-stream into a collection
   nobody queries yet: `autoSoftCommit.maxTime=-1` (currently 5000 — opening searchers for a dark
   collection is pure waste), connector `commit.within.ms=0` (currently 5000, one scheduled commit
   per batch on top), and an `<indexConfig><ramBufferSizeMB>` block (absent, so Solr runs the
   100 MB default). Restore afterwards and commit once at the end.
5. **This suite measures the connector, not the pipeline.** It cannot see the deployed
   serialization points (single-partition output topic -> one sink task; the streamer
   pre-creating source topics at 1 partition). An end-to-end pipeline benchmark is still missing.

## 2026-08-30 — re-run, and the in-flight caveat that changes the headline

8 GiB Docker VM / 5 CPUs.

> ⚠️ **CONTENDED HOST — these margins are not clean-room.** The `objective_love_web` dev stack
> (Postgres/PostGIS, Solr, Neo4j, RabbitMQ, memcached — 5 containers) was resident throughout.
> The pre-run check that reported "0 containers" was run while the Docker daemon was still
> stopping, so a *failed* `docker ps` was misread as an empty host; the containers were confirmed
> afterwards. This is the same contamination as the 2026-08-14 run, which the 2026-08-23
> re-baseline showed had **inflated** the margins — ES degrades more under memory pressure than
> Solr does. So treat every ratio below as an **upper bound**, and prefer 2026-08-23 for
> Solr-vs-ES margins. What is NOT affected by contention is the finding this entry exists for:
> the in-flight=1 vs in-flight=4 comparison, since both columns were measured under the same
> load minutes apart.

Commands:

```bash
mvn -B -Pperf test -Dtest=SolrVsElasticsearchPerfTest                     # CRUD, default inflight=4
mvn -B -Pperf test -Dtest=SolrVsElasticsearchPerfTest -Dperf.inflight=1   # CRUD at PRODUCTION config
mvn -B -Pperf test -Dtest=SolrVsElasticsearchQueryPerfTest -Dperf.query=true
```

### The headline CRUD numbers are measured at `inFlight=4`; production runs `1`

`SolrVsElasticsearchPerfTest.IN_FLIGHT` defaults to **4**
(`Integer.getInteger("perf.inflight", 4)`), but production sets `SOLR_MAX_IN_FLIGHT=1` for
delete/upsert ordering. Every head-to-head table in this file above — including the 2026-08-23
baseline — therefore reports a **write** throughput the deployed system does not currently get.
Reads are unaffected (they do not touch the in-flight lever).

| Operation | Solr @ inflight=1 (**prod today**) | Solr @ inflight=4 | ES | prod margin | inflight=4 margin |
|---|---|---|---|---|---|
| SEED 20k | 1,224 ms (16,340 r/s) | 433 ms (46,189 r/s) | 1,835 / 1,600 ms | **1.50x** | 3.70x |
| UPDATE 20k | 1,083 ms (18,467 r/s) | 415 ms (48,193 r/s) | 1,771 / 1,075 ms | **1.64x** | 2.59x |
| FIND (200x) | 658 ms | 815 ms | 940 / 1,154 ms | **1.43x** | 1.42x |
| SORT (200x) | 726 ms | 817 ms | 1,137 / 1,222 ms | **1.57x** | 1.50x |
| DELETE 20k | 76 ms (263,158 r/s) | 39 ms (512,821 r/s) | 584 / 491 ms | **7.68x** | 12.59x |

**Quote the prod column.** Enabling the version guard is what converts it into the other one:
SEED 1.50x -> 3.70x and UPDATE 1.64x -> 2.59x are the measured value of that single change.

### In-flight scaling — first clean, monotonic run

| inFlight | 20k seed | throughput | vs 1 |
|---|---|---|---|
| 1 | 1,335 ms | 14,981 r/s | 1.00x |
| 4 | 595 ms | 33,613 r/s | **2.24x** |
| 8 | 504 ms | 39,683 r/s | **2.65x** |

No inversion, unlike 2026-08-23 run 2 (0.95x). Still ONE run — the "~2x, re-measure on real
hardware" guidance stands; this does not license quoting 2.65x.

### What the ordering guard costs (`VersionConstraintCostTest`, opt-in `-Dperf.versioncost=true`)

Measured directly, because the scaling figures below were taken with `_offset_ver` stamped but no
processor checking it. Two configsets identical apart from the processor
(`embedded-solr-versioned` vs `embedded-solr-versioned-nocheck`), same records, same `SolrWriter`,
paired and interleaved per round, 2 warmup rounds discarded, 7 measured rounds:

| round | chain OFF | chain ON | ratio |
|---|---|---|---|
| 1 | 552 ms | 984 ms | 1.78x |
| 2 | 576 ms | 956 ms | 1.66x |
| 3 | 525 ms | 914 ms | 1.74x |
| 4 | 596 ms | 988 ms | 1.66x |
| 5 | 571 ms | 827 ms | 1.45x |
| 6 | 498 ms | 790 ms | 1.59x |
| 7 | 516 ms | 859 ms | 1.66x |

**The ordering guard costs ~1.66x on the update path** (median of 7; range 1.45x-1.78x).

**This eats most of the in-flight win.** Gross 2.24x from concurrency, divided by 1.66x for the
guard, nets roughly **1.35x** — assuming the guard's cost is independent of the in-flight level.
It probably is not: the cost was measured on sequential embedded writes, and at in-flight 4 the
version lookups run concurrently, so the guard should get relatively cheaper. So treat **1.35x as
a lower bound and 2.24x as an upper bound** on what enabling `_offset_ver` + `SOLR_MAX_IN_FLIGHT=4`
actually delivers. Closing that gap needs one end-to-end run with BOTH enabled — which is the
measurement to do before committing to the change.

Three methodology traps this walked into, all worth avoiding on a re-run:

1. **JIT warmup was the dominant error.** Un-warmed, the same code gave 1.33x and 1.94x on two
   runs, and a 5-round paired run spread 1.35x-2.62x — round 1 ran ~4x slower in absolute terms
   than round 4. Discarding 2 warmup rounds collapsed the spread to 1.45x-1.78x. Pairing and
   interleaving alone did NOT fix it.
2. **`embedded-solr` is not a valid control.** It does not declare `_offset_ver`, so the stamp
   lands on the catch-all `*` dynamicField as a multiValued string instead of a plong — that arm
   is penalised by field typing, not by the processor. It produced an impossible **0.70x** ("the
   guard makes writes faster"). Hence the dedicated `-nocheck` control.
3. **Timing INSERTS understates the guard to nothing.** On a new id the version lookup is a miss;
   a controlled insert-only run measured 0.93x, inside noise. The processor only does real work
   when a stored version exists, so the test times a re-write of the same ids — which is also what
   production does continuously.

> ⚠️ **The scaling figures below are an UPPER BOUND: the version CHECK was not running.** The test stamps
> `_offset_ver` on every doc, but the perf core has no `DocBasedVersionConstraints` processor —
> that config lives only in `src/test/resources/embedded-solr-versioned/`, used by
> `SolrOrderingEmbeddedTest`. In production the processor does a per-document lookup of the
> existing doc's version before each write, which is real write-path overhead this run never
> paid. The delivered gain from enabling in-flight=4 is therefore **below 2.24x by an unmeasured
> margin**. Measure it by pointing the perf core at the versioned configset before quoting a
> post-enablement number.

### Query suite (50k docs, 300 iters) — reproduces the 2026-08-23 shape

| Query | Solr | ES | today | 2026-08-23 |
|---|---|---|---|---|
| Q1 top-level filter + sort | 4.45 ms | 5.67 ms | 1.27x | 1.26x |
| Q2 custom-field filter + sort (flat vs nested) | 3.68 ms | 4.94 ms | 1.34x | 1.14x |
| Q3 numeric range + sort | 3.69 ms | 4.31 ms | 1.17x | 1.53x |
| Q4 dashboard recent promises | 4.36 ms | 4.37 ms | **1.00x — tie** | 0.99x — tie |
| **Average** | **4.04 ms** | **4.82 ms** | **1.19x** | 1.21x |

Average is stable across two independent runs (1.19x vs 1.21x). **Q4 is now a confirmed tie on two
runs** — the standing "do not claim a win on dashboard recent-promises" holds. Q2/Q3 moved in
opposite directions by more than their gap, so per-query figures remain single-run noise; only the
average is quotable.

### Action items

1. **Correct the quotable claim.** As deployed today: Solr is ~1.5x on bulk insert, ~1.6x on
   update, ~7.7x on delete, ~1.4-1.6x on filter/sort, ~1.2x on the production query suite. The
   ~2.9x/~2.4x write figures previously quoted require `SOLR_MAX_IN_FLIGHT=4`, which is not on.
2. **`_offset_ver` remains the single highest-value change** and now has a price tag: it roughly
   doubles write throughput. It must be declared **`plong`** in the `aggregated_user_data`
   configset — the catch-all `*` dynamicField would type it `string`, which
   `DocBasedVersionConstraints` will not accept.
3. Items 2-5 from 2026-08-23 (clean host, backfill-only ingest settings, and the missing
   end-to-end pipeline benchmark) are unchanged and still open.
4. **The default free-text search path is NOT benchmarked — the biggest coverage gap.** Q1-Q4 are
   an exact term filter, an exact custom-field term, a numeric range and an exists check. None is
   a leading-wildcard substring, which is what `solr_query_builder.contains()` emits for
   `search_anything` and what production runs by default (`SOLR_TEXT_SEARCH_NGRAM=false`). Its own
   code comment records ~10-25x higher tail latency, degrading with scale. ES uses a `wildcard`
   query for the same thing so it is not obvious either engine wins — it is simply **unmeasured**,
   and it is the query an admin actually types. Add a Q5 before quoting a query-suite margin as
   representative of real search.
5. **Averages only.** Every query figure here is a mean over 300 iterations. Leading-wildcard
   latency is a TAIL problem, so a mean would hide it even if Q5 existed. Report p95/p99.

### In-flight sweep — locating the optimum (2026-08-31)

The optimum is a property of the **Solr host**, roughly its CPU core count, so it is measured not
assumed. Sweep any levels in ONE run (same containers, same conditions — far more comparable than
separate runs on a noisy host):

```bash
mvn -B -Pperf test -Dtest=SolrVsElasticsearchPerfTest#writeThroughputScalesWithInFlightRequests \
    -Dperf.guard=true -Dperf.records=50000 -Dperf.inflight.levels=4,5,6,7
```

`-Dperf.guard=true` installs `_offset_ver` + DocBasedVersionConstraints via the Schema/Config APIs
and **verifies it rejects a stale write before timing anything** — without that check a silently
inactive guard reports itself as free.

**Result on a 5-CPU / 8 GiB box, 5 sweeps x 50k records, guard active:**

| inFlight | median | mean | min-max | spread | won |
|---|---|---|---|---|---|
| 4 | 996 ms | 1039 ms | 701-1471 | 2.1x | **0/5** |
| **5** | **666 ms** | **683 ms** | **615-783** | **1.3x** | 2/5 |
| 6 | 769 ms | 834 ms | 671-1097 | 1.6x | 1/5 |
| 7 | 757 ms | 737 ms | 512-1006 | 2.0x | 2/5 |

**The step change is between 4 and 5 — exactly at the core count.** 5/6/7 are one plateau within
noise; 4 sits alone below it and never won a single run. 5 is also the most *consistent* level
measured (spread 1.3x vs 2.1x at 4): a wide spread at 4 means it is sometimes plateau-fast and
sometimes not, which is what a setting that cannot keep the pipeline full looks like.

A coarser sweep (1,2,4,6,8,12,16) agreed: 1/2/4 formed a slow group (1.0-1.6x) and 6/8/12/16 a fast
plateau (2.4-2.9x), with 4 never winning there either.

**Rule: in-flight = Solr's CPU core count.** Do not copy the literal 5 to other hardware, and do
not use 4 on a 5-core host. Beyond the plateau there is nothing to gain and memory/GC headroom to
lose. Single levels are NOT separable on a contended host — read the grouping, not the ranking.

### Verify this yourself — do not take the tables above on trust

**Are the `inflight=4` numbers wrong?** No. They are real measurements of a real configuration —
just not the one that is deployed. Reads are unaffected either way; only writes move:

| Op | quoted (inflight=4) | actual (inflight=1) | overstated by |
|---|---|---|---|
| SEED | 3.70x | 1.50x | **2.47x** |
| UPDATE | 2.59x | 1.64x | **1.58x** |
| DELETE | 12.59x | 7.68x | **1.64x** |
| FIND | 1.42x | 1.43x | 0.99x — unaffected |
| SORT | 1.50x | 1.57x | 0.96x — unaffected |

`4` was not arbitrary either: it matches `KAFKA_TOPIC_PARTITIONS` / `tasks.max`. The defect is
publishing a write margin without naming the in-flight it was measured at.

**Step 1 — confirm what production runs.** All three must agree or the prod column does not apply.

| # | Check | Expect |
|---|---|---|
| 1 | `admin_core_lib/config/admin_core_lib.yaml` -> `max_in_flight` | `${oc.env:SOLR_MAX_IN_FLIGHT,1}` i.e. **1** |
| 2 | `scripts/debezium_setup.py` -> the `max_in_flight) > 1` guard | raises unless `offset_version_field` is set |
| 3 | live worker: `GET /connectors/solr-sink/config` | `max.in.flight.requests` = `"1"`, `kafka.offset.version.field` absent |

If (3) shows `"4"`, read the inflight=4 column instead — and confirm `_offset_ver` is a **plong**
via `GET /solr/aggregated-user-data/schema/fields/_offset_ver` (the catch-all `*` dynamicField
would make it `string`, which `DocBasedVersionConstraints` rejects).

**Step 2 — confirm what the benchmark measures.** Its default is 4, NOT production's 1:

    grep -n 'perf.inflight' src/test/java/com/una/kafka/connect/solr/perf/SolrVsElasticsearchPerfTest.java
    # -> IN_FLIGHT = Integer.getInteger("perf.inflight", 4)

**Step 3 — reproduce both columns.** Needs Docker with **>= 6 GiB** and no other containers (below
6 GiB Elasticsearch fails to START rather than running slowly, which reads as a harness bug).
Check the host with `docker info --format '{{.MemTotal}} {{.NCPU}}'` and `docker ps -q | wc -l`;
these runs used 8 GiB / 5 CPUs / 0 containers.

    mvn -B -Pperf test -Dtest=SolrVsElasticsearchPerfTest -Dperf.inflight=1   # PRODUCTION config
    mvn -B -Pperf test -Dtest=SolrVsElasticsearchPerfTest                     # default 4
    mvn -B -Pperf test -Dtest=SolrVsElasticsearchQueryPerfTest -Dperf.query=true

Every run prints the in-flight it used. **That header line, not this file, is the source of truth**
for which column you are reading:

    [HEAD-TO-HEAD] records=20000 batch=500 inFlight=1 queries=200

**Step 4 — check the shape, not the digits.** Margins move with host contention; tens of percent
between runs is normal. What must hold: Solr wins every CRUD op, reads are insensitive to
in-flight, Q4 is a tie, and the query average sits near 1.2x. A run where Q4 shows Solr winning, or
where FIND/SORT move materially with `-Dperf.inflight`, means the harness or host is wrong — not a
discovery.
