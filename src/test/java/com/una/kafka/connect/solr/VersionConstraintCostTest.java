package com.una.kafka.connect.solr;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * What does the ordering guard COST on the write path?
 *
 * <p>`SolrVsElasticsearchPerfTest` reports in-flight scaling (1 -> 4 = 2.24x) with `_offset_ver`
 * STAMPED but no `DocBasedVersionConstraints` processor actually checking it — its `perf` core is
 * schemaless `_default`. So that figure is an upper bound: enabling the guard in production adds a
 * per-document lookup of the stored version before every write, which that run never paid.
 *
 * <p>This isolates exactly that. Same records, same real {@link SolrWriter}, and two configsets
 * that are byte-identical apart from the processor: `embedded-solr-versioned-nocheck` (control)
 * vs `embedded-solr-versioned`. The control exists because comparing against the general-purpose
 * `embedded-solr` configset is NOT controlled — it does not declare `_offset_ver`, so the stamp
 * lands on the catch-all `*` dynamicField as a multiValued string instead of a plong, and that
 * arm gets penalised by field typing rather than by the processor. Doing it that way produced an
 * impossible 0.70x ("the guard makes writes faster"), which is how the flaw surfaced.
 * Embedded rather than containerised so no HTTP or scheduler noise sits between the arms.
 *
 * <p>Opt-in (it indexes tens of thousands of docs twice): {@code -Dperf.versioncost=true}.
 * Reports a ratio, asserts nothing — a threshold here would just encode one machine's timing.
 */
class VersionConstraintCostTest {

    private static final int RECORDS = Integer.getInteger("perf.versioncost.records", 20_000);
    private static final int REPEATS = Integer.getInteger("perf.versioncost.repeats", 7);
    /** Discarded rounds. The JVM has not compiled the write path yet: round 1 ran ~4x slower than
     *  round 4 on the same code, and that lands entirely in the ratio. */
    private static final int WARMUP = Integer.getInteger("perf.versioncost.warmup", 2);

    @Test
    void reportsWhatTheOrderingGuardCostsOnWrites() throws Exception {
        Assumptions.assumeTrue(Boolean.getBoolean("perf.versioncost"),
                "opt-in: -Dperf.versioncost=true");

        List<SinkRecord> records = records(RECORDS);
        // PAIRED and INTERLEAVED: measure OFF then ON inside the same round, and take the ratio
        // per round. Running all OFF reps then all ON reps lets host load drift between the two
        // blocks and land entirely in the ratio — that is what produced 1.33x and 1.94x on two
        // runs of the same code. Pairing cancels drift that is slower than one round.
        for (int round = 0; round < WARMUP; round++) {
            updatePassMs("embedded-solr-versioned-nocheck", records);
            updatePassMs("embedded-solr-versioned", records);
        }
        System.out.printf("[VERSION-GUARD COST] %d warmup round(s) discarded%n", WARMUP);

        List<Double> ratios = new ArrayList<>();
        for (int round = 0; round < REPEATS; round++) {
            long off = updatePassMs("embedded-solr-versioned-nocheck", records);
            long on = updatePassMs("embedded-solr-versioned", records);
            ratios.add(on / (double) off);
            System.out.printf("[VERSION-GUARD COST] round %d: off=%4d ms  on=%4d ms  ratio=%.2fx%n",
                    round + 1, off, on, on / (double) off);
        }
        Collections.sort(ratios);
        double median = ratios.get(ratios.size() / 2);

        System.out.printf("%n[VERSION-GUARD COST] records=%d rounds=%d (paired, interleaved)%n",
                RECORDS, REPEATS);
        System.out.printf("[VERSION-GUARD COST] UPDATE pass — re-write of the same %d ids%n", RECORDS);
        System.out.printf("[VERSION-GUARD COST] guard costs: median %.2fx  (range %.2fx - %.2fx)%n",
                median, ratios.get(0), ratios.get(ratios.size() - 1));
        System.out.printf("[VERSION-GUARD COST] -> in-flight 1->4 nets ~%.2fx once the guard is on "
                + "(2.24x measured without it)%n", 2.24 / median);
        if (ratios.get(ratios.size() - 1) - ratios.get(0) > 0.30) {
            System.out.println("[VERSION-GUARD COST] NOTE: spread > 0.30x — host is noisy; raise "
                    + "-Dperf.versioncost.repeats or quiet the machine before quoting a figure.");
        }
    }

    /**
     * One timed UPDATE pass: seed the ids (untimed), then time a re-write of the same ids.
     *
     * <p>The re-write is the point. On an insert the version lookup is a miss and costs almost
     * nothing — an insert-only measurement showed 0.93x, inside noise. The processor only does
     * real work when a stored version exists to compare against, and production re-writes the
     * same user ids continuously.
     */
    private static long updatePassMs(String configset, List<SinkRecord> records) throws Exception {
        EmbeddedSolrServer solr = EmbeddedSolrSupport.start(configset);
        try {
            writeAll(writer(solr), records);                // seed — not timed
            solr.commit(EmbeddedSolrSupport.CORE);

            List<SinkRecord> updates = bumpOffsets(records);
            SolrWriter writer = writer(solr);
            long started = System.nanoTime();
            writeAll(writer, updates);
            solr.commit(EmbeddedSolrSupport.CORE);
            return Math.max((System.nanoTime() - started) / 1_000_000, 1);
        } finally {
            EmbeddedSolrSupport.stop(solr);
        }
    }

    private static void writeAll(SolrWriter writer, List<SinkRecord> records) {
        for (SinkRecord record : records) {
            writer.write(record);
        }
        writer.flush();
    }

    /** Same ids at higher offsets: every write is now a version comparison against a stored doc. */
    private static List<SinkRecord> bumpOffsets(List<SinkRecord> records) {
        List<SinkRecord> out = new ArrayList<>(records.size());
        for (SinkRecord r : records) {
            out.add(new SinkRecord(r.topic(), r.kafkaPartition(), r.keySchema(), r.key(),
                    r.valueSchema(), r.value(), r.kafkaOffset() + records.size()));
        }
        return out;
    }

    private static SolrWriter writer(EmbeddedSolrServer solr) {
        Map<String, String> props = new LinkedHashMap<>();
        props.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://embedded/solr");
        props.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, EmbeddedSolrSupport.CORE);
        // Stamped in BOTH arms, so the field write itself cancels out and only the check differs.
        props.put(SolrSinkConfig.KAFKA_OFFSET_VERSION_FIELD_CONFIG, "_offset_ver");
        props.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "UNUSED");
        props.put(SolrSinkConfig.SCHEMA_IGNORE_CONFIG, "true");
        props.put(SolrSinkConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG, "delete");
        return new SolrWriter(solr, new SolrSinkConfig(props), new SyncOffsetTracker());
    }

    private static List<SinkRecord> records(int count) {
        List<SinkRecord> out = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String id = "user-" + i;
            Map<String, Object> value = new LinkedHashMap<>();
            value.put("id", id);
            value.put("first_name", "name-" + i);
            value.put("status", i % 5);
            value.put("height", 150 + (i % 50));
            out.add(new SinkRecord("aggregated-user-data-v1", 0, Schema.STRING_SCHEMA, id,
                    null, value, i));
        }
        return out;
    }
}
