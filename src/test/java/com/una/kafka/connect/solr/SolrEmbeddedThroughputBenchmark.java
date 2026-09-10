package com.una.kafka.connect.solr;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Real Solr ingest throughput through the actual connector path (converter → batching →
 * flush → real Lucene index) against in-process {@link EmbeddedSolrServer}.
 *
 * <p><b>Caveat:</b> in-process, so there is NO HTTP round-trip — this measures the connector's
 * own CPU work (JSON→SolrInputDocument conversion, batching, doc building, Lucene indexing),
 * NOT network-bound production throughput, and it is NOT a Solr-vs-Elasticsearch comparison
 * (that needs Docker + {@code SolrVsElasticsearchPerfTest}). Opt-in: {@code -DsolrBench=true}.
 */
@EnabledIfSystemProperty(named = "solrBench", matches = "true")
class SolrEmbeddedThroughputBenchmark {

    private static final int WARMUP = 10_000;
    private static final int RECORDS = 100_000;

    private static final Schema SCHEMA = SchemaBuilder.struct()
            .field("first_name", Schema.STRING_SCHEMA)
            .field("last_name", Schema.STRING_SCHEMA)
            .field("age", Schema.INT32_SCHEMA)
            .field("active", Schema.BOOLEAN_SCHEMA)
            .field("city", Schema.STRING_SCHEMA)
            .field("score", Schema.FLOAT64_SCHEMA)
            .build();

    private static List<SinkRecord> build(int n, int startOffset) {
        List<SinkRecord> out = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            int id = startOffset + i;
            Struct s = new Struct(SCHEMA)
                    .put("first_name", "First" + id)
                    .put("last_name", "Last" + id)
                    .put("age", 18 + (id % 60))
                    .put("active", (id & 1) == 0)
                    .put("city", "City" + (id % 500))
                    .put("score", (id % 1000) / 10.0);
            out.add(new SinkRecord("perf", 0, Schema.STRING_SCHEMA, "u-" + id, SCHEMA, s, id));
        }
        return out;
    }

    private SolrSinkConfig config() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://embedded/solr");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, EmbeddedSolrSupport.CORE);
        p.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "UNUSED");
        p.put(SolrSinkConfig.SCHEMA_IGNORE_CONFIG, "true");
        p.put(SolrSinkConfig.BATCH_SIZE_CONFIG, "2000");
        return new SolrSinkConfig(p);
    }

    @Test
    void ingestThroughput() throws Exception {
        EmbeddedSolrServer solr = EmbeddedSolrSupport.start();
        try {
            SolrSinkConfig config = config();

            // Warm up JIT + caches (not measured).
            try (SolrWriter w = new SolrWriter(solr, config, new SyncOffsetTracker())) {
                for (SinkRecord r : build(WARMUP, 0)) w.write(r);
                w.flush();
            }
            solr.deleteByQuery(EmbeddedSolrSupport.CORE, "*:*");
            solr.commit(EmbeddedSolrSupport.CORE);

            List<SinkRecord> records = build(RECORDS, 1_000_000);

            long t0 = System.nanoTime();
            try (SolrWriter w = new SolrWriter(solr, config, new SyncOffsetTracker())) {
                for (SinkRecord r : records) w.write(r);
                w.flush();
            }
            long ingestNanos = System.nanoTime() - t0;

            long tc = System.nanoTime();
            solr.commit(EmbeddedSolrSupport.CORE);
            long commitNanos = System.nanoTime() - tc;

            long found = solr.query(EmbeddedSolrSupport.CORE, new SolrQuery("*:*")).getResults().getNumFound();

            double ingestMs = ingestNanos / 1_000_000.0;
            double recsPerSec = RECORDS / (ingestNanos / 1_000_000_000.0);
            System.out.printf(
                    "%n================ SOLR CONNECTOR — REAL IN-PROCESS INGEST ================%n"
                  + " records ingested : %,d  (verified in index: %,d)%n"
                  + " batch.size       : 2000   flush.synchronously=true%n"
                  + " ingest (write+flush): %,.0f ms   -> %,.0f records/sec%n"
                  + " final commit        : %,.0f ms%n"
                  + " NOTE: in-process (no HTTP) — measures connector CPU cost, not network-bound%n"
                  + "       production throughput, and is NOT a Solr-vs-ES comparison.%n"
                  + "========================================================================%n",
                    RECORDS, found, ingestMs, recsPerSec, commitNanos / 1_000_000.0);

            if (found != RECORDS) throw new AssertionError("expected " + RECORDS + " docs, indexed " + found);
        } finally {
            EmbeddedSolrSupport.stop(solr);
        }
    }
}
