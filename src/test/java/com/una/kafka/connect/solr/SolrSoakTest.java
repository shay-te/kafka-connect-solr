package com.una.kafka.connect.solr;

import org.apache.kafka.common.TopicPartition;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Soak test: sustained upsert + delete load through the real {@link SolrBulkProcessor} against
 * in-process Solr, asserting the leak-prone resources stay BOUNDED — the inflight future queue
 * (the leak that was fixed) and the worker-thread count. Raw heap isn't the signal (the Lucene
 * index legitimately grows); unbounded threads/futures are.
 *
 * <p>Opt-in (heavy): {@code -DsoakTest=true}. Default RECORDS=500k.
 */
@EnabledIfSystemProperty(named = "soakTest", matches = "true")
class SolrSoakTest {

    private static final int RECORDS = Integer.getInteger("soak.records", 500_000);

    private SolrSinkConfig config() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://embedded/solr");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, EmbeddedSolrSupport.CORE);
        p.put(SolrSinkConfig.BATCH_SIZE_CONFIG, "500");
        p.put(SolrSinkConfig.FLUSH_SYNCHRONOUSLY_CONFIG, "false"); // async == the leak-prone path
        return new SolrSinkConfig(p);
    }

    private static SolrInputDocument doc(int i) {
        SolrInputDocument d = new SolrInputDocument();
        d.addField("id", "u" + i);
        d.addField("first_name", "First" + (i % 1000));
        d.addField("age", 18 + (i % 60));
        d.addField("city", "City" + (i % 500));
        return d;
    }

    @Test
    void sustainedLoadKeepsThreadsAndInflightBounded() throws Exception {
        EmbeddedSolrServer solr = EmbeddedSolrSupport.start();
        try (SolrBulkProcessor bulk = new SolrBulkProcessor(solr, config())) {
            int baselineThreads = Thread.activeCount();
            long peakInflight = 0;

            for (int i = 0; i < RECORDS; i++) {
                bulk.upsert(EmbeddedSolrSupport.CORE, doc(i),
                        new OffsetState(new TopicPartition("users", 0), i));
                if ((i & 1023) == 0) {
                    bulk.flushAsync();                 // async flush — reaps completed futures
                    bulk.delete(EmbeddedSolrSupport.CORE, "u" + (i / 2),
                            new OffsetState(new TopicPartition("users", 0), i));
                }
                if ((i % 50_000) == 0) {
                    peakInflight = Math.max(peakInflight, bulk.inflightQueueSize());
                    // Neither futures nor threads may grow with the record count.
                    assertThat(bulk.inflightQueueSize())
                            .as("inflight future queue must stay bounded at record " + i)
                            .isLessThan(200);
                    assertThat(Thread.activeCount())
                            .as("worker threads must not leak at record " + i)
                            .isLessThan(baselineThreads + 40);
                }
            }
            bulk.flushSync();
            solr.commit(EmbeddedSolrSupport.CORE);

            assertThat(bulk.inflightQueueSize())
                    .as("after final drain the inflight queue must be ~empty").isLessThan(20);
            assertThat(Thread.activeCount())
                    .as("no thread leak after %,d records", RECORDS).isLessThan(baselineThreads + 40);
            assertThat(bulk.recordsWritten()).isGreaterThan(0);

            System.out.printf("%n[SOAK] records=%,d  peakInflight=%d  finalInflight=%d  "
                            + "baselineThreads=%d  finalThreads=%d%n",
                    RECORDS, peakInflight, bulk.inflightQueueSize(),
                    baselineThreads, Thread.activeCount());
        } finally {
            EmbeddedSolrSupport.stop(solr);
        }
    }
}
