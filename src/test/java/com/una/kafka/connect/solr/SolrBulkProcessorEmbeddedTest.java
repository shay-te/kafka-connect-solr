package com.una.kafka.connect.solr;

import org.apache.kafka.common.TopicPartition;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link SolrBulkProcessor} against a REAL in-process Solr ({@link EmbeddedSolrServer}) — no
 * mocks, no Docker. Documents are written, flushed, committed and then queried back out of a
 * genuine Lucene index to prove the write / delete / batch paths actually persist.
 */
class SolrBulkProcessorEmbeddedTest {

    private EmbeddedSolrServer solr;

    @BeforeEach
    void setUp() throws Exception {
        solr = EmbeddedSolrSupport.start();
    }

    @AfterEach
    void tearDown() {
        EmbeddedSolrSupport.stop(solr);
    }

    private SolrSinkConfig config(String... kv) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://embedded/solr");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, EmbeddedSolrSupport.CORE);
        for (int i = 0; i < kv.length; i += 2) p.put(kv[i], kv[i + 1]);
        return new SolrSinkConfig(p);
    }

    private static SolrInputDocument doc(String id, String name, Object age) {
        SolrInputDocument d = new SolrInputDocument();
        d.addField("id", id);
        d.addField("name", name);
        d.addField("age", age);
        return d;
    }

    private long numFound(String q) throws Exception {
        QueryResponse rsp = solr.query(EmbeddedSolrSupport.CORE, new SolrQuery(q));
        return rsp.getResults().getNumFound();
    }

    @Test
    void upsertsRealDocumentsAndTheyArePersistedAndQueryable() throws Exception {
        try (SolrBulkProcessor bulk = new SolrBulkProcessor(solr, config())) {
            OffsetState s1 = new OffsetState(new TopicPartition("users", 0), 1L);
            OffsetState s2 = new OffsetState(new TopicPartition("users", 0), 2L);
            bulk.upsert(EmbeddedSolrSupport.CORE, doc("u1", "Ada 💙", 36), s1);
            bulk.upsert(EmbeddedSolrSupport.CORE, doc("u2", "Grace", 40), s2);
            bulk.flushSync();
            solr.commit(EmbeddedSolrSupport.CORE);

            assertThat(numFound("*:*")).isEqualTo(2);
            assertThat(numFound("id:u1")).isEqualTo(1);
            // Offsets acked only after the real Solr call returned.
            assertThat(s1.isAcked()).isTrue();
            assertThat(s2.isAcked()).isTrue();
            assertThat(bulk.recordsWritten()).isEqualTo(2);
            assertThat(bulk.recordsFailed()).isEqualTo(0);

            // The unicode value round-tripped through a real Lucene index.
            QueryResponse rsp = solr.query(EmbeddedSolrSupport.CORE, new SolrQuery("id:u1"));
            assertThat(rsp.getResults().get(0).getFieldValues("name")).contains("Ada 💙");
        }
    }

    @Test
    void deleteRemovesAPreviouslyWrittenDocument() throws Exception {
        try (SolrBulkProcessor bulk = new SolrBulkProcessor(solr, config())) {
            bulk.upsert(EmbeddedSolrSupport.CORE, doc("gone", "temp", 1), null);
            bulk.flushSync();
            solr.commit(EmbeddedSolrSupport.CORE);
            assertThat(numFound("id:gone")).isEqualTo(1);

            bulk.delete(EmbeddedSolrSupport.CORE, "gone", new OffsetState(new TopicPartition("users", 0), 5L));
            bulk.flushSync();
            solr.commit(EmbeddedSolrSupport.CORE);
            assertThat(numFound("id:gone")).isEqualTo(0);
        }
    }

    @Test
    void batchSizeThresholdAutoFlushesToSolr() throws Exception {
        // batch.size=2 -> every 2 docs flush automatically without an explicit flushSync.
        try (SolrBulkProcessor bulk = new SolrBulkProcessor(solr, config(SolrSinkConfig.BATCH_SIZE_CONFIG, "2"))) {
            for (int i = 0; i < 6; i++) {
                bulk.upsert(EmbeddedSolrSupport.CORE, doc("b" + i, "n" + i, i),
                        new OffsetState(new TopicPartition("users", 0), i));
            }
            bulk.flushSync(); // drain any partial trailing batch
            solr.commit(EmbeddedSolrSupport.CORE);
            assertThat(numFound("*:*")).isEqualTo(6);
            assertThat(bulk.batchCount()).isGreaterThanOrEqualTo(3); // ≥3 flushes of 2
        }
    }

    @Test
    void writeToUnknownCollectionFailsForRealAndCountsTheFailure() throws Exception {
        // A real Solr error (no such core) — not a mocked throw — must surface on flush and
        // increment the failure counter, leaving the offset un-acked.
        try (SolrBulkProcessor bulk = new SolrBulkProcessor(solr, config(SolrSinkConfig.MAX_RETRIES_CONFIG, "0"))) {
            OffsetState s = new OffsetState(new TopicPartition("users", 0), 3L);
            bulk.upsert("no_such_core", doc("x", "y", 1), s);
            // No-such-core is permanent -> non-retriable (retrying can never create the core).
            org.assertj.core.api.Assertions.assertThatThrownBy(bulk::flushSync)
                    .isInstanceOf(org.apache.kafka.connect.errors.ConnectException.class)
                    .isNotInstanceOf(org.apache.kafka.connect.errors.RetriableException.class);
            assertThat(bulk.recordsFailed()).isGreaterThanOrEqualTo(1);
            assertThat(s.isAcked()).isFalse(); // never acked -> Kafka Connect will redeliver
        }
    }

    @Test
    void deleteFromUnknownCollectionSurfacesTheError() throws Exception {
        try (SolrBulkProcessor bulk = new SolrBulkProcessor(solr, config(SolrSinkConfig.MAX_RETRIES_CONFIG, "0"))) {
            bulk.delete("no_such_core", "id1", new OffsetState(new TopicPartition("users", 0), 4L));
            org.assertj.core.api.Assertions.assertThatThrownBy(bulk::flushSync)
                    .isInstanceOf(org.apache.kafka.connect.errors.ConnectException.class)
                    .isNotInstanceOf(org.apache.kafka.connect.errors.RetriableException.class);
            assertThat(bulk.recordsFailed()).isGreaterThanOrEqualTo(1);
        }
    }

    @Test
    void dryRunDoesNotWriteToSolr() throws Exception {
        try (SolrBulkProcessor bulk = new SolrBulkProcessor(solr, config(SolrSinkConfig.DRY_RUN_CONFIG, "true"))) {
            OffsetState s = new OffsetState(new TopicPartition("users", 0), 9L);
            bulk.upsert(EmbeddedSolrSupport.CORE, doc("dry", "nope", 1), s);
            bulk.flushSync();
            solr.commit(EmbeddedSolrSupport.CORE);

            assertThat(numFound("*:*")).isEqualTo(0);  // nothing hit the index
            assertThat(s.isAcked()).isTrue();           // but the offset is still acked (dry-run "succeeds")
            assertThat(bulk.recordsWritten()).isEqualTo(1);
        }
    }

    @Test
    void asyncFlushDoesNotLeakCompletedFutures() throws Exception {
        // Regression: in async mode only flushSync drained the inflight queue, so flushAsync()
        // let completed futures pile up forever — a slow leak. flushAsync now reaps them.
        try (SolrBulkProcessor bulk = new SolrBulkProcessor(solr, config(SolrSinkConfig.BATCH_SIZE_CONFIG, "1"))) {
            for (int r = 0; r < 300; r++) {
                bulk.upsert(EmbeddedSolrSupport.CORE, doc("leak" + r, "n" + r, r),
                        new OffsetState(new TopicPartition("users", 0), r));
                bulk.flushAsync();   // async path — never flushSync during the loop
            }
            // Let the workers finish; each flushAsync reaps the completed futures.
            for (int i = 0; i < 60 && bulk.inflightQueueSize() > 5; i++) {
                Thread.sleep(50);
                bulk.flushAsync();
            }
            assertThat(bulk.inflightQueueSize())
                    .as("300 async flushes must not leave ~300 dead futures in the queue")
                    .isLessThan(10);
        }
    }
}
