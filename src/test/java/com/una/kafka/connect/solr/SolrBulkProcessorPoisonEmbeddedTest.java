package com.una.kafka.connect.solr;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Poison-document handling at FLUSH time against a REAL in-process Solr — no mocks.
 *
 * <p>A document can convert fine and only be rejected by Solr when the batch is sent (here: a
 * non-numeric value in the {@code height_cm} plong field — a genuine schema type conflict, a
 * non-retriable 400). Historically that ConnectException killed the task: ONE poison doc froze
 * ALL Solr indexing. With {@code behavior.on.malformed.documents=warn|ignore} the rejected run
 * must instead be re-sent op-by-op so healthy docs land, and only the poison doc is dropped —
 * counted in recordsFailed and its offset ACKED so the pipeline moves on. {@code fail} keeps
 * the old behavior (throw).
 */
class SolrBulkProcessorPoisonEmbeddedTest {

    private static final String CORE = EmbeddedSolrSupport.CORE;

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
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, CORE);
        for (int i = 0; i < kv.length; i += 2) p.put(kv[i], kv[i + 1]);
        return new SolrSinkConfig(p);
    }

    private static SolrInputDocument healthy(String id, String name) {
        SolrInputDocument d = new SolrInputDocument();
        d.addField("id", id);
        d.addField("name", name);
        d.addField("height_cm", 180L);
        return d;
    }

    /** Converts fine; Solr rejects it at flush: height_cm is plong in the real schema. */
    private static SolrInputDocument poison(String id) {
        SolrInputDocument d = new SolrInputDocument();
        d.addField("id", id);
        d.addField("name", "conflicted");
        d.addField("height_cm", "one-eighty");
        return d;
    }

    private static OffsetState state(long offset) {
        return new OffsetState(new TopicPartition("users", 0), offset);
    }

    private long numFound(String q) throws Exception {
        return solr.query(CORE, new SolrQuery(q)).getResults().getNumFound();
    }

    @Test
    void warnModeIsolatesPoisonDocSoHealthyDocsLandAndTaskSurvives() throws Exception {
        try (SolrBulkProcessor bulk = new SolrBulkProcessor(solr, config(
                SolrSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, "warn"))) {
            OffsetState s1 = state(1);
            OffsetState s2 = state(2);
            OffsetState s3 = state(3);
            bulk.upsert(CORE, healthy("h1", "Ada"), s1);
            bulk.upsert(CORE, poison("p1"), s2);
            bulk.upsert(CORE, healthy("h2", "Grace"), s3);

            bulk.flushSync(); // must NOT throw: the poison doc may not kill the task
            solr.commit(CORE);

            assertThat(numFound("id:h1")).as("healthy doc before the poison must land").isEqualTo(1);
            assertThat(numFound("id:h2")).as("healthy doc after the poison must land").isEqualTo(1);
            assertThat(numFound("id:p1")).as("the rejected doc must not be indexed").isEqualTo(0);

            assertThat(bulk.recordsWritten()).isEqualTo(2);
            assertThat(bulk.recordsFailed()).as("the poison doc is counted failed").isEqualTo(1);

            assertThat(s1.isAcked()).isTrue();
            assertThat(s3.isAcked()).isTrue();
            assertThat(s2.isAcked())
                    .as("poison offset must be acked so the pipeline moves past it").isTrue();
        }
    }

    @Test
    void ignoreModeSilentlyDropsPoisonAndContinues() throws Exception {
        try (SolrBulkProcessor bulk = new SolrBulkProcessor(solr, config(
                SolrSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, "ignore"))) {
            OffsetState s2 = state(2);
            bulk.upsert(CORE, healthy("h1", "Ada"), state(1));
            bulk.upsert(CORE, poison("p1"), s2);
            bulk.upsert(CORE, healthy("h2", "Grace"), state(3));

            bulk.flushSync();
            solr.commit(CORE);

            assertThat(numFound("id:h1")).isEqualTo(1);
            assertThat(numFound("id:h2")).isEqualTo(1);
            assertThat(numFound("id:p1")).isEqualTo(0);
            assertThat(bulk.recordsWritten()).isEqualTo(2);
            assertThat(bulk.recordsFailed()).isEqualTo(1);
            assertThat(s2.isAcked()).isTrue();
        }
    }

    @Test
    void failModeStillThrowsOnPoisonDocPinningCurrentBehavior() throws Exception {
        try (SolrBulkProcessor bulk = new SolrBulkProcessor(solr, config(
                SolrSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, "fail"))) {
            OffsetState s2 = state(2);
            bulk.upsert(CORE, healthy("h1", "Ada"), state(1));
            bulk.upsert(CORE, poison("p1"), s2);
            bulk.upsert(CORE, healthy("h2", "Grace"), state(3));

            // Solr 400 is permanent: non-retriable ConnectException surfaces (fail-fast / DLQ).
            assertThatThrownBy(bulk::flushSync)
                    .isInstanceOf(ConnectException.class)
                    .isNotInstanceOf(RetriableException.class);
            assertThat(bulk.recordsFailed()).isGreaterThanOrEqualTo(1);
            assertThat(s2.isAcked()).as("nothing acked; Connect redelivers").isFalse();
        }
    }

    @Test
    void poisonedUpsertRunDoesNotAbortTheFollowingDeleteRun() throws Exception {
        try (SolrBulkProcessor bulk = new SolrBulkProcessor(solr, config(
                SolrSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, "warn"))) {
            bulk.upsert(CORE, healthy("victim", "temp"), state(1));
            bulk.flushSync();
            solr.commit(CORE);
            assertThat(numFound("id:victim")).isEqualTo(1);

            // Ordered buffer [add poison][delete victim] -> two runs; isolating the poisoned
            // upsert run must not abort the delete run queued behind it.
            bulk.upsert(CORE, poison("p2"), state(2));
            bulk.delete(CORE, "victim", state(3));
            bulk.flushSync();
            solr.commit(CORE);

            assertThat(numFound("id:victim"))
                    .as("delete run behind the poisoned run must still apply").isEqualTo(0);
            assertThat(numFound("id:p2")).isEqualTo(0);
            assertThat(bulk.recordsFailed()).isEqualTo(1);
        }
    }

    @Test
    void warnModeDeleteRejectionFallsBackIdByIdAndAcks() throws Exception {
        // A real, permanently rejected delete run (no such core). WARN must isolate id-by-id,
        // drop each rejected delete, count it failed and ack it — the task survives.
        try (SolrBulkProcessor bulk = new SolrBulkProcessor(solr, config(
                SolrSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, "warn",
                SolrSinkConfig.MAX_RETRIES_CONFIG, "0"))) {
            OffsetState s1 = state(1);
            OffsetState s2 = state(2);
            bulk.delete("no_such_core", "a", s1);
            bulk.delete("no_such_core", "b", s2);

            bulk.flushSync(); // must NOT throw
            assertThat(bulk.recordsFailed()).isEqualTo(2);
            assertThat(s1.isAcked()).isTrue();
            assertThat(s2.isAcked()).isTrue();
        }
    }
}
