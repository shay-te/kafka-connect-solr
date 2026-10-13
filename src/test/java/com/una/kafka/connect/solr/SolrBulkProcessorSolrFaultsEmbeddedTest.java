package com.una.kafka.connect.solr;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link SolrBulkProcessor} against a real in-process Solr whose update chain fails some writes
 * the way a sick node does ({@link FaultInjectingUpdateProcessorFactory}): a 400 for a poison
 * document or delete, and a transient index I/O error. Covers a transient failure arriving in the
 * middle of poison isolation, delete runs that exhaust their retries, and isolation with
 * {@code commit.within.ms=0}.
 */
class SolrBulkProcessorSolrFaultsEmbeddedTest {

    private static final String CORE = EmbeddedSolrSupport.CORE;

    private EmbeddedSolrServer solr;

    @BeforeEach
    void setUp() throws Exception {
        solr = EmbeddedSolrSupport.start("embedded-solr-faults");
    }

    @AfterEach
    void tearDown() {
        EmbeddedSolrSupport.stop(solr);
    }

    private SolrSinkConfig config(String... kv) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://embedded/solr");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, CORE);
        p.put(SolrSinkConfig.BATCH_SIZE_CONFIG, "100");
        p.put(SolrSinkConfig.LINGER_MS_CONFIG, "600000");
        p.put(SolrSinkConfig.MAX_RETRIES_CONFIG, "1");
        p.put(SolrSinkConfig.RETRY_BACKOFF_MS_CONFIG, "1");
        p.put(SolrSinkConfig.COMMIT_WITHIN_MS_CONFIG, "0");
        for (int i = 0; i < kv.length; i += 2) p.put(kv[i], kv[i + 1]);
        return new SolrSinkConfig(p);
    }

    private static SolrInputDocument user(String id, String name) {
        SolrInputDocument d = new SolrInputDocument();
        d.addField("id", id);
        d.addField("name", name);
        d.addField("height_cm", 172L);
        d.addField("address.city", "Haifa");
        d.addField("custom_fields.label", Arrays.asList("tier", "region"));
        d.addField("custom_fields.value", Arrays.asList("gold", "north"));
        return d;
    }

    /** Converts fine; Solr's schema rejects it (height_cm is plong). */
    private static SolrInputDocument poison(String id) {
        SolrInputDocument d = user(id, "conflicted");
        d.setField("height_cm", "one-seventy-two");
        return d;
    }

    /** The index write for this document fails with an I/O error inside Solr. */
    private static SolrInputDocument ioFailure(String id) {
        SolrInputDocument d = user(id, "unlucky");
        d.addField("fault", "io");
        return d;
    }

    private static OffsetState state(long offset) {
        return new OffsetState(new TopicPartition("users", 0), offset);
    }

    private long found(String q) throws Exception {
        solr.commit(CORE);
        return solr.query(CORE, new SolrQuery(q)).getResults().getNumFound();
    }

    @Test
    void transientFailureDuringUpsertIsolationStopsTheRunAndLeavesTheRestUnacked() throws Exception {
        SolrBulkProcessor bulk = new SolrBulkProcessor(solr, config(
                SolrSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, "warn"));
        OffsetState healthy = state(1);
        OffsetState poisoned = state(2);
        OffsetState transientFail = state(3);
        OffsetState neverSent = state(4);
        bulk.upsert(CORE, user("h1", "Ada"), healthy);
        bulk.upsert(CORE, poison("p1"), poisoned);
        bulk.upsert(CORE, ioFailure("t1"), transientFail);
        bulk.upsert(CORE, user("h2", "Grace"), neverSent);

        assertThatThrownBy(bulk::flushSync).isInstanceOf(RetriableException.class);

        assertThat(healthy.isAcked()).isTrue();
        assertThat(poisoned.isAcked()).as("poison is dropped and acked").isTrue();
        assertThat(transientFail.isAcked()).as("redelivered by Connect").isFalse();
        assertThat(neverSent.isAcked()).as("after the failure, never sent").isFalse();
        assertThat(bulk.recordsWritten()).isEqualTo(1);
        assertThat(bulk.recordsFailed()).isEqualTo(3); // poison + the two unprocessed
        assertThat(bulk.queueDepth()).isZero();
        assertThat(bulk.retries()).isEqualTo(1);
        assertThat(found("id:h1")).isEqualTo(1);
        assertThat(found("id:h2")).isZero();
        assertThat(found("id:p1")).isZero();
        bulk.close();
    }

    @Test
    void transientFailureDuringDeleteIsolationStopsTheRun() throws Exception {
        SolrBulkProcessor bulk = new SolrBulkProcessor(solr, config(
                SolrSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, "warn"));
        bulk.upsert(CORE, user("d1", "to delete"), state(1));
        bulk.flushSync();
        assertThat(found("id:d1")).isEqualTo(1);

        OffsetState deleted = state(2);
        OffsetState refused = state(3);
        OffsetState transientFail = state(4);
        bulk.delete(CORE, "d1", deleted);
        bulk.delete(CORE, "reject-7", refused);
        bulk.delete(CORE, "io-9", transientFail);

        assertThatThrownBy(bulk::flushSync).isInstanceOf(RetriableException.class);

        assertThat(deleted.isAcked()).isTrue();
        assertThat(refused.isAcked()).as("refused delete is dropped and acked").isTrue();
        assertThat(transientFail.isAcked()).isFalse();
        assertThat(found("id:d1")).isZero();
        assertThat(bulk.recordsFailed()).isEqualTo(2); // refused + transient
        bulk.close();
    }

    @Test
    void deleteRunThatExhaustsRetriesIsRetriableAndCountedFailed() throws Exception {
        SolrBulkProcessor bulk = new SolrBulkProcessor(solr, config());
        OffsetState s = state(1);
        bulk.delete(CORE, "io-1", s);
        assertThatThrownBy(bulk::flushSync)
                .isInstanceOf(RetriableException.class)
                .hasMessageContaining("Solr bulk request failed");
        assertThat(s.isAcked()).isFalse();
        assertThat(bulk.retries()).isEqualTo(1);
        assertThat(bulk.recordsFailed()).isEqualTo(1);
        assertThat(bulk.queueDepth()).isZero();
        bulk.close();
    }

    @Test
    void isolationWithoutOffsetStatesStillWritesTheHealthyDocAndDropsThePoison() throws Exception {
        // Callers that do not track offsets pass null states; isolation must not trip over them.
        SolrBulkProcessor bulk = new SolrBulkProcessor(solr, config(
                SolrSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, "ignore"));
        bulk.upsert(CORE, user("n1", "Linus"), null);
        bulk.upsert(CORE, poison("n2"), null);
        bulk.flushSync();
        assertThat(bulk.recordsWritten()).isEqualTo(1);
        assertThat(bulk.recordsFailed()).isEqualTo(1);
        assertThat(found("id:n1")).isEqualTo(1);
        assertThat(found("id:n2")).isZero();
        bulk.close();
    }

    @Test
    void aBatchThatFailedAsynchronouslyIsReapedAndSurfacedByTheNextFlush() throws Exception {
        // flushAsync never throws: the worker's failure is kept and raised by the NEXT flush, so an
        // offset is never committed over a write that failed after the send returned.
        try (SolrBulkProcessor bulk = new SolrBulkProcessor(solr, config(
                SolrSinkConfig.MAX_RETRIES_CONFIG, "0",
                SolrSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, "fail"))) {
            OffsetState failing = state(1);
            bulk.upsert(CORE, ioFailure("async-1"), failing);
            bulk.flushAsync();                                   // fails in the worker, throws nothing here
            for (int i = 0; i < 200 && bulk.recordsFailed() == 0; i++) {
                Thread.sleep(25);                                // let the worker finish before the reap
            }
            assertThat(bulk.recordsFailed()).isEqualTo(1);

            assertThatThrownBy(bulk::flushAsync)                 // reaps the finished, failed future
                    .isInstanceOf(org.apache.kafka.connect.errors.ConnectException.class);
            assertThat(failing.isAcked()).isFalse();
            assertThat(found("id:async-1")).isZero();
        }
    }
}
