package com.una.kafka.connect.solr;

import org.apache.kafka.common.TopicPartition;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Configuration edges of {@link SolrBulkProcessor} against a real in-process Solr: an oversized
 * {@code commit.within.ms}, byte capping switched off, and ordering lanes that were already shut
 * down when a flush is attempted.
 */
class SolrBulkProcessorConfigEdgesEmbeddedTest {

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
        p.put(SolrSinkConfig.LINGER_MS_CONFIG, "600000");
        for (int i = 0; i < kv.length; i += 2) p.put(kv[i], kv[i + 1]);
        return new SolrSinkConfig(p);
    }

    private static SolrInputDocument order(String id, long total) {
        SolrInputDocument d = new SolrInputDocument();
        d.addField("id", id);
        d.addField("customer.name", "Customer " + id);
        d.addField("customer.address.city", "Jerusalem");
        d.addField("line_items.sku", Arrays.asList("SKU-1", "SKU-2", "SKU-3"));
        d.addField("line_items.qty", Arrays.asList(1L, 2L, 5L));
        d.addField("height_cm", total);
        return d;
    }

    private static OffsetState state(long offset) {
        return new OffsetState(new TopicPartition("orders", 0), offset);
    }

    private long found(String q) throws Exception {
        solr.commit(CORE);
        return solr.query(CORE, new SolrQuery(q)).getResults().getNumFound();
    }

    @Test
    void commitWithinBeyondIntRangeIsClampedAndStillAccepted() throws Exception {
        try (SolrBulkProcessor bulk = new SolrBulkProcessor(solr, config(
                SolrSinkConfig.COMMIT_WITHIN_MS_CONFIG, Long.toString(Integer.MAX_VALUE + 1000L)))) {
            OffsetState s = state(1);
            bulk.upsert(CORE, order("o1", 300L), s);
            bulk.flushSync();
            assertThat(s.isAcked()).isTrue();
            assertThat(found("id:o1")).isEqualTo(1);
        }
    }

    @Test
    void byteCapDisabledBatchesOnRecordCountAlone() throws Exception {
        try (SolrBulkProcessor bulk = new SolrBulkProcessor(solr, config(
                SolrSinkConfig.BULK_SIZE_BYTES_CONFIG, "0",
                SolrSinkConfig.BATCH_SIZE_CONFIG, "3"))) {
            bulk.upsert(CORE, order("b1", 1L), state(1));
            bulk.upsert(CORE, order("b2", 2L), state(2));
            assertThat(bulk.hasBuffered()).as("below batch.size, no byte cap").isTrue();
            assertThat(bulk.batchCount()).isZero();
            bulk.upsert(CORE, order("b3", 3L), state(3)); // third record fills the batch
            bulk.flushSync();
            assertThat(bulk.batchCount()).isEqualTo(1);
            assertThat(found("id:b*")).isEqualTo(3);
        }
    }

    @Test
    void aNegativeMaxRetriesIsRejectedWhenTheConfigIsBuilt() {
        // Not only in Validator: a task started without validation used to accept it and then fail
        // every batch without ever reaching Solr (the retry loop ran zero times).
        assertThatThrownBy(() -> config(SolrSinkConfig.MAX_RETRIES_CONFIG, "-1"))
                .isInstanceOf(org.apache.kafka.common.config.ConfigException.class)
                .hasMessageContaining(SolrSinkConfig.MAX_RETRIES_CONFIG);
        assertThatThrownBy(() -> config(SolrSinkConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG, "0"))
                .isInstanceOf(org.apache.kafka.common.config.ConfigException.class)
                .hasMessageContaining(SolrSinkConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG);
    }

    @Test
    void zeroMaxRetriesSendsOnceAndDoesNotRetry() throws Exception {
        try (SolrBulkProcessor bulk = new SolrBulkProcessor(solr, config(
                SolrSinkConfig.MAX_RETRIES_CONFIG, "0",
                SolrSinkConfig.BATCH_SIZE_CONFIG, "10"))) {
            OffsetState s = state(1);
            bulk.upsert(CORE, order("r1", 5L), s);
            bulk.flushSync();
            assertThat(bulk.solrCallCount()).isEqualTo(1);
            assertThat(s.isAcked()).isTrue();
            assertThat(found("id:r1")).isEqualTo(1);
        }
    }

    @Test
    void flushAfterLanesWereShutDownIsRejectedAndKeepsTheRecords() throws Exception {
        SolrBulkProcessor bulk = new SolrBulkProcessor(solr, config(
                SolrSinkConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG, "2",
                SolrSinkConfig.ORDERING_LANES_ENABLED_CONFIG, "true",
                SolrSinkConfig.BATCH_SIZE_CONFIG, "1"));
        assertThat(bulk.laneCount()).isEqualTo(2);
        bulk.close();

        OffsetState s = state(1);
        assertThatThrownBy(() -> bulk.upsert(CORE, order("late", 9L), s))
                .isInstanceOf(RejectedExecutionException.class);
        // Put back, not dropped: preCommit must not commit past it.
        assertThat(bulk.hasBuffered()).isTrue();
        assertThat(s.isAcked()).isFalse();
        assertThat(found("id:late")).isZero();
    }
}
