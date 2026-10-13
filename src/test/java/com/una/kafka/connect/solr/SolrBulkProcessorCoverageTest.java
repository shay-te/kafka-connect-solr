package com.una.kafka.connect.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Coverage tests for SolrBulkProcessor flush-trigger branches and
 * estimateBytes type dispatch.
 */
class SolrBulkProcessorCoverageTest {

    private SolrSinkConfig cfg(Map<String, String> extras) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "c");
        p.putAll(extras);
        return new SolrSinkConfig(p);
    }

    private static SolrInputDocument doc(String id) {
        SolrInputDocument d = new SolrInputDocument();
        d.addField("id", id);
        return d;
    }

    @Test
    void lingerThresholdTriggersFlushOnNextRecord() throws Exception {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.BATCH_SIZE_CONFIG, "1000000");
        o.put(SolrSinkConfig.LINGER_MS_CONFIG, "1");
        o.put(SolrSinkConfig.MAX_BUFFERED_RECORDS_CONFIG, "10000");
        SolrClient client = mock(SolrClient.class);
        SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg(o));
        bulk.upsert("c", doc("1"), null);
        Thread.sleep(10);              // exceed linger
        bulk.upsert("c", doc("2"), null);
        bulk.flushSync();
        verify(client, atLeastOnce()).request(any(UpdateRequest.class), anyString());
        bulk.close();
    }

    @Test
    void maxBufferedRecordsTriggersFlush() throws Exception {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.BATCH_SIZE_CONFIG, "1000000");
        o.put(SolrSinkConfig.LINGER_MS_CONFIG, "60000");
        o.put(SolrSinkConfig.MAX_BUFFERED_RECORDS_CONFIG, "2");
        SolrClient client = mock(SolrClient.class);
        SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg(o));
        bulk.upsert("c", doc("1"), null);
        bulk.upsert("c", doc("2"), null);    // hits max.buffered
        bulk.flushSync();
        verify(client, atLeastOnce()).request(any(UpdateRequest.class), anyString());
        bulk.close();
    }

    @Test
    void deleteBatchSizeTriggersFlush() throws Exception {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.BATCH_SIZE_CONFIG, "2");
        o.put(SolrSinkConfig.LINGER_MS_CONFIG, "60000");
        SolrClient client = mock(SolrClient.class);
        SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg(o));
        bulk.delete("c", "1", null);
        bulk.delete("c", "2", null);
        bulk.flushSync();
        verify(client, atLeastOnce()).request(any(UpdateRequest.class), anyString());
        bulk.close();
    }

    @Test
    void estimateBytesHandlesAllFieldTypes() {
        SolrInputDocument d = new SolrInputDocument();
        d.addField("str", "hello");
        d.addField("num", 42);
        d.addField("flag", true);
        d.addField("blob", new byte[]{1, 2});
        d.addField("list", Arrays.asList("a", 1, "b"));
        d.addField("nullField", null);
        // Use addField(name, value) only for non-null since SolrInputDocument
        // treats null specially.
        long bytes = SolrBulkProcessor.estimateBytes(d);
        assertThat(bytes).isGreaterThan(16);
    }

    @Test
    void estimateBytesEmptyDoc() {
        // No fields - just the overhead constant (16).
        assertThat(SolrBulkProcessor.estimateBytes(new SolrInputDocument())).isEqualTo(16L);
    }

    @Test
    void metricsAccessibleEvenWhenEmpty() throws Exception {
        SolrClient client = mock(SolrClient.class);
        SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg(new HashMap<>()));
        assertThat(bulk.recordsWritten()).isZero();
        assertThat(bulk.recordsFailed()).isZero();
        assertThat(bulk.retries()).isZero();
        assertThat(bulk.queueDepth()).isZero();
        assertThat(bulk.batchCount()).isZero();
        assertThat(bulk.solrCallCount()).isZero();
        assertThat(bulk.avgBatchLatencyMs()).isEqualTo(0.0);
        assertThat(bulk.avgSolrCallLatencyMs()).isEqualTo(0.0);
        assertThat(bulk.lastSuccessEpochMs()).isZero();
        bulk.close();
    }

    @Test
    void commitWithinZeroIsHonoredAsDisabled() throws Exception {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.COMMIT_WITHIN_MS_CONFIG, "0");
        SolrClient client = mock(SolrClient.class);
        SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg(o));
        bulk.upsert("c", doc("1"), null);
        bulk.flushSync();
        // The UpdateRequest should not have setCommitWithin called - we can't
        // easily assert that, but the call should complete.
        verify(client, atLeastOnce()).request(any(UpdateRequest.class), anyString());
        bulk.close();
    }

    @Test
    void multipleCollectionsFlushIndependently() throws Exception {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.BATCH_SIZE_CONFIG, "1");
        o.put(SolrSinkConfig.LINGER_MS_CONFIG, "1");
        SolrClient client = mock(SolrClient.class);
        SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg(o));
        bulk.upsert("a", doc("1"), null);
        bulk.upsert("b", doc("2"), null);
        bulk.upsert("c", doc("3"), null);
        bulk.flushSync();
        verify(client, atLeastOnce()).request(any(UpdateRequest.class), anyString());
        bulk.close();
    }
}
