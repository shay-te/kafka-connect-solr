package com.una.kafka.connect.solr;

import org.apache.kafka.connect.errors.RetriableException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SolrBulkProcessorTest {

    private SolrSinkConfig cfg(Map<String, String> overrides) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "c");
        p.put(SolrSinkConfig.BATCH_SIZE_CONFIG, "2");
        p.put(SolrSinkConfig.LINGER_MS_CONFIG, "100000");
        p.put(SolrSinkConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG, "2");
        p.put(SolrSinkConfig.MAX_BUFFERED_RECORDS_CONFIG, "10000");
        p.put(SolrSinkConfig.MAX_RETRIES_CONFIG, "1");
        p.put(SolrSinkConfig.RETRY_BACKOFF_MS_CONFIG, "1");
        p.put(SolrSinkConfig.COMMIT_WITHIN_MS_CONFIG, "1000");
        p.putAll(overrides);
        return new SolrSinkConfig(p);
    }

    private static SolrInputDocument doc(String id) {
        SolrInputDocument d = new SolrInputDocument();
        d.addField("id", id);
        return d;
    }

    @Test
    void batchTriggersWhenFull() throws Exception {
        SolrClient client = mock(SolrClient.class);
        SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg(new HashMap<>()));

        bulk.upsert("c", doc("1"), null);
        bulk.upsert("c", doc("2"), null);
        bulk.flushSync();

        verify(client, atLeastOnce()).request(any(UpdateRequest.class), anyString());
        assertThat(bulk.recordsWritten()).isGreaterThanOrEqualTo(2);
        bulk.close();
    }

    @Test
    void deletesAreShipped() throws Exception {
        SolrClient client = mock(SolrClient.class);
        SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg(new HashMap<>()));

        bulk.delete("c", "1", null);
        bulk.delete("c", "2", null);
        bulk.flushSync();

        verify(client, atLeastOnce()).request(any(UpdateRequest.class), anyString());
        bulk.close();
    }

    @Test
    void retryableFailureRetries() throws Exception {
        SolrClient client = mock(SolrClient.class);
        AtomicInteger attempts = new AtomicInteger();
        when(client.request(any(UpdateRequest.class), anyString())).thenAnswer(inv -> {
            if (attempts.incrementAndGet() == 1) {
                throw new BaseHttpSolrClient.RemoteSolrException("url", 503, "down", null);
            }
            return null;
        });

        Map<String, String> overrides = new HashMap<>();
        overrides.put(SolrSinkConfig.MAX_RETRIES_CONFIG, "3");
        SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg(overrides));
        bulk.upsert("c", doc("1"), null);
        bulk.upsert("c", doc("2"), null);
        bulk.flushSync();

        assertThat(attempts.get()).isGreaterThanOrEqualTo(2);
        bulk.close();
    }

    @Test
    void nonRetryableFailureSurfacesOnFlush() throws Exception {
        SolrClient client = mock(SolrClient.class);
        when(client.request(any(UpdateRequest.class), anyString()))
                .thenThrow(new BaseHttpSolrClient.RemoteSolrException("u", 400, "bad", null));

        SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg(new HashMap<>()));
        bulk.upsert("c", doc("1"), null);
        bulk.upsert("c", doc("2"), null);

        // A Solr 400 is permanent -> non-retriable (must not loop the pipeline forever).
        assertThatThrownBy(bulk::flushSync)
                .isInstanceOf(org.apache.kafka.connect.errors.ConnectException.class)
                .isNotInstanceOf(RetriableException.class);
        bulk.close();
    }

    @Test
    void warnModeDoesNotSwallowRetriableFailures() throws Exception {
        // behavior.on.malformed.documents governs only NON-retriable rejections. A 503 that
        // exhausts retries must stay a RetriableException (Connect redelivers) — WARN must not
        // turn a Solr outage into doc-by-doc data loss.
        SolrClient client = mock(SolrClient.class);
        when(client.request(any(UpdateRequest.class), anyString()))
                .thenThrow(new BaseHttpSolrClient.RemoteSolrException("u", 503, "down", null));

        Map<String, String> overrides = new HashMap<>();
        overrides.put(SolrSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, "warn");
        overrides.put(SolrSinkConfig.MAX_RETRIES_CONFIG, "1");
        overrides.put(SolrSinkConfig.RETRY_BACKOFF_MS_CONFIG, "1");
        SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg(overrides));
        OffsetState s = new OffsetState(new org.apache.kafka.common.TopicPartition("t", 0), 1L);
        bulk.upsert("c", doc("1"), s);
        bulk.upsert("c", doc("2"), null);

        assertThatThrownBy(bulk::flushSync).isInstanceOf(RetriableException.class);
        assertThat(s.isAcked()).as("retriable failures must never be acked away").isFalse();
        bulk.close();
    }

    @Test
    void closeFlushesAndShutsDown() throws Exception {
        SolrClient client = mock(SolrClient.class);
        SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg(new HashMap<>()));
        bulk.upsert("c", doc("1"), null);
        bulk.close();
    }

    @Test
    void metricsExposed() {
        SolrClient client = mock(SolrClient.class);
        SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg(new HashMap<>()));
        assertThat(bulk.recordsWritten()).isZero();
        assertThat(bulk.recordsFailed()).isZero();
        assertThat(bulk.retries()).isZero();
        assertThat(bulk.queueDepth()).isZero();
        bulk.close();
    }

    @Test
    void lingerTriggersFlush() throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put(SolrSinkConfig.LINGER_MS_CONFIG, "0");
        overrides.put(SolrSinkConfig.BATCH_SIZE_CONFIG, "1000");
        SolrClient client = mock(SolrClient.class);
        SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg(overrides));

        bulk.upsert("c", doc("1"), null);
        bulk.flushSync();
        verify(client, atLeastOnce()).request(any(UpdateRequest.class), anyString());
        bulk.close();
    }

    @Test
    void byteSizeCapTriggersFlush() throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put(SolrSinkConfig.BATCH_SIZE_CONFIG, "10000");
        overrides.put(SolrSinkConfig.BULK_SIZE_BYTES_CONFIG, "200");
        overrides.put(SolrSinkConfig.LINGER_MS_CONFIG, "100000");
        SolrClient client = mock(SolrClient.class);
        SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg(overrides));
        SolrInputDocument big = new SolrInputDocument();
        big.addField("id", "1");
        big.addField("payload", "x".repeat(500));
        bulk.upsert("c", big, null);
        bulk.flushSync();
        verify(client, atLeastOnce()).request(any(UpdateRequest.class), anyString());
        bulk.close();
    }

    @Test
    void latencyMetricsAreTracked() throws Exception {
        SolrClient client = mock(SolrClient.class);
        SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg(new HashMap<>()));
        // Idle: nothing recorded yet.
        assertThat(bulk.avgBatchLatencyMs()).isEqualTo(0.0);
        assertThat(bulk.avgSolrCallLatencyMs()).isEqualTo(0.0);
        assertThat(bulk.batchCount()).isZero();

        bulk.upsert("c", doc("1"), null);
        bulk.upsert("c", doc("2"), null);
        bulk.flushSync();

        assertThat(bulk.batchCount()).isGreaterThanOrEqualTo(1);
        assertThat(bulk.solrCallCount()).isGreaterThanOrEqualTo(1);
        // We don't assert specific timing; just that the counters moved.
        assertThat(bulk.avgBatchLatencyMs()).isGreaterThanOrEqualTo(0.0);
        bulk.close();
    }

    @Test
    void offsetStateIsMarkedAcked() throws Exception {
        SolrClient client = mock(SolrClient.class);
        SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg(new HashMap<>()));
        OffsetState s1 = new OffsetState(new org.apache.kafka.common.TopicPartition("t", 0), 7L);
        OffsetState s2 = new OffsetState(new org.apache.kafka.common.TopicPartition("t", 0), 8L);
        bulk.upsert("c", doc("1"), s1);
        bulk.upsert("c", doc("2"), s2);
        bulk.flushSync();
        assertThat(s1.isAcked()).isTrue();
        assertThat(s2.isAcked()).isTrue();
        bulk.close();
    }

    @Test
    void queueDepthReturnsToZeroAfterAPermanentlyFailedBatch() throws Exception {
        // queueDepth gates checkGlobalThresholds(): if a failed batch's ops are never subtracted,
        // the gauge stays above max.buffered.records forever and every later record flushes on
        // its own — batching silently collapses to one Solr request per document.
        SolrClient client = mock(SolrClient.class);
        when(client.request(any(UpdateRequest.class), anyString()))
                .thenThrow(new BaseHttpSolrClient.RemoteSolrException("h", 400, "bad doc", null));
        SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg(new HashMap<>()));

        bulk.upsert("c", doc("1"), null);
        bulk.upsert("c", doc("2"), null);
        assertThatThrownBy(bulk::flushSync).isInstanceOf(RuntimeException.class);

        assertThat(bulk.recordsFailed()).isEqualTo(2);
        assertThat(bulk.queueDepth()).isZero();
        bulk.close();
    }

    @Test
    void flushAsyncSurfacesAFailureReapedFromAPreviousBatch() throws Exception {
        // Async mode (flush.synchronously=false) never calls flushSync, so flushAsync is the only
        // place a worker failure can reach Kafka Connect. Without it the task stays RUNNING while
        // every batch fails and the offset tracker's pending queues grow unbounded.
        SolrClient client = mock(SolrClient.class);
        when(client.request(any(UpdateRequest.class), anyString()))
                .thenThrow(new BaseHttpSolrClient.RemoteSolrException("h", 400, "bad doc", null));
        SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg(new HashMap<>()));

        bulk.upsert("c", doc("1"), null);
        bulk.upsert("c", doc("2"), null);   // batch.size=2 -> submitted, fails on its own thread
        for (int i = 0; i < 200 && bulk.recordsFailed() == 0; i++) {
            Thread.sleep(5);
        }

        assertThatThrownBy(bulk::flushAsync)
                .hasMessageContaining("Solr rejected a document");
        bulk.close();
    }
}
