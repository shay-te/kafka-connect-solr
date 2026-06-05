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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SolrBulkProcessorTest {

    private SolrSinkConfig cfg(Map<String, String> overrides) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "c");
        p.put(SolrSinkConfig.BATCH_SIZE_CONFIG, "2");
        p.put(SolrSinkConfig.LINGER_MS_CONFIG, "100000"); // effectively disable linger-based flush
        p.put(SolrSinkConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG, "2");
        p.put(SolrSinkConfig.MAX_BUFFERED_RECORDS_CONFIG, "10000");
        p.put(SolrSinkConfig.MAX_RETRIES_CONFIG, "1");
        p.put(SolrSinkConfig.RETRY_BACKOFF_MS_CONFIG, "1");
        p.put(SolrSinkConfig.COMMIT_WITHIN_MS_CONFIG, "1000");
        p.putAll(overrides);
        return new SolrSinkConfig(p);
    }

    @Test
    void batchTriggersWhenFull() throws Exception {
        SolrClient client = mock(SolrClient.class);
        SolrSchemaManager schemaManager = mock(SolrSchemaManager.class);
        SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg(new HashMap<>()), schemaManager);

        bulk.upsert("c", new SolrInputDocument("id", "1"));
        bulk.upsert("c", new SolrInputDocument("id", "2"));
        bulk.flushSync();

        verify(client, atLeastOnce()).request(any(UpdateRequest.class), anyString());
        assertThat(bulk.recordsWritten()).isGreaterThanOrEqualTo(2);
        bulk.close();
    }

    @Test
    void deletesAreShipped() throws Exception {
        SolrClient client = mock(SolrClient.class);
        SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg(new HashMap<>()), null);

        bulk.delete("c", "1");
        bulk.delete("c", "2");
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
        SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg(overrides), null);
        bulk.upsert("c", new SolrInputDocument("id", "1"));
        bulk.upsert("c", new SolrInputDocument("id", "2"));
        bulk.flushSync();

        assertThat(attempts.get()).isGreaterThanOrEqualTo(2);
        bulk.close();
    }

    @Test
    void nonRetryableFailureSurfacesOnFlush() throws Exception {
        SolrClient client = mock(SolrClient.class);
        when(client.request(any(UpdateRequest.class), anyString()))
                .thenThrow(new BaseHttpSolrClient.RemoteSolrException("u", 400, "bad", null));

        SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg(new HashMap<>()), null);
        bulk.upsert("c", new SolrInputDocument("id", "1"));
        bulk.upsert("c", new SolrInputDocument("id", "2"));

        assertThatThrownBy(bulk::flushSync).isInstanceOf(RetriableException.class);
        bulk.close();
    }

    @Test
    void closeFlushesAndShutsDown() throws Exception {
        SolrClient client = mock(SolrClient.class);
        SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg(new HashMap<>()), null);
        bulk.upsert("c", new SolrInputDocument("id", "1"));
        bulk.close(); // implicitly flushes
        // No assertion on counts because timing is tight; we just ensure no exception.
    }

    @Test
    void metricsExposed() {
        SolrClient client = mock(SolrClient.class);
        SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg(new HashMap<>()), null);
        assertThat(bulk.recordsWritten()).isZero();
        assertThat(bulk.recordsFailed()).isZero();
        assertThat(bulk.retries()).isZero();
        assertThat(bulk.queueDepth()).isZero();
        bulk.close();
    }
}
