package com.una.kafka.connect.solr;

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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Locks in the counters that used to never increment - regression net for
 * the dead-counter bug.
 */
class CounterMetricsTest {

    private SolrSinkConfig cfg(Map<String, String> overrides) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "c");
        p.put(SolrSinkConfig.BATCH_SIZE_CONFIG, "1");
        p.put(SolrSinkConfig.LINGER_MS_CONFIG, "1");
        p.put(SolrSinkConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG, "1");
        p.put(SolrSinkConfig.RETRY_BACKOFF_MS_CONFIG, "1");
        p.putAll(overrides);
        return new SolrSinkConfig(p);
    }

    private static SolrInputDocument doc(String id) {
        SolrInputDocument d = new SolrInputDocument();
        d.addField("id", id);
        return d;
    }

    @Test
    void retriesCounterAdvancesOnTransientFailure() throws Exception {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.MAX_RETRIES_CONFIG, "3");
        SolrClient client = mock(SolrClient.class);
        AtomicInteger attempts = new AtomicInteger();
        when(client.request(any(UpdateRequest.class), anyString())).thenAnswer(inv -> {
            if (attempts.incrementAndGet() <= 2) {
                throw new BaseHttpSolrClient.RemoteSolrException("u", 503, "down", null);
            }
            return null;
        });

        SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg(o));
        bulk.upsert("c", doc("1"), null);
        bulk.flushSync();

        // Two retries happened before the third call succeeded.
        assertThat(bulk.retries()).isEqualTo(2L);
        assertThat(bulk.recordsFailed()).isZero();
        bulk.close();
    }

    @Test
    void failedCounterAdvancesWhenRetriesExhaust() throws Exception {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.MAX_RETRIES_CONFIG, "1");
        SolrClient client = mock(SolrClient.class);
        try {
            when(client.request(any(UpdateRequest.class), anyString()))
                    .thenThrow(new BaseHttpSolrClient.RemoteSolrException("u", 503, "down", null));
        } catch (Exception ignored) {
            // mock setup
        }

        SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg(o));
        bulk.upsert("c", doc("1"), null);
        bulk.upsert("c", doc("2"), null);

        // flushSync rethrows on the first failure; the second batch may still be
        // in flight. close() then drains everything (its internal flushSync
        // catches), so by the time we assert both batches have finished and
        // mutated recordsFailed.
        assertThatThrownBy(bulk::flushSync).isInstanceOf(RuntimeException.class);
        bulk.close();

        assertThat(bulk.recordsFailed()).isGreaterThanOrEqualTo(2L);
        assertThat(bulk.retries()).isGreaterThanOrEqualTo(1L);
    }

    @Test
    void nonRetryableFailureCountsAsFailedNoRetries() {
        SolrClient client = mock(SolrClient.class);
        try {
            when(client.request(any(UpdateRequest.class), anyString()))
                    .thenThrow(new BaseHttpSolrClient.RemoteSolrException("u", 400, "bad", null));
        } catch (Exception ignored) {
            // mock setup
        }

        SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg(new HashMap<>()));
        bulk.upsert("c", doc("1"), null);

        assertThatThrownBy(bulk::flushSync).isInstanceOf(RuntimeException.class);
        assertThat(bulk.recordsFailed()).isGreaterThanOrEqualTo(1L);
        assertThat(bulk.retries()).isZero();
        bulk.close();
    }
}
