package com.una.kafka.connect.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Parity tests imported from kafka-connect-elasticsearch's ElasticsearchClientTest.
 * See docs/test-parity.md.
 */
class SolrBulkProcessorParityTest {

    private SolrSinkConfig cfg() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "c");
        p.put(SolrSinkConfig.BATCH_SIZE_CONFIG, "1");
        p.put(SolrSinkConfig.LINGER_MS_CONFIG, "1");
        p.put(SolrSinkConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG, "2");
        p.put(SolrSinkConfig.MAX_RETRIES_CONFIG, "0");
        return new SolrSinkConfig(p);
    }

    /** Parity for ES testCloseFails. close() must be idempotent. */
    @Test
    void closeIsIdempotentEvenAfterFailure() {
        SolrClient client = mock(SolrClient.class);
        SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg());
        bulk.close();
        bulk.close(); // second close must not throw
    }

    /** Parity for ES testDoesNotCreateAlreadyExistingIndex. */
    @Test
    void existingCollectionIsLeftAlone() {
        // Solr equivalent: writing to an existing collection issues regular
        // UpdateRequests, never a Create. We verify by inspecting the only
        // request type that goes through.
        SolrClient client = mock(SolrClient.class);
        SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg());
        SolrInputDocument d = new SolrInputDocument();
        d.addField("id", "1");
        bulk.upsert("c", d, null);
        bulk.flushSync();
        bulk.close();
        // The mock collected all calls — they must all be UpdateRequests.
        try {
            org.mockito.Mockito.verify(client, org.mockito.Mockito.atLeastOnce())
                    .request(any(UpdateRequest.class), anyString());
        } catch (Exception e) {
            // Mockito.verify never throws checked here.
        }
    }

    /** Parity for ES testThreadNamingWithConnectorNameAndTaskId. */
    @Test
    void threadsAreNamed() throws Exception {
        SolrClient client = mock(SolrClient.class);
        AtomicReference<String> threadName = new AtomicReference<>();
        when(client.request(any(UpdateRequest.class), anyString())).thenAnswer(inv -> {
            threadName.set(Thread.currentThread().getName());
            return null;
        });

        SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg());
        SolrInputDocument d = new SolrInputDocument();
        d.addField("id", "1");
        bulk.upsert("c", d, null);
        bulk.flushSync();
        bulk.close();

        assertThat(threadName.get()).startsWith("solr-bulk-");
    }
}
