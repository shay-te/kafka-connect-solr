package com.una.kafka.connect.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * Drives SolrBulkProcessor's dry-run upsert and delete paths — when dryRun=true
 * the executor schedules the work but never calls the SolrClient.
 */
class SolrBulkProcessorDryRunTest {

    private SolrSinkConfig cfg(Map<String, String> extras) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "c");
        p.put(SolrSinkConfig.DRY_RUN_CONFIG, "true");
        p.putAll(extras);
        return new SolrSinkConfig(p);
    }

    private static SolrInputDocument doc(String id) {
        SolrInputDocument d = new SolrInputDocument();
        d.addField("id", id);
        return d;
    }

    @Test
    void dryRunUpsertDoesNotCallSolrClient() throws Exception {
        SolrClient client = mock(SolrClient.class);
        SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg(new HashMap<>()));
        bulk.upsert("c", doc("u1"), null);
        bulk.upsert("c", doc("u2"), null);
        bulk.flushSync();
        // No requests issued because dryRun short-circuits sendUpsert.
        verifyNoInteractions(client);
        assertThat(bulk.recordsWritten()).isEqualTo(2L);
        bulk.close();
    }

    @Test
    void dryRunDeleteDoesNotCallSolrClient() throws Exception {
        SolrClient client = mock(SolrClient.class);
        SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg(new HashMap<>()));
        bulk.delete("c", "d1", null);
        bulk.delete("c", "d2", null);
        bulk.flushSync();
        verifyNoInteractions(client);
        assertThat(bulk.recordsWritten()).isEqualTo(2L);
        bulk.close();
    }

    @Test
    void dryRunEmptyBatchHandlesGracefully() throws Exception {
        SolrClient client = mock(SolrClient.class);
        SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg(new HashMap<>()));
        // No upserts at all — flushSync is a no-op.
        bulk.flushSync();
        verifyNoInteractions(client);
        bulk.close();
    }

    @Test
    void closeIsIdempotent() throws Exception {
        SolrClient client = mock(SolrClient.class);
        SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg(new HashMap<>()));
        bulk.close();
        // Second close must not throw — the executor is already shut down.
        bulk.close();
    }
}
