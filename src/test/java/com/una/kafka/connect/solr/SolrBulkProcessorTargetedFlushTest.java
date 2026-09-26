package com.una.kafka.connect.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Verifies that hitting a per-buffer threshold flushes ONLY that collection's buffer —
 * not every collection's buffer. Without this guarantee, one hot collection forces every
 * cold collection to issue a tiny Solr request on each of its flushes, killing throughput
 * for multi-collection routing workloads.
 */
class SolrBulkProcessorTargetedFlushTest {

    private SolrSinkConfig cfg(Map<String, String> overrides) {
        Map<String, String> props = new HashMap<>();
        props.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        props.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "c");
        props.put(SolrSinkConfig.BATCH_SIZE_CONFIG, "3");
        // Set linger far away so it doesn't fire during the test.
        props.put(SolrSinkConfig.LINGER_MS_CONFIG, "60000");
        props.put(SolrSinkConfig.MAX_BUFFERED_RECORDS_CONFIG, "1000000");
        props.put(SolrSinkConfig.MAX_RETRIES_CONFIG, "0");
        props.put(SolrSinkConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG, "2");
        props.putAll(overrides);
        return new SolrSinkConfig(props);
    }

    private static SolrInputDocument doc(String id) {
        SolrInputDocument d = new SolrInputDocument();
        d.addField("id", id);
        return d;
    }

    @Test
    void hotCollectionThresholdDoesNotFlushColdCollection() throws Exception {
        SolrClient client = mock(SolrClient.class);
        SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg(new HashMap<>()));

        // Cold collection: just 1 doc, well below the batch.size=3 threshold.
        bulk.upsert("cold", doc("cold-1"), null);

        // Hot collection: 3 docs, hits the threshold and should flush — but ONLY itself.
        bulk.upsert("hot", doc("hot-1"), null);
        bulk.upsert("hot", doc("hot-2"), null);
        bulk.upsert("hot", doc("hot-3"), null);

        // Poll until the hot batch's records are accounted for (no Thread.sleep race).
        awaitRecordsWritten(bulk, 3);
        verify(client, atLeastOnce()).request(any(UpdateRequest.class), eq("hot"));
        verify(client, never()).request(any(UpdateRequest.class), eq("cold"));

        // After explicit flushSync, BOTH collections flush.
        bulk.flushSync();
        verify(client, atLeastOnce()).request(any(UpdateRequest.class), eq("cold"));
        bulk.close();
    }

    @Test
    void globalFlushAsyncSweepsEveryNonEmptyBuffer() throws Exception {
        // The targeted-flush change must NOT regress the global-flush contract:
        // explicit flushAsync() / flushSync() / preCommit() must still flush every
        // non-empty buffer, otherwise preCommit can't reliably commit offsets.
        SolrClient client = mock(SolrClient.class);
        SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg(new HashMap<>()));

        bulk.upsert("a", doc("a-1"), null);
        bulk.upsert("b", doc("b-1"), null);
        bulk.upsert("c", doc("c-1"), null);

        bulk.flushSync();   // global flush
        verify(client, atLeastOnce()).request(any(UpdateRequest.class), eq("a"));
        verify(client, atLeastOnce()).request(any(UpdateRequest.class), eq("b"));
        verify(client, atLeastOnce()).request(any(UpdateRequest.class), eq("c"));
        bulk.close();
    }

    @Test
    void deleteThresholdAlsoTargetedPerCollection() throws Exception {
        SolrClient client = mock(SolrClient.class);
        SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg(new HashMap<>()));

        bulk.delete("cold", "x-1", null);   // 1 of 3, below threshold

        bulk.delete("hot", "y-1", null);
        bulk.delete("hot", "y-2", null);
        bulk.delete("hot", "y-3", null);    // triggers per-buffer flush

        awaitRecordsWritten(bulk, 3);
        verify(client, atLeastOnce()).request(any(UpdateRequest.class), eq("hot"));
        verify(client, never()).request(any(UpdateRequest.class), eq("cold"));

        bulk.flushSync();
        verify(client, atLeastOnce()).request(any(UpdateRequest.class), eq("cold"));
        bulk.close();
    }

    /** Poll until the processor has written at least {@code target} records, or fail after 5 s. */
    private static void awaitRecordsWritten(SolrBulkProcessor bulk, long target) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5_000L;
        while (bulk.recordsWritten() < target && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        long observed = bulk.recordsWritten();
        if (observed < target) {
            throw new AssertionError(
                    "Timed out waiting for " + target + " records; observed " + observed);
        }
    }
}
