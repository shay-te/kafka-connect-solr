package com.una.kafka.connect.solr.perf;

import com.una.kafka.connect.solr.SolrBulkProcessor;
import com.una.kafka.connect.solr.SolrSinkConfig;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Stress test for the in-memory bulk pipeline. The Solr client is mocked to
 * sleep a configurable amount of time, simulating remote latency so the
 * concurrency layer is actually exercised.
 *
 * Opt in via {@code mvn test -Pperf}.
 */
@Tag("performance")
class SolrBulkProcessorStressTest {

    private static SolrInputDocument doc(int i) {
        SolrInputDocument d = new SolrInputDocument();
        d.addField("id", String.valueOf(i));
        d.addField("first_name", "n" + i);
        d.addField("age", i % 80);
        return d;
    }

    private SolrSinkConfig cfg(int batchSize, int inFlight, int maxBuffer) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "c");
        p.put(SolrSinkConfig.BATCH_SIZE_CONFIG, String.valueOf(batchSize));
        p.put(SolrSinkConfig.LINGER_MS_CONFIG, "5");
        p.put(SolrSinkConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG, String.valueOf(inFlight));
        p.put(SolrSinkConfig.MAX_BUFFERED_RECORDS_CONFIG, String.valueOf(maxBuffer));
        p.put(SolrSinkConfig.MAX_RETRIES_CONFIG, "0");
        p.put(SolrSinkConfig.COMMIT_WITHIN_MS_CONFIG, "1000");
        p.put(SolrSinkConfig.FLUSH_TIMEOUT_MS_CONFIG, "120000");
        return new SolrSinkConfig(p);
    }

    @Test
    void concurrentBatchesBeatSequentialUnderLatency() throws Exception {
        final int records = 50_000;
        final int batchSize = 1_000;
        final long simulatedRttMs = 5; // pretend each Solr round-trip costs 5 ms

        SolrClient client = mock(SolrClient.class);
        AtomicLong totalServed = new AtomicLong();
        AtomicInteger concurrent = new AtomicInteger();
        AtomicInteger peakConcurrent = new AtomicInteger();
        when(client.request(any(UpdateRequest.class), anyString())).thenAnswer(inv -> {
            int now = concurrent.incrementAndGet();
            peakConcurrent.updateAndGet(p -> Math.max(p, now));
            try {
                Thread.sleep(simulatedRttMs);
            } finally {
                concurrent.decrementAndGet();
            }
            totalServed.incrementAndGet();
            return null;
        });

        SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg(batchSize, 8, 20_000), null);
        long t0 = System.nanoTime();
        for (int i = 0; i < records; i++) {
            bulk.upsert("c", doc(i), null);
        }
        bulk.flushSync();
        long ns = System.nanoTime() - t0;
        bulk.close();

        double seconds = ns / 1_000_000_000.0;
        double rps = records / seconds;
        long expectedBatches = (records + batchSize - 1) / batchSize;
        double serialLowerBound = expectedBatches * simulatedRttMs / 1000.0;
        System.out.printf("SolrBulkProcessor stress: %d records in %.2fs (%.0f r/s), "
                        + "batches served=%d, peak concurrency=%d, serial lower bound=%.2fs%n",
                records, seconds, rps, totalServed.get(), peakConcurrent.get(), serialLowerBound);

        assertThat(totalServed.get()).isGreaterThanOrEqualTo(expectedBatches);
        // Concurrency must actually exceed 1.
        assertThat(peakConcurrent.get()).isGreaterThanOrEqualTo(2);
        // Wall time must be meaningfully less than serial; allow generous slack for CI.
        assertThat(seconds).isLessThan(serialLowerBound * 0.6);
    }

    @Test
    void backpressureDoesNotDeadlock() throws Exception {
        SolrClient client = mock(SolrClient.class);
        when(client.request(any(UpdateRequest.class), anyString())).thenAnswer(inv -> {
            Thread.sleep(2);
            return null;
        });

        SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg(500, 4, 1_000), null);
        // Push way more than maxBuffer to force back-pressure via inflight queue.
        for (int i = 0; i < 20_000; i++) {
            bulk.upsert("c", doc(i), null);
        }
        bulk.flushSync();
        bulk.close();
        // If we got here, no deadlock.
    }
}
