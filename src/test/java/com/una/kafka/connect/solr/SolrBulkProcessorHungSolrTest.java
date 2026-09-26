package com.una.kafka.connect.solr;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateHttp2SolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link SolrBulkProcessor} through a real HTTP/2 SolrJ client against a Solr that accepts the
 * connection and never answers ({@link BlackholeSolrServer}). Every path here is the connector's
 * reaction to a hung server: the flush deadline, a worker that outlives close(), a task thread
 * interrupted during shutdown, and the streaming client's stalled queue.
 */
class SolrBulkProcessorHungSolrTest {

    private BlackholeSolrServer hung;

    @BeforeEach
    void setUp() throws Exception {
        hung = new BlackholeSolrServer();
    }

    @AfterEach
    void tearDown() throws Exception {
        hung.close();
        Thread.interrupted(); // never leak an interrupt into the next test
    }

    private SolrSinkConfig config(String... kv) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, hung.baseUrl());
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "users");
        p.put(SolrSinkConfig.BATCH_SIZE_CONFIG, "100");
        p.put(SolrSinkConfig.LINGER_MS_CONFIG, "600000");
        p.put(SolrSinkConfig.MAX_RETRIES_CONFIG, "0");
        for (int i = 0; i < kv.length; i += 2) p.put(kv[i], kv[i + 1]);
        return new SolrSinkConfig(p);
    }

    private static SolrInputDocument user(String id) {
        SolrInputDocument d = new SolrInputDocument();
        d.addField("id", id);
        d.addField("name", "user " + id);
        d.addField("address.city", "Tel Aviv");
        d.addField("tags", java.util.Arrays.asList("vip", "beta"));
        return d;
    }

    private static OffsetState state(long offset) {
        return new OffsetState(new TopicPartition("users", 0), offset);
    }

    @Test
    void flushDeadlineAlreadySpentIsRetriableAndRecordStaysUnacked() throws Exception {
        // 1 ms truncates to 0 ms left by the time the first in-flight future is polled.
        SolrSinkConfig cfg = config(
                SolrSinkConfig.FLUSH_TIMEOUT_MS_CONFIG, "1",
                SolrSinkConfig.READ_TIMEOUT_MS_CONFIG, "500");
        SolrClient client = SolrClientFactory.create(cfg);
        try (SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg)) {
            OffsetState s = state(1);
            bulk.upsert("users", user("u1"), s);
            assertThatThrownBy(bulk::flushSync)
                    .isInstanceOf(RetriableException.class)
                    .hasMessage("Solr flush timed out");
            assertThat(s.isAcked()).isFalse();
        } finally {
            client.close();
        }
    }

    @Test
    void waitingOnAHungBatchTimesOutAsARetriableFlushError() throws Exception {
        SolrSinkConfig cfg = config(
                SolrSinkConfig.FLUSH_TIMEOUT_MS_CONFIG, "150",
                SolrSinkConfig.READ_TIMEOUT_MS_CONFIG, "30000");
        SolrClient client = SolrClientFactory.create(cfg);
        try {
            SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg);
            OffsetState s = state(1);
            bulk.upsert("users", user("u1"), s);
            // f.get(remaining) itself times out: a hung Solr must be retried, not fail the task.
            assertThatThrownBy(bulk::flushSync)
                    .isInstanceOf(RetriableException.class)
                    .hasMessage("Solr flush timed out")
                    .hasCauseInstanceOf(TimeoutException.class);
            assertThat(s.isAcked()).isFalse();
            hung.close(); // release the stuck worker so close() does not wait out the grace
            bulk.close();
        } finally {
            client.close();
        }
    }

    @Test
    void anInterruptWhileWaitingOnAHungBatchIsRetriedAndKeepsTheInterrupt() throws Exception {
        SolrSinkConfig cfg = config(
                SolrSinkConfig.FLUSH_TIMEOUT_MS_CONFIG, "30000",
                SolrSinkConfig.READ_TIMEOUT_MS_CONFIG, "30000");
        SolrClient client = SolrClientFactory.create(cfg);
        try {
            SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg);
            OffsetState s = state(1);
            bulk.upsert("users", user("u1"), s);
            bulk.flushAsync();                     // the batch is in flight against the hung server
            Thread.currentThread().interrupt();    // Connect stopping the task mid-flush
            assertThatThrownBy(bulk::flushSync)
                    .isInstanceOf(RetriableException.class)
                    .hasMessage("Solr flush interrupted");
            assertThat(Thread.interrupted()).as("the interrupt must survive the flush").isTrue();
            assertThat(s.isAcked()).isFalse();
            hung.close();
            bulk.close();
        } finally {
            client.close();
        }
    }

    @Test
    void closeInterruptedWhileAWorkerIsStuckStopsThePoolAndKeepsTheInterrupt() throws Exception {
        SolrSinkConfig cfg = config(
                SolrSinkConfig.FLUSH_TIMEOUT_MS_CONFIG, "100",
                SolrSinkConfig.READ_TIMEOUT_MS_CONFIG, "30000");
        SolrClient client = SolrClientFactory.create(cfg);
        try {
            SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg);
            bulk.upsert("users", user("u1"), state(1));
            // The stuck future is polled (and dropped) by this failed flush, so close() has
            // nothing left to wait on and goes straight to awaitTermination.
            assertThatThrownBy(bulk::flushSync).isInstanceOf(ConnectException.class);

            // Connect interrupts a task thread that overruns its shutdown timeout.
            Thread.currentThread().interrupt();
            bulk.close();
            assertThat(Thread.interrupted()).as("close() must restore the interrupt").isTrue();
        } finally {
            client.close();
        }
    }

    @Test
    void closeGivesUpOnAWorkerThatOutlivesTheTerminationGrace() throws Exception {
        // Worker blocked for 30 s; close() waits its fixed 10 s grace, then shutdownNow().
        SolrSinkConfig cfg = config(
                SolrSinkConfig.FLUSH_TIMEOUT_MS_CONFIG, "100",
                SolrSinkConfig.READ_TIMEOUT_MS_CONFIG, "30000");
        SolrClient client = SolrClientFactory.create(cfg);
        try {
            SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg);
            OffsetState s = state(1);
            bulk.upsert("users", user("u1"), s);
            long start = System.nanoTime();
            bulk.close(); // final flush fails (logged), then the grace period expires
            long elapsedMs = (System.nanoTime() - start) / 1_000_000L;
            assertThat(elapsedMs).isGreaterThanOrEqualTo(10_000L);
            assertThat(s.isAcked()).isFalse();
            assertThat(bulk.recordsWritten()).isZero();
        } finally {
            client.close();
        }
    }

    @Test
    void stalledStreamingQueueFailsTheFlushAsRetriable() throws Exception {
        // SolrJ reads its stall threshold once per client; 600 ms keeps the test short.
        String previous = System.getProperty("solr.cloud.client.stallTime");
        System.setProperty("solr.cloud.client.stallTime", "600");
        SolrSinkConfig cfg = config(
                SolrSinkConfig.STREAMING_ENABLED_CONFIG, "true",
                SolrSinkConfig.STREAMING_THREADS_CONFIG, "1",
                SolrSinkConfig.READ_TIMEOUT_MS_CONFIG, "30000",
                SolrSinkConfig.BATCH_SIZE_CONFIG, "2");
        SolrClient client;
        try {
            client = SolrClientFactory.create(cfg);
        } finally {
            if (previous == null) System.clearProperty("solr.cloud.client.stallTime");
            else System.setProperty("solr.cloud.client.stallTime", previous);
        }
        assertThat(client).isInstanceOf(ConcurrentUpdateHttp2SolrClient.class);
        try {
            SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg);
            bulk.upsert("users", user("s1"), state(1));
            bulk.upsert("users", user("s2"), state(2)); // batch full -> handed to the stream
            assertThatThrownBy(bulk::flushSync)
                    .isInstanceOf(RetriableException.class)
                    .hasMessage("Solr streaming flush failed")
                    .hasRootCauseInstanceOf(java.io.IOException.class);
            hung.close(); // the server goes away: the stuck stream fails instead of idling out
            client.close();
            bulk.close();
        } finally {
            client.close();
        }
    }
}
