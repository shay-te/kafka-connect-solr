package com.una.kafka.connect.solr;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.solr.client.solrj.SolrClient;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Coverage tests for SolrSinkTask paths not exercised by the functional test:
 * partition revoke iteration, stop with null fields, flush() with offsets,
 * async preCommit path.
 */
class SolrSinkTaskCoverageTest {

    private static class TestTask extends SolrSinkTask {
        private final SolrClient client;
        private final SolrWriter writer;
        TestTask(SolrClient client, SolrWriter writer) { this.client = client; this.writer = writer; }
        @Override protected SolrClient createClient(SolrSinkConfig config) { return client; }
        @Override protected SolrWriter createWriter(SolrClient c, SolrSinkConfig cfg, OffsetTracker tracker) { return writer; }
    }

    private Map<String, String> baseProps() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "users");
        return p;
    }

    @Test
    void asyncPreCommitDelegatesToOffsetTracker() {
        Map<String, String> props = baseProps();
        props.put(SolrSinkConfig.FLUSH_SYNCHRONOUSLY_CONFIG, "false");
        SolrWriter writer = mock(SolrWriter.class);
        TestTask task = new TestTask(mock(SolrClient.class), writer);
        task.start(props);

        Map<TopicPartition, OffsetAndMetadata> current = new HashMap<>();
        current.put(new TopicPartition("users", 0), new OffsetAndMetadata(10L));
        Map<TopicPartition, OffsetAndMetadata> out = task.preCommit(current);

        verify(writer).flushAsync();
        verify(writer, never()).flush();
        // AsyncOffsetTracker has empty pending - returns currentOffsets verbatim.
        assertThat(out).isEqualTo(current);
        task.stop();
    }

    @Test
    void streamingModeForcesSyncPath() {
        Map<String, String> props = baseProps();
        // streaming=true + flush.synchronously=false would otherwise pick async; sync wins.
        props.put(SolrSinkConfig.STREAMING_ENABLED_CONFIG, "true");
        props.put(SolrSinkConfig.FLUSH_SYNCHRONOUSLY_CONFIG, "false");
        SolrWriter writer = mock(SolrWriter.class);
        TestTask task = new TestTask(mock(SolrClient.class), writer);
        task.start(props);

        Map<TopicPartition, OffsetAndMetadata> current = new HashMap<>();
        current.put(new TopicPartition("users", 0), new OffsetAndMetadata(10L));
        Map<TopicPartition, OffsetAndMetadata> out = task.preCommit(current);

        verify(writer).flush();
        assertThat(out).isEqualTo(current);
        task.stop();
    }

    @Test
    void flushDelegatesToWriter() {
        SolrWriter writer = mock(SolrWriter.class);
        TestTask task = new TestTask(mock(SolrClient.class), writer);
        task.start(baseProps());

        task.flush(Collections.emptyMap());
        verify(writer).flush();
        task.stop();
    }

    @Test
    void closeForwardsEachRevokedPartition() {
        SolrWriter writer = mock(SolrWriter.class);
        TestTask task = new TestTask(mock(SolrClient.class), writer);
        task.start(baseProps());

        TopicPartition tp0 = new TopicPartition("users", 0);
        TopicPartition tp1 = new TopicPartition("users", 1);
        task.close(Arrays.asList(tp0, tp1));

        verify(writer).partitionRevoked(tp0);
        verify(writer).partitionRevoked(tp1);
        task.stop();
    }

    @Test
    void closeIsNoopBeforeStart() {
        // close() before start() - both writer and offsetTracker are null. Should not throw.
        SolrSinkTask task = new SolrSinkTask();
        task.close(Arrays.asList(new TopicPartition("users", 0)));
        task.stop();
    }

    @Test
    void stopSwallowsOffsetTrackerCloseFailures() throws Exception {
        SolrWriter writer = mock(SolrWriter.class);
        TestTask task = new TestTask(mock(SolrClient.class), writer) {
            @Override
            protected SolrWriter createWriter(SolrClient c, SolrSinkConfig cfg, OffsetTracker tracker) {
                // Wrap the tracker to throw on close.
                OffsetTracker throwing = new OffsetTracker() {
                    @Override public OffsetState track(org.apache.kafka.connect.sink.SinkRecord r) { return null; }
                    @Override public Map<TopicPartition, OffsetAndMetadata> safeOffsets(Map<TopicPartition, OffsetAndMetadata> cur) { return cur; }
                    @Override public void closePartition(TopicPartition p) {}
                    @Override public void close() { throw new RuntimeException("boom"); }
                };
                return super.createWriter(c, cfg, throwing);
            }
        };
        task.start(baseProps());
        // Should not propagate the exception.
        task.stop();
        verify(writer).close();
    }

    @Test
    void versionStringIsNonEmpty() {
        assertThat(new SolrSinkTask().version()).isNotEmpty();
    }
}
