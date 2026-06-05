package com.una.kafka.connect.solr;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.client.solrj.SolrClient;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class SolrSinkTaskTest {

    private static class TestTask extends SolrSinkTask {
        private final SolrClient client;
        private final SolrWriter writer;
        TestTask(SolrClient client, SolrWriter writer) { this.client = client; this.writer = writer; }
        @Override protected SolrClient createClient(SolrSinkConfig config) { return client; }
        @Override protected SolrWriter createWriter(SolrClient c, SolrSinkConfig cfg) { return writer; }
    }

    private Map<String, String> baseProps() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "users");
        return p;
    }

    @Test
    void versionIsExposed() {
        SolrSinkTask t = new SolrSinkTask();
        assertThat(t.version()).isNotEmpty();
    }

    @Test
    void putWritesEachRecord() {
        SolrWriter writer = mock(SolrWriter.class);
        TestTask task = new TestTask(mock(SolrClient.class), writer);
        task.start(baseProps());

        SinkRecord r1 = new SinkRecord("users", 0, Schema.STRING_SCHEMA, "a", null, "x", 1L);
        SinkRecord r2 = new SinkRecord("users", 0, Schema.STRING_SCHEMA, "b", null, "y", 2L);
        task.put(java.util.Arrays.asList(r1, r2));

        verify(writer, times(2)).write(any(SinkRecord.class));
        task.stop();
    }

    @Test
    void putWithNullOrEmptyIsNoop() {
        SolrWriter writer = mock(SolrWriter.class);
        TestTask task = new TestTask(mock(SolrClient.class), writer);
        task.start(baseProps());
        task.put(null);
        task.put(Collections.emptyList());
        verify(writer, never()).write(any());
        task.stop();
    }

    @Test
    void retriableExceptionPropagates() {
        SolrWriter writer = mock(SolrWriter.class);
        doThrow(new RetriableException("transient")).when(writer).write(any());
        TestTask task = new TestTask(mock(SolrClient.class), writer);
        task.start(baseProps());

        SinkRecord r = new SinkRecord("users", 0, Schema.STRING_SCHEMA, "a", null, "x", 1L);
        assertThatThrownBy(() -> task.put(Collections.singletonList(r)))
                .isInstanceOf(RetriableException.class);
        task.stop();
    }

    @Test
    void unexpectedExceptionWrappedAsRetriable() {
        SolrWriter writer = mock(SolrWriter.class);
        doThrow(new RuntimeException("kaboom")).when(writer).write(any());
        TestTask task = new TestTask(mock(SolrClient.class), writer);
        task.start(baseProps());

        SinkRecord r = new SinkRecord("users", 0, Schema.STRING_SCHEMA, "a", null, "x", 1L);
        assertThatThrownBy(() -> task.put(Collections.singletonList(r)))
                .isInstanceOf(RetriableException.class);
        task.stop();
    }

    @Test
    void preCommitAndFlushDelegateToWriter() {
        SolrWriter writer = mock(SolrWriter.class);
        TestTask task = new TestTask(mock(SolrClient.class), writer);
        task.start(baseProps());

        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        offsets.put(new TopicPartition("users", 0), new OffsetAndMetadata(100L));

        Map<TopicPartition, OffsetAndMetadata> returned = task.preCommit(offsets);
        assertThat(returned).isEqualTo(offsets);
        task.flush(offsets);

        verify(writer, atLeastOnce()).flush();
        task.stop();
    }

    @Test
    void stopWithoutStartIsSafe() {
        assertThatCode(() -> new SolrSinkTask().stop()).doesNotThrowAnyException();
    }
}
