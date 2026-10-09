package com.una.kafka.connect.solr;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * How the task surfaces flush failures to Kafka Connect. A failure that must stop the task has to come
 * out of put(): thrown only from preCommit, Connect rewinds and redelivers forever. A document Solr
 * rejects goes to the errant-record reporter Connect provides. And after a failed async flush the rewind
 * redelivers every record, so states tracked before it must not pin the commit.
 */
class SolrSinkTaskFailureHandlingTest {

    private static final TopicPartition PARTITION = new TopicPartition("users", 0);

    private static Map<String, String> props(boolean flushSynchronously) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "users");
        p.put(SolrSinkConfig.FLUSH_SYNCHRONOUSLY_CONFIG, String.valueOf(flushSynchronously));
        return p;
    }

    private static SinkRecord record(long offset) {
        Map<String, Object> value = new HashMap<>();
        value.put("first_name", "Ada");
        return new SinkRecord("users", 0, Schema.STRING_SCHEMA, String.valueOf(offset), null, value, offset);
    }

    private static List<SinkRecord> records(long... offsets) {
        List<SinkRecord> out = new ArrayList<>();
        for (long offset : offsets) {
            out.add(record(offset));
        }
        return out;
    }

    /** A task whose writer is given, and which hands the writer the tracker the task built. */
    private static final class TestTask extends SolrSinkTask {
        private final SolrWriter writer;
        OffsetTracker tracker;

        TestTask(SolrWriter writer) {
            this.writer = writer;
        }

        @Override
        protected SolrClient createClient(SolrSinkConfig config) {
            return mock(SolrClient.class);
        }

        @Override
        protected SolrWriter createWriter(SolrClient client, SolrSinkConfig config, OffsetTracker tracker) {
            this.tracker = tracker;
            return writer;
        }
    }

    @Test
    void aFailureThatMustStopTheTaskIsThrownFromPut() {
        SolrWriter writer = mock(SolrWriter.class);
        doThrow(new ConnectException("Solr failed the request for a reason that is not about a document"))
                .when(writer).throwIfFatal();
        TestTask task = new TestTask(writer);
        task.start(props(true));

        assertThatThrownBy(() -> task.put(records(1)))
                .isInstanceOf(ConnectException.class)
                .isNotInstanceOf(RetriableException.class);
        assertThatThrownBy(() -> task.put(Collections.emptyList()))
                .as("the idle put Connect sends after linger.ms stops the task too")
                .isInstanceOf(ConnectException.class);
        verify(writer, never()).write(any());
    }

    @Test
    void aDocumentSolrRejectsReachesTheReporterConnectProvides() throws Exception {
        ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
        when(reporter.report(any(), any())).thenReturn(CompletableFuture.completedFuture(null));
        SinkTaskContext context = mock(SinkTaskContext.class);
        when(context.errantRecordReporter()).thenReturn(reporter);
        SolrClient solr = mock(SolrClient.class);
        when(solr.request(any(UpdateRequest.class), anyString())).thenThrow(
                new BaseHttpSolrClient.RemoteSolrException("http://x/solr/users", 400, "bad document", null));
        SolrSinkTask task = new SolrSinkTask() {
            @Override
            protected SolrClient createClient(SolrSinkConfig config) {
                return solr;
            }
        };
        Map<String, String> p = props(true);
        p.put(SolrSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, "warn");
        p.put(SolrSinkConfig.MAX_RETRIES_CONFIG, "0");
        p.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "UNUSED");
        p.put(SolrSinkConfig.SCHEMA_IGNORE_CONFIG, "true");
        task.initialize(context);
        task.start(p);
        SinkRecord rejected = record(7);

        task.put(Collections.singletonList(rejected));
        Map<TopicPartition, OffsetAndMetadata> consumerPosition = new HashMap<>();
        consumerPosition.put(PARTITION, new OffsetAndMetadata(8L));
        task.preCommit(consumerPosition);

        verify(reporter).report(eq(rejected), any(BaseHttpSolrClient.RemoteSolrException.class));
        task.stop();
    }

    @Test
    void afterAFailedAsyncFlushTheRedeliveredRecordsCommitAgain() {
        // flush.synchronously=false: the real AsyncOffsetTracker decides the committed offsets.
        SolrWriter writer = mock(SolrWriter.class);
        TestTask task = new TestTask(writer);
        List<OffsetState> sent = new ArrayList<>();
        doAnswer(call -> {
            sent.add(task.tracker.track(call.getArgument(0)));
            return null;
        }).when(writer).write(any());
        AtomicInteger flushes = new AtomicInteger();
        doAnswer(call -> {
            if (flushes.incrementAndGet() == 1) {
                throw new RetriableException("Solr bulk request failed");   // that batch is never acked
            }
            sent.forEach(OffsetState::markAcked);
            return null;
        }).when(writer).flushAsync();
        task.start(props(false));

        task.put(records(0, 1, 2));
        Map<TopicPartition, OffsetAndMetadata> consumerPosition = new HashMap<>();
        consumerPosition.put(PARTITION, new OffsetAndMetadata(3L));
        assertThatThrownBy(() -> task.preCommit(consumerPosition)).isInstanceOf(RetriableException.class);

        // Connect rewinds to the last commit and redelivers the same records.
        sent.clear();
        task.put(records(0, 1, 2));
        Map<TopicPartition, OffsetAndMetadata> committed = task.preCommit(consumerPosition);

        assertThat(committed.get(PARTITION).offset())
                .as("the states of the failed flush must not pin the commit at offset 0 forever")
                .isEqualTo(3L);
        assertThat(((AsyncOffsetTracker) task.tracker).pendingCount()).isZero();
        task.stop();
    }
}
