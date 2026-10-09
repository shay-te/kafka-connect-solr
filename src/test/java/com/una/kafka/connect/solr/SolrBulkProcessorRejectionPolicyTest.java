package com.una.kafka.connect.solr;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateHttp2SolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.cloud.ZooKeeperException;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * What happens to a batch Solr refuses. Only a rejection of the documents themselves (400/409/413) may be
 * dropped, and only under warn/ignore, after it reached the errant-record reporter (the DLQ). Anything
 * else - auth, permission, a missing collection, or a rejection under fail - acks nothing and stops the
 * task: a later flush must not report success, or preCommit would commit past the lost records. A cluster-state
 * or transport failure is retried and never dead-lettered.
 */
class SolrBulkProcessorRejectionPolicyTest {

    private static SolrSinkConfig cfg(String behavior) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "c");
        p.put(SolrSinkConfig.BATCH_SIZE_CONFIG, "100");
        p.put(SolrSinkConfig.LINGER_MS_CONFIG, "100000");
        p.put(SolrSinkConfig.MAX_RETRIES_CONFIG, "0");
        p.put(SolrSinkConfig.RETRY_BACKOFF_MS_CONFIG, "1");
        p.put(SolrSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, behavior);
        return new SolrSinkConfig(p);
    }

    private static SolrInputDocument doc(String id) {
        SolrInputDocument d = new SolrInputDocument();
        d.addField("id", id);
        return d;
    }

    private static SinkRecord record(long offset) {
        return new SinkRecord("t", 0, Schema.STRING_SCHEMA, "k" + offset, null, "v", offset);
    }

    private static OffsetState state(long offset) {
        return new OffsetState(new TopicPartition("t", 0), offset);
    }

    private static BaseHttpSolrClient.RemoteSolrException remote(int code) {
        return new BaseHttpSolrClient.RemoteSolrException("http://x/solr/c", code, "HTTP " + code, null);
    }

    /** ZkStateReader's answer when it cannot read the collection from ZooKeeper: the client's own 400. */
    private static SolrException zooKeeperReadFailure(Exception cause) {
        return new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Could not load collection from ZK: c", cause);
    }

    /** Answers like Solr: a request carrying document {@code badId} fails with {@code code}, the rest succeed. */
    private static SolrClient solrRejecting(String badId, int code) throws Exception {
        SolrClient client = mock(SolrClient.class);
        when(client.request(any(UpdateRequest.class), anyString())).thenAnswer(call -> {
            UpdateRequest request = call.getArgument(0);
            boolean carriesBad = request.getDocuments() != null && request.getDocuments().stream()
                    .anyMatch(d -> badId.equals(d.getFieldValue("id")));
            if (carriesBad) {
                throw remote(code);
            }
            return null;
        });
        return client;
    }

    @Test
    void aFailureThatIsNotAboutADocumentIsNeverAckedAndStopsTheTask() throws Exception {
        for (int code : new int[] {401, 403, 404, 405}) {
            SolrClient client = mock(SolrClient.class);
            when(client.request(any(UpdateRequest.class), anyString())).thenThrow(remote(code));
            SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg("warn"));
            OffsetState first = state(1);
            OffsetState second = state(2);
            bulk.upsert("c", doc("1"), -1L, first, record(1));
            bulk.upsert("c", doc("2"), -1L, second, record(2));

            assertThatThrownBy(bulk::flushSync).as("HTTP %d", code)
                    .isInstanceOf(ConnectException.class)
                    .isNotInstanceOf(RetriableException.class)
                    .hasMessageContaining("not about a document");
            assertThat(first.isAcked()).as("HTTP %d acks nothing", code).isFalse();
            assertThat(second.isAcked()).as("HTTP %d acks nothing", code).isFalse();
            assertThat(bulk.recordsFailed()).isEqualTo(2);
            // Nothing is left to send, yet a later flush (preCommit) still must not report success.
            assertThatThrownBy(bulk::flushSync).as("HTTP %d stays failed", code).isInstanceOf(ConnectException.class);
            assertThatThrownBy(bulk::throwIfFatal).isInstanceOf(ConnectException.class);
            bulk.close();
        }
    }

    @Test
    void aDocumentRejectionUnderWarnGoesToTheReporterAndOnlyThatDocumentIsDropped() throws Exception {
        ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
        when(reporter.report(any(), any())).thenReturn(CompletableFuture.completedFuture(null));
        SolrBulkProcessor bulk = new SolrBulkProcessor(solrRejecting("bad", 400), cfg("warn"), reporter);
        OffsetState good = state(1);
        OffsetState bad = state(2);
        SinkRecord badRecord = record(2);
        bulk.upsert("c", doc("good"), -1L, good, record(1));
        bulk.upsert("c", doc("bad"), -1L, bad, badRecord);

        bulk.flushSync();

        verify(reporter).report(eq(badRecord), any(BaseHttpSolrClient.RemoteSolrException.class));
        verifyNoMoreInteractions(reporter);
        assertThat(good.isAcked()).isTrue();
        assertThat(bad.isAcked()).as("acked only after the DLQ has it").isTrue();
        assertThat(bulk.recordsWritten()).isEqualTo(1);
        assertThat(bulk.recordsFailed()).isEqualTo(1);
        bulk.close();
    }

    @Test
    void aReporterThatRefusesTheDocumentStopsTheTaskInsteadOfDroppingIt() throws Exception {
        // errors.tolerance=none: the reporter refuses, so the connector must fail rather than drop.
        ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
        when(reporter.report(any(), any())).thenThrow(new ConnectException("Tolerance exceeded in error handler"));
        SolrBulkProcessor bulk = new SolrBulkProcessor(solrRejecting("bad", 400), cfg("warn"), reporter);
        OffsetState bad = state(1);
        bulk.upsert("c", doc("bad"), -1L, bad, record(1));

        assertThatThrownBy(bulk::flushSync).isInstanceOf(ConnectException.class)
                .isNotInstanceOf(RetriableException.class);
        assertThat(bad.isAcked()).isFalse();
        assertThatThrownBy(bulk::throwIfFatal).hasMessageContaining("errant-record reporter refused");
        bulk.close();
    }

    @Test
    void aDeadLetterWriteThatFailsStopsTheTaskInsteadOfDroppingTheDocument() throws Exception {
        ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
        CompletableFuture<Void> failedWrite = new CompletableFuture<>();
        failedWrite.completeExceptionally(new IllegalStateException("DLQ topic unavailable"));
        when(reporter.report(any(), any())).thenReturn(failedWrite);
        SolrBulkProcessor bulk = new SolrBulkProcessor(solrRejecting("bad", 400), cfg("ignore"), reporter);
        OffsetState bad = state(1);
        bulk.upsert("c", doc("bad"), -1L, bad, record(1));

        assertThatThrownBy(bulk::flushSync).isInstanceOf(ConnectException.class);
        assertThat(bad.isAcked()).isFalse();
        assertThatThrownBy(bulk::throwIfFatal).hasMessageContaining("could not write");
        bulk.close();
    }

    @Test
    void underFailADocumentRejectionStopsTheTaskAndNoLaterFlushCanCommitPastIt() throws Exception {
        SolrBulkProcessor bulk = new SolrBulkProcessor(solrRejecting("bad", 400), cfg("fail"));
        OffsetState bad = state(1);
        bulk.upsert("c", doc("bad"), -1L, bad, record(1));

        assertThatThrownBy(bulk::flushSync).hasMessageContaining("Solr rejected a document");
        assertThat(bad.isAcked()).isFalse();
        // Before, this flush found nothing in flight and returned: sync preCommit then committed past the record.
        assertThatThrownBy(bulk::flushSync).hasMessageContaining("Solr rejected a document");
        bulk.close();
    }

    @Test
    void aFailureThatIsNotAboutADocumentDuringIsolationStopsItUnacked() throws Exception {
        // The run is refused as bad documents (400), but re-sending op by op then hits a 401.
        AtomicInteger calls = new AtomicInteger();
        SolrClient client = mock(SolrClient.class);
        when(client.request(any(UpdateRequest.class), anyString())).thenAnswer(call -> {
            int n = calls.incrementAndGet();
            if (n == 1) {
                throw remote(400);
            }
            if (n == 3) {
                throw remote(401);
            }
            return null;
        });
        SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg("warn"));
        OffsetState sent = state(1);
        OffsetState refused = state(2);
        OffsetState neverSent = state(3);
        bulk.upsert("c", doc("1"), -1L, sent, record(1));
        bulk.upsert("c", doc("2"), -1L, refused, record(2));
        bulk.upsert("c", doc("3"), -1L, neverSent, record(3));

        assertThatThrownBy(bulk::flushSync).hasMessageContaining("not about a document");
        assertThat(sent.isAcked()).isTrue();
        assertThat(refused.isAcked()).isFalse();
        assertThat(neverSent.isAcked()).isFalse();
        assertThat(bulk.recordsWritten()).isEqualTo(1);
        assertThat(bulk.recordsFailed()).isEqualTo(2);
        bulk.close();
    }

    @Test
    void onlySolrsAnswersAboutTheDocumentsCountAsDocumentRejections() {
        for (int code : new int[] {400, 409, 413}) {
            assertThat(SolrBulkProcessor.isDocumentRejection(remote(code))).as("HTTP %d", code).isTrue();
        }
        for (int code : new int[] {401, 403, 404, 405, 406, 415}) {
            assertThat(SolrBulkProcessor.isDocumentRejection(remote(code))).as("HTTP %d", code).isFalse();
        }
        // An in-process Solr raises the core's own 400 for a bad document.
        assertThat(SolrBulkProcessor.isDocumentRejection(
                new SolrException(SolrException.ErrorCode.BAD_REQUEST, "ERROR: [doc=1] unknown field 'x'"))).isTrue();
        // The cloud client raises this 400 itself when the collection is gone: not a document problem.
        assertThat(SolrBulkProcessor.isDocumentRejection(
                new SolrException(SolrException.ErrorCode.BAD_REQUEST, "Collection not found: users"))).isFalse();
        assertThat(SolrBulkProcessor.isDocumentRejection(new RuntimeException("wrapped", remote(400)))).isTrue();
        assertThat(SolrBulkProcessor.isDocumentRejection(new IllegalStateException("client-side failure"))).isFalse();
    }

    @Test
    void clusterStateAndTransportFailuresAreNeverDocumentRejections() {
        for (Exception cause : new Exception[] {new KeeperException.ConnectionLossException(),
                new IOException("connection reset"), new InterruptedException()}) {
            assertThat(SolrBulkProcessor.isDocumentRejection(zooKeeperReadFailure(cause))).as("%s", cause).isFalse();
        }
        // An in-process core's 400 for a bad value keeps the parse error as its cause: still a rejection.
        assertThat(SolrBulkProcessor.isDocumentRejection(new SolrException(SolrException.ErrorCode.BAD_REQUEST,
                "ERROR: [doc=1] Error adding field 'age'='abc'", new NumberFormatException("abc")))).isTrue();
    }

    @Test
    void aClusterStateFailureUnderWarnIsRetriedAndNeverDeadLettered() throws Exception {
        for (SolrException failure : new SolrException[] {
                zooKeeperReadFailure(new KeeperException.ConnectionLossException()),
                new ZooKeeperException(SolrException.ErrorCode.SERVER_ERROR, "ZooKeeper session expired"),
                new SolrException(SolrException.ErrorCode.INVALID_STATE, "Not enough nodes to handle the request")}) {
            ErrantRecordReporter reporter = mock(ErrantRecordReporter.class);
            when(reporter.report(any(), any())).thenReturn(CompletableFuture.completedFuture(null));
            SolrClient client = mock(SolrClient.class);
            when(client.request(any(UpdateRequest.class), anyString())).thenThrow(failure);
            SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg("warn"), reporter);
            OffsetState pending = state(1);
            bulk.upsert("c", doc("1"), -1L, pending, record(1));

            assertThatThrownBy(bulk::flushSync).as(failure.getMessage()).isInstanceOf(RetriableException.class);
            assertThat(pending.isAcked()).as(failure.getMessage()).isFalse();
            verifyNoInteractions(reporter);
            // Nothing sticky: the redelivered batch may still succeed.
            bulk.throwIfFatal();
            bulk.close();
        }
    }

    @Test
    void aFailedRunCountsTheRunsItAbortsSoEveryOpLeavesTheQueue() throws Exception {
        // [upsert][delete][upsert]: the first run fails, so the two after it are never sent.
        for (int code : new int[] {401, 503}) {
            SolrClient client = mock(SolrClient.class);
            when(client.request(any(UpdateRequest.class), anyString())).thenThrow(remote(code));
            SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg("warn"));
            bulk.upsert("c", doc("1"), -1L, state(1), record(1));
            bulk.delete("c", "2", state(2), record(2));
            bulk.upsert("c", doc("3"), -1L, state(3), record(3));

            assertThatThrownBy(bulk::flushSync).as("HTTP %d", code).isInstanceOf(ConnectException.class);
            assertThat(bulk.queueDepth()).as("HTTP %d: every op left the queue", code).isZero();
            assertThat(bulk.recordsFailed()).as("HTTP %d", code).isEqualTo(3);
            bulk.close();
        }
    }

    @Test
    void aStreamingFlushFailureThatIsNotTransientStopsTheTask() throws Exception {
        ConcurrentUpdateHttp2SolrClient client = mock(ConcurrentUpdateHttp2SolrClient.class);
        doThrow(new IllegalStateException("streaming client closed")).when(client).blockUntilFinished();
        SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg("warn"));
        bulk.upsert("c", doc("1"), -1L, state(1), record(1));

        assertThatThrownBy(bulk::flushSync).isInstanceOf(ConnectException.class)
                .isNotInstanceOf(RetriableException.class);
        assertThatThrownBy(bulk::throwIfFatal).hasMessageContaining("Solr streaming flush failed");
        bulk.close();
    }
}
