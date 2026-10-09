package com.una.kafka.connect.solr;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.solr.client.solrj.SolrClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class SolrSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(SolrSinkTask.class);
    // Connect always sets the connector name, and SolrSinkConnector the task id; the defaults serve a task started alone.
    private static final String CONNECTOR_NAME_CONFIG = "name";
    private static final String UNNAMED_CONNECTOR = "unnamed";
    private static final String FIRST_TASK = "0";
    private static final long NOT_FAILING = -1L;

    private SolrWriter writer;
    private SolrSinkTaskMetrics metrics;
    private OffsetTracker offsetTracker;
    private ErrantRecordReporter reporter;
    private boolean sync;
    private boolean streaming;
    private long lingerMs;
    private long retryTimeoutMs;
    // Wall-clock ms of the first retriable failure no successful write has followed; NOT_FAILING while Solr takes writes.
    private long failingSinceMs = NOT_FAILING;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting SolrSinkTask v{}", version());
        SolrSinkConfig config = new SolrSinkConfig(props);
        SolrClient client = createClient(config);
        // Before the writer: its linger.ms clock starts when it is created, and this is a round trip.
        String solrVersion = SolrVersionDetector.detect(client);
        log.info("Writing to Solr {}", solrVersion);
        // CUHTTP2 buffers internally; per-record async offsets aren't meaningful in streaming mode.
        this.streaming = config.streamingEnabled();
        this.sync = config.flushSynchronously() || streaming;
        this.offsetTracker = sync ? new SyncOffsetTracker() : new AsyncOffsetTracker();
        this.reporter = errantRecordReporter();
        this.writer = createWriter(client, config, offsetTracker);
        this.lingerMs = config.lingerMs();
        this.retryTimeoutMs = config.retryTimeoutMs();
        this.metrics = SolrSinkTaskMetrics.register(props.getOrDefault(CONNECTOR_NAME_CONFIG, UNNAMED_CONNECTOR),
                props.getOrDefault(SolrSinkTaskMetrics.TASK_ID_CONFIG, FIRST_TASK), writer, solrVersion);
    }

    protected SolrClient createClient(SolrSinkConfig config) {
        return SolrClientFactory.create(config);
    }

    protected SolrWriter createWriter(SolrClient client, SolrSinkConfig config, OffsetTracker tracker) {
        return new SolrWriter(client, config, tracker, reporter);
    }

    /** Connect's DLQ for documents Solr rejects; null without one (or on a worker older than Kafka 2.6). */
    private ErrantRecordReporter errantRecordReporter() {
        if (context == null) {
            return null;
        }
        try {
            return context.errantRecordReporter();
        } catch (NoSuchMethodError | NoClassDefFoundError e) {
            log.warn("This Kafka Connect worker has no errant-record reporter; documents Solr rejects are only logged");
            return null;
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        // A failure that is not retriable must come out of put(): from preCommit Connect only rewinds.
        writer.throwIfFatal();
        throwIfStalled();
        try {
            putRecords(records);
        } catch (RetriableException e) {
            noteRetriableFailure();
            throw e;
        }
    }

    /** Starts the retry clock at the first retriable failure that no write has followed yet. */
    private void noteRetriableFailure() {
        if (failingSinceMs == NOT_FAILING) {
            failingSinceMs = System.currentTimeMillis();
        }
    }

    /** RUNNING must mean progress: once no write has reached Solr for retry.timeout.ms of retries, the task fails. */
    private void throwIfStalled() {
        if (failingSinceMs == NOT_FAILING || retryTimeoutMs < 0) {
            return;
        }
        if (writer.lastSuccessEpochMs() > failingSinceMs) {
            failingSinceMs = NOT_FAILING;
            return;
        }
        long stalledMs = System.currentTimeMillis() - failingSinceMs;
        if (stalledMs > retryTimeoutMs) {
            throw new ConnectException("No write has reached Solr for " + stalledMs + " ms of retried failures ("
                    + SolrSinkConfig.RETRY_TIMEOUT_MS_CONFIG + "=" + retryTimeoutMs + "): failing the task");
        }
    }

    private void putRecords(Collection<SinkRecord> records) {
        if (records == null || records.isEmpty()) {
            try {
                writer.flushIfLingerElapsed();
            } catch (ConnectException ce) {
                throw ce;
            } catch (Exception e) {
                throw putFailure("Idle linger flush failed", e);
            }
            wakeAfterLingerIfBuffered();
            return;
        }
        for (SinkRecord r : records) {
            try {
                writer.write(r);
            } catch (ConnectException ce) {
                // RetriableException, DataException, and other ConnectException subclasses
                // are already framework-typed - propagate them as-is so Kafka Connect
                // applies the right policy (retry vs DLQ vs fail).
                throw ce;
            } catch (Exception e) {
                throw putFailure("Failed to enqueue record " + r.kafkaOffset(), e);
            }
        }
        wakeAfterLingerIfBuffered();
    }

    /** Retriable only when transient: Connect redelivers a RetriableException forever, the task still RUNNING. */
    private static ConnectException putFailure(String message, Exception cause) {
        return RetryUtil.isRetriable(cause)
                ? new RetriableException(message, cause) : new ConnectException(message, cause);
    }

    /**
     * An idle consumer poll blocks until the next offset commit (offset.flush.interval.ms, 60 s
     * by default), and linger.ms is only checked when a record is written — so a partial batch
     * left at the end of a burst sat unsent for up to a minute. Asking Connect to wake the task
     * after linger.ms delivers an empty put() in time, which flushes it on this thread.
     */
    private void wakeAfterLingerIfBuffered() {
        if (context != null && writer.hasBuffered()) {
            context.timeout(lingerMs);
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(
            Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        if (sync) {
            try {
                writer.flush();
            } catch (RetriableException e) {
                noteRetriableFailure();
                throw e;
            }
            logMetrics(streaming ? "streaming" : "sync");
            return currentOffsets;
        }
        try {
            writer.flushAsync();
        } catch (RuntimeException e) {
            // Connect rewinds every partition to its last commit and redelivers: states from before can never be acked.
            offsetTracker.reset();
            if (e instanceof RetriableException) {
                noteRetriableFailure();
            }
            throw e;
        }
        logMetrics("async");
        return offsetTracker.safeOffsets(currentOffsets);
    }

    private void logMetrics(String mode) {
        if (!log.isInfoEnabled()) return;
        log.info("Solr {} flush: written={} failed={} retries={} queueDepth={} "
                        + "batches={} avgBatchMs={} avgSolrCallMs={}",
                mode,
                writer.recordsWritten(), writer.recordsFailed(),
                writer.retries(), writer.queueDepth(),
                writer.batchCount(),
                String.format(java.util.Locale.ROOT, "%.2f", writer.avgBatchLatencyMs()),
                String.format(java.util.Locale.ROOT, "%.2f", writer.avgSolrCallLatencyMs()));
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        writer.flush();
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        if (offsetTracker != null) {
            for (TopicPartition tp : partitions) {
                offsetTracker.closePartition(tp);
            }
        }
        if (writer != null) {
            for (TopicPartition tp : partitions) {
                writer.partitionRevoked(tp);
            }
        }
    }

    @Override
    public void stop() {
        log.info("Stopping SolrSinkTask");
        if (metrics != null) {
            metrics.unregister();
        }
        if (writer != null) {
            writer.close();
        }
        if (offsetTracker != null) {
            try {
                offsetTracker.close();
            } catch (Exception e) {
                log.warn("OffsetTracker close failed: {}", e.getMessage());
            }
        }
    }
}
