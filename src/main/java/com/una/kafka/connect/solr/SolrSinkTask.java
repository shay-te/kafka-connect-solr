package com.una.kafka.connect.solr;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.solr.client.solrj.SolrClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class SolrSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(SolrSinkTask.class);

    private SolrWriter writer;
    private OffsetTracker offsetTracker;
    private boolean sync;
    private boolean streaming;
    private long lingerMs;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting SolrSinkTask v{}", version());
        SolrSinkConfig config = new SolrSinkConfig(props);
        SolrClient client = createClient(config);
        // CUHTTP2 buffers internally; per-record async offsets aren't meaningful in streaming mode.
        this.streaming = config.streamingEnabled();
        this.sync = config.flushSynchronously() || streaming;
        this.offsetTracker = sync ? new SyncOffsetTracker() : new AsyncOffsetTracker();
        this.writer = createWriter(client, config, offsetTracker);
        this.lingerMs = config.lingerMs();
    }

    protected SolrClient createClient(SolrSinkConfig config) {
        return SolrClientFactory.create(config);
    }

    protected SolrWriter createWriter(SolrClient client, SolrSinkConfig config, OffsetTracker tracker) {
        return new SolrWriter(client, config, tracker);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records == null || records.isEmpty()) {
            writer.flushIfLingerElapsed();
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
                throw new RetriableException("Failed to enqueue record " + r.kafkaOffset(), e);
            }
        }
        wakeAfterLingerIfBuffered();
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
            writer.flush();
            logMetrics(streaming ? "streaming" : "sync");
            return currentOffsets;
        }
        writer.flushAsync();
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
