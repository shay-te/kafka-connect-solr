package com.una.kafka.connect.solr;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
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

    private SolrSinkConfig config;
    private SolrClient client;
    private SolrWriter writer;
    private OffsetTracker offsetTracker;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting SolrSinkTask v{}", version());
        this.config = new SolrSinkConfig(props);
        this.client = createClient(config);
        this.offsetTracker = config.flushSynchronously() ? new SyncOffsetTracker() : new AsyncOffsetTracker();
        this.writer = createWriter(client, config, offsetTracker);
    }

    /** Test seam. */
    protected SolrClient createClient(SolrSinkConfig config) {
        return SolrClientFactory.create(config);
    }

    /** Test seam. */
    protected SolrWriter createWriter(SolrClient client, SolrSinkConfig config, OffsetTracker tracker) {
        return new SolrWriter(client, config, tracker);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records == null || records.isEmpty()) {
            return;
        }
        for (SinkRecord r : records) {
            try {
                writer.write(r);
            } catch (RetriableException re) {
                throw re;
            } catch (Exception e) {
                throw new RetriableException("Failed to enqueue record " + r.kafkaOffset(), e);
            }
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(
            Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        if (config.flushSynchronously()) {
            writer.flush();
            log.debug("Solr flush complete: written={}, failed={}, retries={}, queueDepth={}",
                    writer.recordsWritten(), writer.recordsFailed(),
                    writer.retries(), writer.queueDepth());
            return currentOffsets;
        }
        // Async mode: fire any pending writes but DO NOT block. Return only
        // the offsets that have actually been acked by Solr so the consumer
        // can keep up while Solr is digesting older batches.
        writer.flushAsync();
        return offsetTracker.safeOffsets(currentOffsets);
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        // flush() is the legacy hook; respect the caller and always block here.
        writer.flush();
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        if (offsetTracker != null) {
            for (TopicPartition tp : partitions) {
                offsetTracker.closePartition(tp);
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
            try { offsetTracker.close(); } catch (Exception ignored) { }
        }
    }
}
