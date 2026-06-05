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

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting SolrSinkTask v{}", version());
        this.config = new SolrSinkConfig(props);
        this.client = SolrClientFactory.create(config);
        this.writer = new SolrWriter(client, config);
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
        writer.flush();
        log.debug("Solr flush complete: written={}, failed={}, retries={}, queueDepth={}",
                writer.recordsWritten(), writer.recordsFailed(),
                writer.retries(), writer.queueDepth());
        return currentOffsets;
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
        writer.flush();
    }

    @Override
    public void stop() {
        log.info("Stopping SolrSinkTask");
        if (writer != null) {
            writer.close();
        }
    }
}
