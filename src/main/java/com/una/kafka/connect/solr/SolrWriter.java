package com.una.kafka.connect.solr;

import com.una.kafka.connect.solr.SolrSinkConfig.BehaviorOnMalformed;
import com.una.kafka.connect.solr.SolrSinkConfig.BehaviorOnNullValues;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Public entry point for SolrSinkTask. Converts records and hands them to
 * the bulk processor, applying tombstone and malformed-doc policies.
 */
public class SolrWriter implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SolrWriter.class);

    private final SolrClient client;
    private final SolrSinkConfig config;
    private final SolrRecordConverter converter;
    private final SolrBulkProcessor bulk;
    private final CollectionResolver collections;
    private final SolrSchemaManager schemaManager;

    public SolrWriter(SolrClient client, SolrSinkConfig config) {
        this.client = client;
        this.config = config;
        this.converter = new SolrRecordConverter(config);
        this.schemaManager = new SolrSchemaManager(client, config);
        this.bulk = new SolrBulkProcessor(client, config, schemaManager);
        this.collections = new CollectionResolver(config);
    }

    public void write(SinkRecord record) {
        String collection = collections.resolve(record.topic());
        if (record.value() == null) {
            handleTombstone(record, collection);
            return;
        }
        try {
            schemaManager.evolveIfNeeded(collection, record.valueSchema());
            SolrInputDocument doc = converter.convert(record);
            bulk.upsert(collection, doc);
        } catch (DataException de) {
            handleMalformed(record, de);
        }
    }

    private void handleTombstone(SinkRecord record, String collection) {
        BehaviorOnNullValues behavior = config.behaviorOnNullValues();
        switch (behavior) {
            case IGNORE:
                return;
            case FAIL:
                throw new DataException("Tombstone record encountered with behavior=fail: "
                        + record.topic() + "-" + record.kafkaPartition() + "@" + record.kafkaOffset());
            case DELETE:
                if (record.key() == null) {
                    log.warn("Tombstone has null key, cannot delete; skipping");
                    return;
                }
                bulk.delete(collection, String.valueOf(record.key()));
                return;
            default:
                throw new DataException("Unknown null value behavior: " + behavior);
        }
    }

    private void handleMalformed(SinkRecord record, DataException de) {
        BehaviorOnMalformed behavior = config.behaviorOnMalformed();
        switch (behavior) {
            case IGNORE:
                return;
            case WARN:
                log.warn("Skipping malformed record at {}-{}@{}: {}",
                        record.topic(), record.kafkaPartition(), record.kafkaOffset(), de.getMessage());
                return;
            case FAIL:
            default:
                throw de;
        }
    }

    public void flush() {
        bulk.flushSync();
    }

    public long recordsWritten() { return bulk.recordsWritten(); }
    public long recordsFailed() { return bulk.recordsFailed(); }
    public long retries() { return bulk.retries(); }
    public int queueDepth() { return bulk.queueDepth(); }

    @Override
    public void close() {
        bulk.close();
        try {
            client.close();
        } catch (IOException e) {
            log.warn("SolrClient close failed: {}", e.getMessage());
        }
    }
}
