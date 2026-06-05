package com.una.kafka.connect.solr;

import com.una.kafka.connect.solr.SolrSinkConfig.BehaviorOnMalformed;
import com.una.kafka.connect.solr.SolrSinkConfig.BehaviorOnNullValues;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;

/**
 * Public entry point for SolrSinkTask. Reads the per-topic ignore lists,
 * looks up an external version header (if configured), converts and
 * dispatches to the bulk processor — handing in an {@link OffsetState}
 * so an async tracker can advance Kafka offsets per-record.
 */
public class SolrWriter implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SolrWriter.class);

    private final SolrClient client;
    private final SolrSinkConfig config;
    private final SolrRecordConverter converter;
    private final SolrBulkProcessor bulk;
    private final CollectionResolver collections;
    private final SolrSchemaManager schemaManager;
    private final ExternalResourceManager resources;
    private final OffsetTracker offsetTracker;

    private final Set<String> topicsIgnoreKey;
    private final Set<String> topicsIgnoreSchema;

    public SolrWriter(SolrClient client, SolrSinkConfig config, OffsetTracker offsetTracker) {
        this.client = client;
        this.config = config;
        this.converter = new SolrRecordConverter(config);
        this.schemaManager = new SolrSchemaManager(client, config);
        this.resources = new ExternalResourceManager(client, config);
        this.bulk = new SolrBulkProcessor(client, config, schemaManager);
        this.collections = new CollectionResolver(config);
        this.offsetTracker = offsetTracker;
        this.topicsIgnoreKey = config.topicKeyIgnoreSet();
        this.topicsIgnoreSchema = config.topicSchemaIgnoreSet();
    }

    /** Backwards-compatible ctor used by tests written before async offsets. */
    public SolrWriter(SolrClient client, SolrSinkConfig config) {
        this(client, config, new SyncOffsetTracker());
    }

    public void write(SinkRecord record) {
        String collection = collections.resolve(record.topic());
        resources.ensure(collection);
        if (record.value() == null) {
            handleTombstone(record, collection);
            return;
        }
        try {
            if (!topicsIgnoreSchema.contains(record.topic())) {
                schemaManager.evolveIfNeeded(collection, record.valueSchema());
            }
            SolrInputDocument doc = converter.convert(record, isKeyIgnored(record.topic()));
            applyExternalVersion(doc, record);
            OffsetState state = offsetTracker.track(record);
            bulk.upsert(collection, doc, state);
        } catch (DataException de) {
            handleMalformed(record, de);
        }
    }

    private boolean isKeyIgnored(String topic) {
        return config.keyIgnore() || topicsIgnoreKey.contains(topic);
    }

    private void applyExternalVersion(SolrInputDocument doc, SinkRecord record) {
        if (!config.externalVersioningEnabled()) {
            return;
        }
        String headerName = config.externalVersionHeader();
        Header h = record.headers().lastWithName(headerName);
        if (h == null || h.value() == null) {
            return;
        }
        Long version = coerceLong(h.value());
        if (version != null) {
            doc.setField("_version_", version);
        }
    }

    private Long coerceLong(Object v) {
        if (v == null) return null;
        if (v instanceof Number) return ((Number) v).longValue();
        if (v instanceof byte[]) {
            String s = new String((byte[]) v, java.nio.charset.StandardCharsets.UTF_8).trim();
            try { return Long.parseLong(s); } catch (NumberFormatException e) { return null; }
        }
        String s = v.toString().trim();
        try { return Long.parseLong(s); } catch (NumberFormatException e) { return null; }
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
                OffsetState state = offsetTracker.track(record);
                bulk.delete(collection, String.valueOf(record.key()), state);
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

    public void flushAsync() {
        bulk.flushAsync();
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
