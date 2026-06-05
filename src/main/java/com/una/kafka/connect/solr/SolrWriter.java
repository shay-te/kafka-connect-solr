package com.una.kafka.connect.solr;

import com.una.kafka.connect.solr.SolrSinkConfig.BehaviorOnMalformed;
import com.una.kafka.connect.solr.SolrSinkConfig.BehaviorOnNullValues;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class SolrWriter implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SolrWriter.class);

    private final SolrClient client;
    private final SolrSinkConfig config;
    private final SolrRecordConverter converter;
    private final CollectionResolver collections;
    private final SolrSchemaManager schemaManager;
    private final ExternalResourceManager resources;
    private final OffsetTracker offsetTracker;

    private final Set<String> topicsIgnoreKey;
    private final Set<String> topicsIgnoreSchema;

    private final boolean keyIgnoreGlobal;
    private final boolean externalVersioningEnabled;
    private final String externalVersionHeader;
    private final BehaviorOnNullValues behaviorOnNullValues;
    private final BehaviorOnMalformed behaviorOnMalformed;

    private final boolean partitionFanout;
    private final SolrBulkProcessor sharedProcessor;
    private final ConcurrentMap<TopicPartition, SolrBulkProcessor> perPartition;

    public SolrWriter(SolrClient client, SolrSinkConfig config, OffsetTracker offsetTracker) {
        this.client = client;
        this.config = config;
        this.converter = new SolrRecordConverter(config);
        this.schemaManager = new SolrSchemaManager(client, config);
        this.resources = new ExternalResourceManager(client, config);
        this.collections = new CollectionResolver(config);
        this.offsetTracker = offsetTracker;
        this.topicsIgnoreKey = config.topicKeyIgnoreSet();
        this.topicsIgnoreSchema = config.topicSchemaIgnoreSet();
        this.keyIgnoreGlobal = config.keyIgnore();
        this.externalVersioningEnabled = config.externalVersioningEnabled();
        this.externalVersionHeader = config.externalVersionHeader();
        this.behaviorOnNullValues = config.behaviorOnNullValues();
        this.behaviorOnMalformed = config.behaviorOnMalformed();

        this.partitionFanout = config.partitionFanoutEnabled();
        if (partitionFanout) {
            this.sharedProcessor = null;
            this.perPartition = new ConcurrentHashMap<>();
            log.info("SolrWriter: per-partition fanout enabled");
        } else {
            this.sharedProcessor = new SolrBulkProcessor(client, config, schemaManager);
            this.perPartition = null;
        }
    }

    public SolrWriter(SolrClient client, SolrSinkConfig config) {
        this(client, config, new SyncOffsetTracker());
    }

    private SolrBulkProcessor processorFor(SinkRecord record) {
        if (!partitionFanout) {
            return sharedProcessor;
        }
        TopicPartition tp = new TopicPartition(record.topic(), record.kafkaPartition());
        SolrBulkProcessor existing = perPartition.get(tp);
        if (existing != null) {
            return existing;
        }
        return perPartition.computeIfAbsent(tp,
                k -> new SolrBulkProcessor(client, config, schemaManager));
    }

    public void write(SinkRecord record) {
        String collection = collections.resolve(record.topic());
        resources.ensure(collection);
        SolrBulkProcessor bulk = processorFor(record);
        if (record.value() == null) {
            handleTombstone(record, collection, bulk);
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
        return keyIgnoreGlobal || topicsIgnoreKey.contains(topic);
    }

    private void applyExternalVersion(SolrInputDocument doc, SinkRecord record) {
        if (!externalVersioningEnabled) {
            return;
        }
        Header h = record.headers().lastWithName(externalVersionHeader);
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

    private void handleTombstone(SinkRecord record, String collection, SolrBulkProcessor bulk) {
        BehaviorOnNullValues behavior = behaviorOnNullValues;
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
                Object key = record.key();
                String id = key instanceof String ? (String) key : String.valueOf(key);
                OffsetState state = offsetTracker.track(record);
                bulk.delete(collection, id, state);
                return;
            default:
                throw new DataException("Unknown null value behavior: " + behavior);
        }
    }

    private void handleMalformed(SinkRecord record, DataException de) {
        BehaviorOnMalformed behavior = behaviorOnMalformed;
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
        forEachProcessor(SolrBulkProcessor::flushSync);
    }

    public void flushAsync() {
        forEachProcessor(SolrBulkProcessor::flushAsync);
    }

    public void partitionRevoked(TopicPartition tp) {
        if (!partitionFanout) {
            return;
        }
        SolrBulkProcessor proc = perPartition.remove(tp);
        if (proc != null) {
            proc.close();
        }
    }

    private void forEachProcessor(java.util.function.Consumer<SolrBulkProcessor> action) {
        if (partitionFanout) {
            for (SolrBulkProcessor p : perPartition.values()) action.accept(p);
        } else {
            action.accept(sharedProcessor);
        }
    }

    public long recordsWritten() {
        if (!partitionFanout) return sharedProcessor.recordsWritten();
        long total = 0;
        for (SolrBulkProcessor p : perPartition.values()) total += p.recordsWritten();
        return total;
    }

    public long recordsFailed() {
        if (!partitionFanout) return sharedProcessor.recordsFailed();
        long total = 0;
        for (SolrBulkProcessor p : perPartition.values()) total += p.recordsFailed();
        return total;
    }

    public long retries() {
        if (!partitionFanout) return sharedProcessor.retries();
        long total = 0;
        for (SolrBulkProcessor p : perPartition.values()) total += p.retries();
        return total;
    }

    public int queueDepth() {
        if (!partitionFanout) return sharedProcessor.queueDepth();
        int total = 0;
        for (SolrBulkProcessor p : perPartition.values()) total += p.queueDepth();
        return total;
    }

    public long batchCount() {
        if (!partitionFanout) return sharedProcessor.batchCount();
        long total = 0;
        for (SolrBulkProcessor p : perPartition.values()) total += p.batchCount();
        return total;
    }

    public long solrCallCount() {
        if (!partitionFanout) return sharedProcessor.solrCallCount();
        long total = 0;
        for (SolrBulkProcessor p : perPartition.values()) total += p.solrCallCount();
        return total;
    }

    public double avgBatchLatencyMs() {
        if (!partitionFanout) return sharedProcessor.avgBatchLatencyMs();
        long totalCount = 0;
        double weightedSum = 0.0;
        for (SolrBulkProcessor p : perPartition.values()) {
            long c = p.batchCount();
            if (c == 0) continue;
            weightedSum += p.avgBatchLatencyMs() * c;
            totalCount += c;
        }
        return totalCount == 0 ? 0.0 : weightedSum / totalCount;
    }

    public double avgSolrCallLatencyMs() {
        if (!partitionFanout) return sharedProcessor.avgSolrCallLatencyMs();
        long totalCount = 0;
        double weightedSum = 0.0;
        for (SolrBulkProcessor p : perPartition.values()) {
            long c = p.solrCallCount();
            if (c == 0) continue;
            weightedSum += p.avgSolrCallLatencyMs() * c;
            totalCount += c;
        }
        return totalCount == 0 ? 0.0 : weightedSum / totalCount;
    }

    public long lastSuccessEpochMs() {
        if (!partitionFanout) return sharedProcessor.lastSuccessEpochMs();
        long max = 0L;
        for (SolrBulkProcessor p : perPartition.values()) {
            long t = p.lastSuccessEpochMs();
            if (t > max) max = t;
        }
        return max;
    }

    @Override
    public void close() {
        if (partitionFanout) {
            for (SolrBulkProcessor p : perPartition.values()) p.close();
            perPartition.clear();
        } else {
            sharedProcessor.close();
        }
        try {
            client.close();
        } catch (IOException e) {
            log.warn("SolrClient close failed: {}", e.getMessage());
        }
    }
}
