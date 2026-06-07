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
    // Solr's reserved optimistic-concurrency field; set when external-version header is present.
    private static final String SOLR_VERSION_FIELD = "_version_";

    private final SolrClient client;
    private final SolrSinkConfig config;
    private final SolrRecordConverter converter;
    private final CollectionResolver collections;
    private final SolrSchemaManager schemaManager;
    private final ExternalResourceManager resources;
    private final OffsetTracker offsetTracker;

    private final Set<String> topicsIgnoreKey;
    private final Set<String> topicsIgnoreSchema;
    private final boolean topicsIgnoreKeyEmpty;
    private final boolean topicsIgnoreSchemaEmpty;

    private final boolean keyIgnoreGlobal;
    private final boolean externalVersioningEnabled;
    private final String externalVersionHeader;
    private final BehaviorOnNullValues behaviorOnNullValues;
    private final BehaviorOnMalformed behaviorOnMalformed;
    // Cached so we can skip the per-record schemaManager.evolveIfNeeded(...) call entirely
    // when auto-evolve is off (the default + the production-recommended setting).
    private final boolean schemaEvolveEnabled;
    // Global schema.ignore=true means "don't use Connect's record schema for destination mapping",
    // which implies "don't auto-evolve the Solr schema either". Honoured alongside topic.schema.ignore.
    private final boolean schemaIgnoreGlobal;

    private final boolean partitionFanout;
    private final SolrBulkProcessor sharedProcessor;
    private final ConcurrentMap<TopicPartition, SolrBulkProcessor> perPartition;

    private String lastTopic;
    private int lastPartition = -1;
    private SolrBulkProcessor lastProcessor;

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
        this.topicsIgnoreKeyEmpty = topicsIgnoreKey.isEmpty();
        this.topicsIgnoreSchemaEmpty = topicsIgnoreSchema.isEmpty();
        this.keyIgnoreGlobal = config.keyIgnore();
        this.externalVersioningEnabled = config.externalVersioningEnabled();
        this.externalVersionHeader = config.externalVersionHeader();
        this.behaviorOnNullValues = config.behaviorOnNullValues();
        this.behaviorOnMalformed = config.behaviorOnMalformed();
        this.schemaEvolveEnabled = config.schemaAutoEvolve();
        this.schemaIgnoreGlobal = config.schemaIgnore();

        this.partitionFanout = config.partitionFanoutEnabled();
        if (partitionFanout) {
            this.sharedProcessor = null;
            this.perPartition = new ConcurrentHashMap<>();
            log.info("SolrWriter: per-partition fanout enabled");
        } else {
            this.sharedProcessor = new SolrBulkProcessor(client, config);
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
        int partition = record.kafkaPartition();
        String topic = record.topic();
        if (partition == lastPartition && topic.equals(lastTopic) && lastProcessor != null) {
            return lastProcessor;
        }
        TopicPartition tp = new TopicPartition(topic, partition);
        SolrBulkProcessor proc = perPartition.get(tp);
        if (proc == null) {
            proc = perPartition.computeIfAbsent(tp,
                    k -> new SolrBulkProcessor(client, config));
        }
        lastTopic = topic;
        lastPartition = partition;
        lastProcessor = proc;
        return proc;
    }

    public void write(SinkRecord record) {
        String topic = record.topic();
        // Resolve tombstone path before the full pipeline so IGNORE (default) skips
        // collection-resolve / resource-ensure / processor-lookup work entirely.
        if (record.value() == null) {
            if (behaviorOnNullValues == BehaviorOnNullValues.IGNORE) {
                return;
            }
            String collection = collections.resolve(topic);
            resources.ensure(collection);
            handleTombstone(record, collection, processorFor(record));
            return;
        }
        String collection = collections.resolve(topic);
        resources.ensure(collection);
        SolrBulkProcessor bulk = processorFor(record);
        try {
            if (schemaEvolveEnabled
                    && !schemaIgnoreGlobal
                    && (topicsIgnoreSchemaEmpty || !topicsIgnoreSchema.contains(topic))) {
                schemaManager.evolveIfNeeded(collection, record.valueSchema());
            }
            boolean keyIgnored = keyIgnoreGlobal
                    || (!topicsIgnoreKeyEmpty && topicsIgnoreKey.contains(topic));
            SolrInputDocument doc = converter.convert(record, keyIgnored);
            applyExternalVersion(doc, record);
            OffsetState state = offsetTracker.track(record);
            // Converter accumulates the byte estimate during field building when bulk.size.bytes>0,
            // letting SolrBulkProcessor skip the second-pass walk of the doc.
            bulk.upsert(collection, doc, converter.lastConversionByteEstimate(), state);
        } catch (DataException de) {
            handleMalformed(record, de);
        }
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
            doc.setField(SOLR_VERSION_FIELD, version);
        }
    }

    private Long coerceLong(Object v) {
        if (v == null) return null;
        if (v instanceof Number) return ((Number) v).longValue();
        if (v instanceof byte[]) return parseAsciiLong((byte[]) v);
        if (v instanceof CharSequence) return parseAsciiLong((CharSequence) v);
        return parseAsciiLong(v.toString());
    }

    static Long parseAsciiLong(byte[] b) {
        int n = b.length;
        int i = 0, end = n;
        while (i < end && b[i] <= 0x20) i++;
        while (end > i && b[end - 1] <= 0x20) end--;
        if (i == end) return null;
        boolean neg = false;
        if (b[i] == '-') { neg = true; i++; }
        else if (b[i] == '+') { i++; }
        if (i == end) return null;
        long acc = 0;
        for (; i < end; i++) {
            int d = b[i] - '0';
            if (d < 0 || d > 9) return null;
            acc = acc * 10 + d;
        }
        return neg ? -acc : acc;
    }

    static Long parseAsciiLong(CharSequence s) {
        int n = s.length();
        int i = 0, end = n;
        while (i < end && s.charAt(i) <= 0x20) i++;
        while (end > i && s.charAt(end - 1) <= 0x20) end--;
        if (i == end) return null;
        boolean neg = false;
        char c0 = s.charAt(i);
        if (c0 == '-') { neg = true; i++; }
        else if (c0 == '+') { i++; }
        if (i == end) return null;
        long acc = 0;
        for (; i < end; i++) {
            int d = s.charAt(i) - '0';
            if (d < 0 || d > 9) return null;
            acc = acc * 10 + d;
        }
        return neg ? -acc : acc;
    }

    private void handleTombstone(SinkRecord record, String collection, SolrBulkProcessor bulk) {
        switch (behaviorOnNullValues) {
            case IGNORE:
                return;
            case FAIL:
                throw new DataException("Tombstone record encountered with behavior=fail: "
                        + record.topic() + "-" + record.kafkaPartition() + "@" + record.kafkaOffset());
            case DELETE:
                Object key = record.key();
                if (key == null) {
                    log.warn("Tombstone has null key, cannot delete; skipping");
                    return;
                }
                // Solr's deleteById accepts only String ids — even when the id field is plong,
                // the wire API requires String form. Solr coerces "12345" → 12345 server-side.
                // So we String-coerce here regardless of id.coerce.to.string.
                String id = key instanceof String ? (String) key : String.valueOf(key);
                bulk.delete(collection, id, offsetTracker.track(record));
                return;
            default:
                throw new DataException("Unknown null value behavior: " + behaviorOnNullValues);
        }
    }

    private void handleMalformed(SinkRecord record, DataException de) {
        switch (behaviorOnMalformed) {
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
        if (proc == lastProcessor) {
            lastProcessor = null;
            lastTopic = null;
            lastPartition = -1;
        }
    }

    private void forEachProcessor(java.util.function.Consumer<SolrBulkProcessor> action) {
        if (partitionFanout) {
            for (SolrBulkProcessor p : perPartition.values()) action.accept(p);
        } else {
            action.accept(sharedProcessor);
        }
    }

    public long recordsWritten()        { return sumLong(SolrBulkProcessor::recordsWritten); }
    public long recordsFailed()         { return sumLong(SolrBulkProcessor::recordsFailed); }
    public long retries()               { return sumLong(SolrBulkProcessor::retries); }
    public int  queueDepth()            { return (int) sumLong(p -> (long) p.queueDepth()); }
    public long batchCount()            { return sumLong(SolrBulkProcessor::batchCount); }
    public long solrCallCount()         { return sumLong(SolrBulkProcessor::solrCallCount); }
    public double avgBatchLatencyMs()   { return weightedAvg(SolrBulkProcessor::avgBatchLatencyMs, SolrBulkProcessor::batchCount); }
    public double avgSolrCallLatencyMs(){ return weightedAvg(SolrBulkProcessor::avgSolrCallLatencyMs, SolrBulkProcessor::solrCallCount); }
    public long lastSuccessEpochMs()    { return maxLong(SolrBulkProcessor::lastSuccessEpochMs); }

    private long sumLong(java.util.function.ToLongFunction<SolrBulkProcessor> f) {
        if (!partitionFanout) return f.applyAsLong(sharedProcessor);
        long total = 0;
        for (SolrBulkProcessor p : perPartition.values()) total += f.applyAsLong(p);
        return total;
    }

    private long maxLong(java.util.function.ToLongFunction<SolrBulkProcessor> f) {
        if (!partitionFanout) return f.applyAsLong(sharedProcessor);
        long max = 0L;
        for (SolrBulkProcessor p : perPartition.values()) {
            long t = f.applyAsLong(p);
            if (t > max) max = t;
        }
        return max;
    }

    private double weightedAvg(java.util.function.ToDoubleFunction<SolrBulkProcessor> value,
                               java.util.function.ToLongFunction<SolrBulkProcessor> weight) {
        if (!partitionFanout) return value.applyAsDouble(sharedProcessor);
        long totalCount = 0;
        double weightedSum = 0.0;
        for (SolrBulkProcessor p : perPartition.values()) {
            long c = weight.applyAsLong(p);
            if (c == 0) continue;
            weightedSum += value.applyAsDouble(p) * c;
            totalCount += c;
        }
        return totalCount == 0 ? 0.0 : weightedSum / totalCount;
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
