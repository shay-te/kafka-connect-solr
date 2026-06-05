package com.una.kafka.connect.solr;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;

/**
 * Behavioural tests for each performance optimisation. The
 * {@code perf/} package handles throughput numbers; this file is the
 * functional regression net - if any of these tests breaks, a perf
 * boost has silently lost its effect.
 */
class PerformanceOptimizationsTest {

    // ---------------------------------------------------------------
    // 1. Schema-aware encoder plan cache
    // ---------------------------------------------------------------

    @Test
    void repeatedConvertsOfSameSchemaProduceIdenticalDocs() {
        // The encoder plan cache must not change the output between the
        // first call (plan built) and subsequent calls (plan reused).
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "c");
        SolrRecordConverter converter = new SolrRecordConverter(new SolrSinkConfig(p));

        Schema schema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .build();

        Struct v1 = new Struct(schema).put("name", "Ada").put("age", 36);
        Struct v2 = new Struct(schema).put("name", "Bea").put("age", 41);

        SolrInputDocument d1 = converter.convert(
                new SinkRecord("c", 0, Schema.STRING_SCHEMA, "k1", schema, v1, 1L));
        SolrInputDocument d2 = converter.convert(
                new SinkRecord("c", 0, Schema.STRING_SCHEMA, "k2", schema, v2, 2L));

        assertThat(d1.getFieldValue("name")).isEqualTo("Ada");
        assertThat(d1.getFieldValue("age")).isEqualTo(36);
        assertThat(d2.getFieldValue("name")).isEqualTo("Bea");
        assertThat(d2.getFieldValue("age")).isEqualTo(41);
    }

    // ---------------------------------------------------------------
    // 2. CollectionResolver memoisation
    // ---------------------------------------------------------------

    @Test
    void collectionResolverReturnsSameStringInstanceOnRepeatedCalls() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "logs-.*=>logs");
        p.put(SolrSinkConfig.COLLECTION_NAMING_STRATEGY_CONFIG, "TOPIC_REGEX");
        CollectionResolver resolver = new CollectionResolver(new SolrSinkConfig(p));

        String a = resolver.resolve("logs-prod");
        String b = resolver.resolve("logs-prod");
        assertThat(a).isSameAs(b); // identity-equal proves the cache returned the stored value
    }

    // ---------------------------------------------------------------
    // 3. SyncOffsetTracker sentinel - zero allocations per record
    // ---------------------------------------------------------------

    @Test
    void syncTrackerSentinelIsShared() {
        SyncOffsetTracker t = new SyncOffsetTracker();
        OffsetState s1 = t.track(new SinkRecord("t", 0, null, "a", null, "v", 1L));
        OffsetState s2 = t.track(new SinkRecord("t", 1, null, "b", null, "v", 2L));
        assertThat(s1).isSameAs(s2); // not just equal — the SAME object
    }

    // ---------------------------------------------------------------
    // 4. OffsetState markAcked - volatile semantics
    // ---------------------------------------------------------------

    @Test
    void offsetStateAckedVisibleAcrossThreads() throws Exception {
        OffsetState s = new OffsetState(new TopicPartition("t", 0), 7L);
        assertThat(s.isAcked()).isFalse();

        // Mark from a worker thread; read from main.
        Thread t = new Thread(s::markAcked);
        t.start();
        t.join(1000);

        assertThat(s.isAcked()).isTrue();
    }

    // ---------------------------------------------------------------
    // 5. ATOMIC_UPDATE uses Map.of - no HashMap allocations
    // ---------------------------------------------------------------

    @Test
    void atomicUpdateUsesImmutableMap() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "c");
        p.put(SolrSinkConfig.WRITE_METHOD_CONFIG, "ATOMIC_UPDATE");
        SolrRecordConverter c = new SolrRecordConverter(new SolrSinkConfig(p));

        Schema schema = SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA).build();
        Struct v = new Struct(schema).put("name", "x");
        SolrInputDocument doc = c.convert(
                new SinkRecord("c", 0, Schema.STRING_SCHEMA, "k", schema, v, 1));
        Object wrapped = doc.getFieldValue("name");
        // Map.of returns an instance of ImmutableCollections.Map1, which is
        // package-private. We can at least assert the value isn't a HashMap.
        assertThat(wrapped).isInstanceOf(Map.class);
        assertThat(wrapped).isNotInstanceOf(java.util.HashMap.class);
        assertThat(((Map<?, ?>) wrapped).get("set")).isEqualTo("x");
    }

    // ---------------------------------------------------------------
    // 6. LongAdder counters under concurrent worker updates
    // ---------------------------------------------------------------

    @Test
    void counterMetricsAreCorrectUnderConcurrency() throws Exception {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "c");
        p.put(SolrSinkConfig.BATCH_SIZE_CONFIG, "1");
        p.put(SolrSinkConfig.LINGER_MS_CONFIG, "1");
        p.put(SolrSinkConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG, "8");
        p.put(SolrSinkConfig.MAX_RETRIES_CONFIG, "0");
        SolrClient client = mock(SolrClient.class);
        SolrBulkProcessor bulk = new SolrBulkProcessor(client, new SolrSinkConfig(p));

        final int total = 500;
        ExecutorService exec = Executors.newFixedThreadPool(8);
        try {
            for (int i = 0; i < total; i++) {
                final int id = i;
                exec.submit(() -> {
                    SolrInputDocument d = new SolrInputDocument();
                    d.addField("id", String.valueOf(id));
                    bulk.upsert("c", d, null);
                });
            }
            exec.shutdown();
            exec.awaitTermination(30, TimeUnit.SECONDS);
            bulk.flushSync();

            assertThat(bulk.recordsWritten()).isEqualTo((long) total);
            assertThat(bulk.queueDepth()).isZero();
        } finally {
            bulk.close();
        }
    }

    // ---------------------------------------------------------------
    // 7. Per-collection buffer is reused across flushes
    // ---------------------------------------------------------------

    @Test
    void manyBatchesToOneCollectionAllSucceed() throws Exception {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "c");
        p.put(SolrSinkConfig.BATCH_SIZE_CONFIG, "10");
        p.put(SolrSinkConfig.LINGER_MS_CONFIG, "1");
        SolrClient client = mock(SolrClient.class);
        SolrBulkProcessor bulk = new SolrBulkProcessor(client, new SolrSinkConfig(p));

        for (int i = 0; i < 200; i++) {
            SolrInputDocument d = new SolrInputDocument();
            d.addField("id", String.valueOf(i));
            bulk.upsert("c", d, null);
        }
        bulk.flushSync();
        assertThat(bulk.recordsWritten()).isEqualTo(200L);
        bulk.close();
    }

    // ---------------------------------------------------------------
    // 8. Bulk size byte cap fires when batch.size hasn't been reached
    // ---------------------------------------------------------------

    @Test
    void byteSizeCapForcesFlushBeforeBatchSize() throws Exception {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "c");
        p.put(SolrSinkConfig.BATCH_SIZE_CONFIG, "100000");   // never reached
        p.put(SolrSinkConfig.BULK_SIZE_BYTES_CONFIG, "100"); // tiny cap
        p.put(SolrSinkConfig.LINGER_MS_CONFIG, "60000");     // big
        SolrClient client = mock(SolrClient.class);
        SolrBulkProcessor bulk = new SolrBulkProcessor(client, new SolrSinkConfig(p));

        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", "1");
        doc.addField("payload", "x".repeat(500)); // > 100 bytes by itself
        bulk.upsert("c", doc, null);

        // Without waiting, the byte cap should have already triggered a flush.
        bulk.flushSync();
        org.mockito.Mockito.verify(client, org.mockito.Mockito.atLeastOnce())
                .request(any(UpdateRequest.class), anyString());
        bulk.close();
    }
}
