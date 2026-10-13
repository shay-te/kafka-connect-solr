package com.una.kafka.connect.solr;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Stress-tests the numeric-id pass-through path under concurrent access to a single
 * {@link SolrRecordConverter} instance. The Kafka Connect contract only requires
 * single-threaded {@code put()}, but the converter has internal caches (planCache,
 * ThreadLocal builders) that are easy to break — these tests make the safety
 * guarantees explicit.
 *
 * <p>Failure mode if anything is wrong: dropped fields, swapped ids, NPEs, hangs.</p>
 */
class SolrRecordConverterConcurrencyTest {

    private static final int THREADS = 8;
    private static final int RECORDS_PER_THREAD = 5_000;
    private static final long TEST_TIMEOUT_SECONDS = 60L;

    private SolrSinkConfig coerceFalse() {
        Map<String, String> overrides = new HashMap<>();
        overrides.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        overrides.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "c");
        overrides.put(SolrSinkConfig.ID_COERCE_TO_STRING_CONFIG, "false");
        return new SolrSinkConfig(overrides);
    }

    @Test
    void sharedSchemaConcurrentLongKeysEachIdMatchesItsRecord() throws Exception {
        SolrRecordConverter converter = new SolrRecordConverter(coerceFalse());
        Schema valueSchema = SchemaBuilder.struct().field("payload", Schema.STRING_SCHEMA).build();

        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        CyclicBarrier start = new CyclicBarrier(THREADS);
        CountDownLatch done = new CountDownLatch(THREADS);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Map<Long, Long> roundtrip = new ConcurrentHashMap<>();

        try {
            for (int threadIdx = 0; threadIdx < THREADS; threadIdx++) {
                final long base = (long) threadIdx * RECORDS_PER_THREAD;
                pool.submit(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < RECORDS_PER_THREAD; i++) {
                            long expectedId = base + i;
                            Struct value = new Struct(valueSchema).put("payload", "x");
                            SinkRecord record = new SinkRecord("c", 0,
                                    Schema.INT64_SCHEMA, expectedId,
                                    valueSchema, value, expectedId);
                            SolrInputDocument doc = converter.convert(record);
                            Object actualId = doc.getFieldValue("id");
                            if (!(actualId instanceof Long) || ((Long) actualId) != expectedId) {
                                failure.compareAndSet(null, new AssertionError(
                                        "thread " + Thread.currentThread().getName()
                                                + " expected " + expectedId + " got " + actualId));
                                return;
                            }
                            roundtrip.put(expectedId, (Long) actualId);
                        }
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    } finally {
                        done.countDown();
                    }
                });
            }
            assertThat(done.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                    .as("all threads should complete within %d s", TEST_TIMEOUT_SECONDS)
                    .isTrue();
            assertThat(failure.get()).as("no failure across %d threads", THREADS).isNull();
            assertThat(roundtrip).hasSize(THREADS * RECORDS_PER_THREAD);
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void distinctSchemasPerThreadDoNotCorruptPlanCache() throws Exception {
        // Each thread uses its OWN Schema instance — forces planCache writes from every thread.
        // This is the canonical "concurrent put into IdentityHashMap" stress shape.
        SolrRecordConverter converter = new SolrRecordConverter(coerceFalse());

        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        CyclicBarrier start = new CyclicBarrier(THREADS);
        CountDownLatch done = new CountDownLatch(THREADS);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        try {
            for (int threadIdx = 0; threadIdx < THREADS; threadIdx++) {
                final int tIdx = threadIdx;
                pool.submit(() -> {
                    try {
                        // Each thread builds a unique schema (different field name) so its planCache
                        // entry is a distinct map slot.
                        Schema valueSchema = SchemaBuilder.struct()
                                .field("payload_" + tIdx, Schema.STRING_SCHEMA)
                                .build();
                        start.await();
                        for (int i = 0; i < RECORDS_PER_THREAD; i++) {
                            long expectedId = (long) tIdx * RECORDS_PER_THREAD + i;
                            Struct value = new Struct(valueSchema).put("payload_" + tIdx, "x");
                            SinkRecord record = new SinkRecord("c", 0,
                                    Schema.INT64_SCHEMA, expectedId,
                                    valueSchema, value, expectedId);
                            SolrInputDocument doc = converter.convert(record);
                            Object actualId = doc.getFieldValue("id");
                            if (!(actualId instanceof Long) || ((Long) actualId) != expectedId) {
                                failure.compareAndSet(null, new AssertionError(
                                        "id mismatch on thread " + tIdx
                                                + " expected " + expectedId + " got " + actualId));
                                return;
                            }
                            // Verify the field-name plan was applied correctly for this thread's schema.
                            if (doc.getFieldValue("payload_" + tIdx) == null) {
                                failure.compareAndSet(null, new AssertionError(
                                        "field 'payload_" + tIdx + "' missing on thread " + tIdx));
                                return;
                            }
                        }
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    } finally {
                        done.countDown();
                    }
                });
            }
            assertThat(done.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
            assertThat(failure.get()).isNull();
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void mixedStringAndNumericKeysConcurrentlyDoNotCrossContaminate() throws Exception {
        SolrRecordConverter converter = new SolrRecordConverter(coerceFalse());
        Schema valueSchema = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();

        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        CyclicBarrier start = new CyclicBarrier(THREADS);
        CountDownLatch done = new CountDownLatch(THREADS);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        try {
            for (int threadIdx = 0; threadIdx < THREADS; threadIdx++) {
                final int tIdx = threadIdx;
                pool.submit(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < RECORDS_PER_THREAD; i++) {
                            Struct value = new Struct(valueSchema).put("x", "v");
                            boolean numericKey = (tIdx + i) % 2 == 0;
                            Object expectedId;
                            Schema keySchema;
                            if (numericKey) {
                                expectedId = (long) tIdx * RECORDS_PER_THREAD + i;
                                keySchema = Schema.INT64_SCHEMA;
                            } else {
                                expectedId = "thr-" + tIdx + "-rec-" + i;
                                keySchema = Schema.STRING_SCHEMA;
                            }
                            SinkRecord record = new SinkRecord("c", 0,
                                    keySchema, expectedId, valueSchema, value, i);
                            SolrInputDocument doc = converter.convert(record);
                            Object actualId = doc.getFieldValue("id");
                            if (!expectedId.equals(actualId)) {
                                failure.compareAndSet(null, new AssertionError(
                                        "id mismatch: thread=" + tIdx + " i=" + i
                                                + " expected=" + expectedId + " got=" + actualId));
                                return;
                            }
                            if (numericKey && !(actualId instanceof Long)) {
                                failure.compareAndSet(null, new AssertionError(
                                        "expected Long id but got " + actualId.getClass()));
                                return;
                            }
                            if (!numericKey && !(actualId instanceof String)) {
                                failure.compareAndSet(null, new AssertionError(
                                        "expected String id but got " + actualId.getClass()));
                                return;
                            }
                        }
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    } finally {
                        done.countDown();
                    }
                });
            }
            assertThat(done.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
            assertThat(failure.get()).isNull();
        } finally {
            pool.shutdownNow();
        }
    }

    @Test
    void tpoStrategyConcurrentlyProducesUniqueStringIds() throws Exception {
        // TOPIC_PARTITION_OFFSET uses a ThreadLocal StringBuilder — concurrent calls must
        // not produce ids belonging to a different thread's record.
        Map<String, String> overrides = new HashMap<>();
        overrides.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        overrides.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "c");
        overrides.put(SolrSinkConfig.ID_STRATEGY_CONFIG, "TOPIC_PARTITION_OFFSET");
        SolrRecordConverter converter = new SolrRecordConverter(new SolrSinkConfig(overrides));
        Schema valueSchema = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();

        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        CyclicBarrier start = new CyclicBarrier(THREADS);
        CountDownLatch done = new CountDownLatch(THREADS);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        // Every (topic-partition-offset) combination is unique across the test.
        ConcurrentHashMap<String, Boolean> seen = new ConcurrentHashMap<>();

        try {
            for (int threadIdx = 0; threadIdx < THREADS; threadIdx++) {
                final int tIdx = threadIdx;
                pool.submit(() -> {
                    try {
                        start.await();
                        for (int i = 0; i < RECORDS_PER_THREAD; i++) {
                            Struct value = new Struct(valueSchema).put("x", "v");
                            String topic = "topic-" + tIdx;
                            int partition = tIdx;
                            long offset = i;
                            SinkRecord record = new SinkRecord(topic, partition,
                                    null, null, valueSchema, value, offset);
                            SolrInputDocument doc = converter.convert(record);
                            String expectedId = topic + "-" + partition + "-" + offset;
                            Object actualId = doc.getFieldValue("id");
                            if (!expectedId.equals(actualId)) {
                                failure.compareAndSet(null, new AssertionError(
                                        "TPO mismatch: expected=" + expectedId + " got=" + actualId));
                                return;
                            }
                            if (seen.putIfAbsent(expectedId, Boolean.TRUE) != null) {
                                failure.compareAndSet(null, new AssertionError(
                                        "duplicate id observed across threads: " + expectedId));
                                return;
                            }
                        }
                    } catch (Throwable t) {
                        failure.compareAndSet(null, t);
                    } finally {
                        done.countDown();
                    }
                });
            }
            assertThat(done.await(TEST_TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
            assertThat(failure.get()).isNull();
            assertThat(seen).hasSize(THREADS * RECORDS_PER_THREAD);
        } finally {
            pool.shutdownNow();
        }
    }
}
