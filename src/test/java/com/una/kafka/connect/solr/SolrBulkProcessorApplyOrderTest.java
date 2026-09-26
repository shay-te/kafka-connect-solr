package com.una.kafka.connect.solr;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Apply-order guarantee of {@link SolrBulkProcessor}: within one buffered put()/flush interval,
 * upserts and deletes for the same collection must reach Solr in Kafka-offset (insertion) order.
 *
 * <p>The historical bug: the processor kept two independent per-collection buffers (upserts and
 * deletes) and flushAsync() always sent ALL upserts before ALL deletes. So the Kafka sequence
 * [delete@N, create@N+1] for the same key was applied as create-then-delete, silently deleting a
 * re-created document. {@code max.in.flight.requests=1} does not help — order was already broken
 * when the ops were partitioned into the two buffers.
 *
 * <p>All tests run against a REAL in-process Solr ({@link EmbeddedSolrServer}) — genuine index,
 * genuine update chain, no mocks.
 */
class SolrBulkProcessorApplyOrderTest {

    private static final String TOPIC = EmbeddedSolrSupport.CORE;

    private EmbeddedSolrServer solr;

    @AfterEach
    void tearDown() {
        EmbeddedSolrSupport.stop(solr);
    }

    private Map<String, String> baseProps() {
        Map<String, String> p = new LinkedHashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://embedded/solr");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, EmbeddedSolrSupport.CORE);
        p.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "UNUSED");
        p.put(SolrSinkConfig.SCHEMA_IGNORE_CONFIG, "true");
        p.put(SolrSinkConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG, "delete");
        // Keep every op buffered until the explicit flush so the delete/create pair
        // lands in ONE flush interval — the exact shape of the reported bug.
        p.put(SolrSinkConfig.BATCH_SIZE_CONFIG, "1000");
        p.put(SolrSinkConfig.LINGER_MS_CONFIG, "60000");
        p.put(SolrSinkConfig.MAX_BUFFERED_RECORDS_CONFIG, "100000");
        // 1 in-flight request: proves serial dispatch alone cannot repair the order.
        p.put(SolrSinkConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG, "1");
        return p;
    }

    private SolrWriter writer() {
        return new SolrWriter(solr, new SolrSinkConfig(baseProps()), new SyncOffsetTracker());
    }

    private static SinkRecord rec(String id, String name, long offset) {
        Map<String, Object> v = new LinkedHashMap<>();
        v.put("id", id);
        v.put("name", name);
        return new SinkRecord(TOPIC, 0, Schema.STRING_SCHEMA, id, null, v, offset);
    }

    private static SinkRecord tombstone(String id, long offset) {
        return new SinkRecord(TOPIC, 0, Schema.STRING_SCHEMA, id, null, null, offset);
    }

    private static SolrInputDocument doc(String id, String name) {
        SolrInputDocument d = new SolrInputDocument();
        d.addField("id", id);
        d.addField("name", name);
        return d;
    }

    private long numFound(String core, String q) throws Exception {
        solr.commit(core);
        return solr.query(core, new SolrQuery(q)).getResults().getNumFound();
    }

    // ---------------------------------------------------------------------------------------
    // (1) THE reproducer: delete@N then create@N+1 for the same key, buffered together.
    //     On the old two-buffer code the create was sent BEFORE the delete, so the re-created
    //     user ended up deleted. The doc must EXIST after the flush.
    // ---------------------------------------------------------------------------------------
    @Test
    void deleteThenRecreateSameKeyInOneFlushIntervalKeepsTheDocument() throws Exception {
        solr = EmbeddedSolrSupport.start();
        try (SolrWriter w = writer()) {
            // Seed: the user exists (its own flush interval).
            w.write(rec("u1", "original", 50));
            w.flush();
            assertThat(numFound(TOPIC, "id:u1")).isEqualTo(1);

            // Kafka order in ONE put interval: delete@100 then re-create@101.
            w.write(tombstone("u1", 100));
            w.write(rec("u1", "recreated", 101));
            w.flush();

            assertThat(numFound(TOPIC, "id:u1"))
                    .as("delete@100 then create@101 buffered together must leave the doc ALIVE")
                    .isEqualTo(1);
            assertThat(solr.query(TOPIC, new SolrQuery("id:u1")).getResults()
                    .get(0).getFirstValue("name")).isEqualTo("recreated");
        }
    }

    // ---------------------------------------------------------------------------------------
    // (2) The reverse: create@N then delete@N+1 must stay deleted.
    // ---------------------------------------------------------------------------------------
    @Test
    void createThenDeleteSameKeyInOneFlushIntervalStaysDeleted() throws Exception {
        solr = EmbeddedSolrSupport.start();
        try (SolrWriter w = writer()) {
            w.write(rec("u2", "shortlived", 200));
            w.write(tombstone("u2", 201));
            w.flush();

            assertThat(numFound(TOPIC, "id:u2"))
                    .as("create@200 then delete@201 buffered together must leave the doc DELETED")
                    .isEqualTo(0);
        }
    }

    // ---------------------------------------------------------------------------------------
    // (3a) Interleaving across two keys: each key's ops must apply in its own Kafka order.
    // ---------------------------------------------------------------------------------------
    @Test
    void interleavedOpsAcrossTwoKeysApplyInKafkaOrder() throws Exception {
        solr = EmbeddedSolrSupport.start();
        try (SolrWriter w = writer()) {
            // Kafka order: create a, create b, delete a, re-create a, delete b.
            w.write(rec("a", "a-v1", 300));
            w.write(rec("b", "b-v1", 301));
            w.write(tombstone("a", 302));
            w.write(rec("a", "a-v2", 303));
            w.write(tombstone("b", 304));
            w.flush();

            assertThat(numFound(TOPIC, "id:a")).as("a was re-created after its delete").isEqualTo(1);
            assertThat(solr.query(TOPIC, new SolrQuery("id:a")).getResults()
                    .get(0).getFirstValue("name")).isEqualTo("a-v2");
            assertThat(numFound(TOPIC, "id:b")).as("b's delete was its last op").isEqualTo(0);
        }
    }

    // ---------------------------------------------------------------------------------------
    // (3b) Interleaving across two collections (real cores): per-collection order holds even
    //      when ops for the two collections alternate in the shared processor.
    // ---------------------------------------------------------------------------------------
    @Test
    void interleavedOpsAcrossTwoCollectionsKeepPerCollectionOrder() throws Exception {
        solr = EmbeddedSolrSupport.start("embedded-solr", "orders");
        Map<String, String> p = baseProps();
        SolrBulkProcessor bulk = new SolrBulkProcessor(solr, new SolrSinkConfig(p));
        try {
            // Seed both cores.
            bulk.upsert("users", doc("x", "users-seed"), null);
            bulk.upsert("orders", doc("x", "orders-seed"), null);
            bulk.flushSync();
            assertThat(numFound("users", "id:x")).isEqualTo(1);
            assertThat(numFound("orders", "id:x")).isEqualTo(1);

            // Interleaved: users[del x, add x], orders[del x, add x] — alternating collections.
            bulk.delete("users", "x", null);
            bulk.delete("orders", "x", null);
            bulk.upsert("users", doc("x", "users-recreated"), null);
            bulk.upsert("orders", doc("x", "orders-recreated"), null);
            bulk.flushSync();

            assertThat(numFound("users", "id:x"))
                    .as("users: delete-then-create must leave doc alive").isEqualTo(1);
            assertThat(numFound("orders", "id:x"))
                    .as("orders: delete-then-create must leave doc alive").isEqualTo(1);
            assertThat(solr.query("users", new SolrQuery("id:x")).getResults()
                    .get(0).getFirstValue("name")).isEqualTo("users-recreated");
            assertThat(solr.query("orders", new SolrQuery("id:x")).getResults()
                    .get(0).getFirstValue("name")).isEqualTo("orders-recreated");
        } finally {
            bulk.close();
        }
    }

    // ---------------------------------------------------------------------------------------
    // (4a) batch.size applies to the COMBINED ordered buffer: mixed ops totalling batch.size
    //      flush without any explicit flush call — and in order.
    // ---------------------------------------------------------------------------------------
    @Test
    void batchSizeThresholdCountsMixedOpsAndFlushesInOrder() throws Exception {
        solr = EmbeddedSolrSupport.start();
        Map<String, String> p = baseProps();
        p.put(SolrSinkConfig.BATCH_SIZE_CONFIG, "3");
        SolrBulkProcessor bulk = new SolrBulkProcessor(solr, new SolrSinkConfig(p));
        try {
            bulk.upsert("users", doc("t1", "v1"), null);   // 1
            bulk.delete("users", "t1", null);              // 2
            bulk.upsert("users", doc("t1", "v2"), null);   // 3 -> combined threshold trips

            awaitRecordsWritten(bulk, 3);                  // flushed WITHOUT flushSync
            assertThat(numFound("users", "id:t1"))
                    .as("add,del,add flushed by batch.size must end with the doc alive")
                    .isEqualTo(1);
            assertThat(solr.query("users", new SolrQuery("id:t1")).getResults()
                    .get(0).getFirstValue("name")).isEqualTo("v2");
        } finally {
            bulk.close();
        }
    }

    // ---------------------------------------------------------------------------------------
    // (4b) bulk.size.bytes still trips a flush on the combined buffer.
    // ---------------------------------------------------------------------------------------
    @Test
    void bulkSizeBytesThresholdStillFlushes() throws Exception {
        solr = EmbeddedSolrSupport.start();
        Map<String, String> p = baseProps();
        p.put(SolrSinkConfig.BULK_SIZE_BYTES_CONFIG, "1"); // any upsert trips the byte threshold
        SolrBulkProcessor bulk = new SolrBulkProcessor(solr, new SolrSinkConfig(p));
        try {
            bulk.delete("users", "nope", null);            // buffered (deletes carry no bytes)
            bulk.upsert("users", doc("y1", "bytes"), null); // trips bytes -> flushes BOTH, in order

            awaitRecordsWritten(bulk, 2);
            assertThat(numFound("users", "id:y1")).isEqualTo(1);
        } finally {
            bulk.close();
        }
    }

    /** Poll until the processor has written at least {@code target} records, or fail after 10 s. */
    private static void awaitRecordsWritten(SolrBulkProcessor bulk, long target) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000L;
        while (bulk.recordsWritten() < target && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertThat(bulk.recordsWritten())
                .as("records written by threshold-triggered flush")
                .isGreaterThanOrEqualTo(target);
    }
}
