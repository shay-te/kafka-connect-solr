package com.una.kafka.connect.solr;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.common.SolrDocument;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end test of OUR ACTUAL data flow: the exact denormalized user document the kstreams
 * JoinStreamsApp emits (schemaless JSON — nested promises_summary object, arrays-of-objects for
 * promises/custom_fields/funnel_leads, flattened package fields, a "lat,lon" location, an "id"
 * that is ALSO the Kafka key) driven through the REAL SolrSinkTask configured exactly like
 * scripts/debezium_setup.py, into a REAL Solr container.
 */
class RealDataFlowIT {

    private static GenericContainer<?> solr;
    private static String baseUrl;
    private static final String CORE = "userdata";

    @BeforeAll
    static void start() {
        solr = new GenericContainer<>(DockerImageName.parse("solr:9.4"))
                .withExposedPorts(8983)
                .withCommand("solr-precreate", CORE)
                .waitingFor(Wait.forHttp("/solr/" + CORE + "/select?q=*:*")
                        .forPort(8983).forStatusCode(200).withStartupTimeout(Duration.ofMinutes(2)));
        solr.start();
        baseUrl = "http://" + solr.getHost() + ":" + solr.getMappedPort(8983) + "/solr";
    }

    @AfterAll
    static void stop() {
        if (solr != null) solr.stop();
    }

    @BeforeEach
    void clean() throws Exception {
        try (Http2SolrClient c = new Http2SolrClient.Builder(baseUrl).build()) {
            c.deleteByQuery(CORE, "*:*");
            c.commit(CORE);
        }
    }

    /** Task configured exactly like the production Solr sink (debezium_setup.py:_add_solr_connector). */
    private SolrSinkTask productionTask() {
        Map<String, String> p = new LinkedHashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, baseUrl);
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, CORE);
        p.put(SolrSinkConfig.KEY_IGNORE_CONFIG, "false");
        p.put(SolrSinkConfig.ID_STRATEGY_CONFIG, "KAFKA_KEY");
        p.put(SolrSinkConfig.SCHEMA_IGNORE_CONFIG, "true");
        p.put(SolrSinkConfig.WRITE_METHOD_CONFIG, "INDEX");
        p.put(SolrSinkConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG, "delete");
        p.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "UNUSED"); // standalone core already exists
        p.put(SolrSinkConfig.MAX_RETRIES_CONFIG, "0");                  // surface errors immediately
        SolrSinkTask task = new SolrSinkTask();
        task.start(p);
        return task;
    }

    /** The real kstreams output document, as the schemaless Map the connector receives. */
    private static Map<String, Object> realUserDocument(int id) {
        Map<String, Object> u = new LinkedHashMap<>();
        u.put("id", id);
        u.put("first_name", "Ada");
        u.put("last_name", "Lovelace");
        u.put("email", "ada@example.com");
        u.put("location", "40.7128,-74.0060");            // "lat,lon" spatial
        u.put("created_at", "2026-01-01");
        u.put("conversation_timeline_status", 2);
        u.put("last_admin_message_date", "2026-06-01");
        // Flattened package fields
        u.put("package_name", "premium");
        u.put("package_id", 9);
        u.put("package_active", true);
        u.put("package_duration_months", 12);
        u.put("package_start_date", "2026-01-01");
        // Nested object: promises_summary { total, counts{...}, worst_status }
        Map<String, Object> counts = new LinkedHashMap<>();
        for (String s : new String[]{"at_risk", "pending", "cancelled", "broken", "kept_late", "kept"}) counts.put(s, 0);
        counts.put("pending", 2);
        counts.put("kept", 1);
        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("total", 3);
        summary.put("counts", counts);
        summary.put("worst_status", 2);
        u.put("promises_summary", summary);
        // Arrays of objects
        u.put("promises", List.of(objMap("id", 100L, "status", 2L, "promiser_name", "Org Admin",
                "promiser_email", "org@x.io", "due_date", "2026-07-01")));
        u.put("custom_fields", List.of(objMap("custom_field_id", 7, "label", "Height", "type", "1", "value_1", 180)));
        u.put("funnel_leads", List.of(objMap("id", 1, "target_id", id, "stage", "new")));
        u.put("comments", List.of(objMap("id", 5, "body", "hello 💙")));
        return u;
    }

    private static Map<String, Object> objMap(Object... kv) {
        Map<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) m.put((String) kv[i], kv[i + 1]);
        return m;
    }

    private long numFound(String q) throws Exception {
        try (Http2SolrClient c = new Http2SolrClient.Builder(baseUrl).build()) {
            return c.query(CORE, new SolrQuery(q)).getResults().getNumFound();
        }
    }

    private SolrDocument fetch(String id) throws Exception {
        try (Http2SolrClient c = new Http2SolrClient.Builder(baseUrl).build()) {
            List<SolrDocument> r = c.query(CORE, new SolrQuery("id:" + id)).getResults();
            return r.isEmpty() ? null : r.get(0);
        }
    }

    @Test
    void realKstreamsUserDocumentIndexesEndToEnd() throws Exception {
        SolrSinkTask task = productionTask();
        try {
            // After the InsertKey/extractKey SMTs, the Kafka key is the doc's id; the value
            // (still containing "id") is schemaless -> valueSchema is null.
            Map<String, Object> value = realUserDocument(42);
            List<SinkRecord> batch = new ArrayList<>();
            batch.add(new SinkRecord("aggregated-user-data-v1", 0,
                    Schema.STRING_SCHEMA, "42", null, value, 1L));
            task.put(batch);
            task.flush(new java.util.HashMap<>());

            try (Http2SolrClient c = new Http2SolrClient.Builder(baseUrl).build()) {
                c.commit(CORE);
            }

            assertThat(numFound("*:*")).as("the real user document must index").isEqualTo(1);
            SolrDocument doc = fetch("42");
            assertThat((Object) doc).isNotNull();
            // Scalars + flattened package fields
            assertThat(doc.getFieldValue("first_name")).isNotNull();
            assertThat(doc.getFieldValue("package_name")).isNotNull();
            assertThat(doc.getFieldValue("location")).isNotNull();
            // Nested object flattened to dotted fields
            List<String> names = new ArrayList<>(doc.getFieldNames());
            assertThat(names.contains("promises_summary.total"))
                    .as("nested promises_summary.total must flatten to a dotted field").isTrue();
            assertThat(names.contains("promises_summary.counts.pending"))
                    .as("doubly-nested promises_summary.counts.pending must flatten too").isTrue();
            // Arrays of objects must flatten to multi-valued dotted fields (not raw maps).
            assertThat(names.contains("promises.id")).as("array-of-objects promises.id must flatten").isTrue();
            assertThat(names.contains("promises.promiser_name")).isTrue();
            assertThat(names.contains("custom_fields.label")).isTrue();
            assertThat(names.contains("funnel_leads.stage")).isTrue();
            assertThat(doc.getFirstValue("promises.id")).isEqualTo(100L); // multi-valued flattened field
            // The value's own 'id' was NOT re-emitted as a second uniqueKey value.
            assertThat(doc.getFieldValue("id")).isEqualTo("42");
        } finally {
            task.stop();
        }
    }

    @Test
    void tombstoneDeletesTheUser() throws Exception {
        SolrSinkTask task = productionTask();
        try {
            task.put(List.of(new SinkRecord("aggregated-user-data-v1", 0,
                    Schema.STRING_SCHEMA, "77", null, realUserDocument(77), 1L)));
            task.flush(new java.util.HashMap<>());
            try (Http2SolrClient c = new Http2SolrClient.Builder(baseUrl).build()) { c.commit(CORE); }
            assertThat(numFound("id:77")).isEqualTo(1);

            // behavior.on.null.values=delete -> a tombstone removes the user.
            task.put(List.of(new SinkRecord("aggregated-user-data-v1", 0,
                    Schema.STRING_SCHEMA, "77", null, null, 2L)));
            task.flush(new java.util.HashMap<>());
            try (Http2SolrClient c = new Http2SolrClient.Builder(baseUrl).build()) { c.commit(CORE); }
            assertThat(numFound("id:77")).isEqualTo(0);
        } finally {
            task.stop();
        }
    }

    // ---------------- CRUD against real Solr with the real document ----------------

    private static final java.util.concurrent.atomic.AtomicLong OFFSET = new java.util.concurrent.atomic.AtomicLong();

    private void put(SolrSinkTask task, String key, Map<String, Object> value) {
        task.put(List.of(new SinkRecord("aggregated-user-data-v1", 0,
                Schema.STRING_SCHEMA, key, null, value, OFFSET.incrementAndGet())));
        task.flush(new java.util.HashMap<>());
    }

    private void commit() throws Exception {
        try (Http2SolrClient c = new Http2SolrClient.Builder(baseUrl).build()) { c.commit(CORE); }
    }

    private List<String> names(SolrDocument d) {
        return new ArrayList<>(d.getFieldNames());
    }

    private static Map<String, Object> userWithPromiseIds(int id, long... promiseIds) {
        Map<String, Object> u = realUserDocument(id);
        List<Map<String, Object>> promises = new ArrayList<>();
        for (long pid : promiseIds) {
            promises.add(objMap("id", pid, "status", 2L, "promiser_name", "Actor" + pid));
        }
        u.put("promises", promises);
        return u;
    }

    @Test
    void createThenUpdateOneToOneReplacesFieldsAndDropsStaleOnes() throws Exception {
        SolrSinkTask task = productionTask();
        try {
            // CREATE
            put(task, "10", realUserDocument(10));
            commit();
            SolrDocument created = fetch("10");
            assertThat((Object) created).isNotNull();
            assertThat(created.getFirstValue("first_name")).isEqualTo("Ada");
            assertThat(names(created).contains("package_name")).isTrue();

            // UPDATE (same id): rename, and DROP package_name entirely.
            Map<String, Object> updated = new LinkedHashMap<>();
            updated.put("id", 10);
            updated.put("first_name", "Grace");
            updated.put("location", "1.0,2.0");
            put(task, "10", updated);
            commit();

            SolrDocument after = fetch("10");
            assertThat(after.getFirstValue("first_name")).as("scalar updated").isEqualTo("Grace");
            assertThat(names(after).contains("package_name"))
                    .as("INDEX is a full replace — the removed field must be gone, not stale").isFalse();
            assertThat(numFound("*:*")).as("update must not create a duplicate doc").isEqualTo(1);
        } finally {
            task.stop();
        }
    }

    @Test
    void updateShrinkingOneToManyArrayRemovesTheDeletedChildren() throws Exception {
        SolrSinkTask task = productionTask();
        try {
            // CREATE with three promises (one user -> many promises).
            put(task, "20", userWithPromiseIds(20, 100L, 101L, 102L));
            commit();
            assertThat(fetch("20").getFieldValues("promises.id"))
                    .as("all three children present").containsExactlyInAnyOrder(100L, 101L, 102L);

            // UPDATE: user now has only one promise — the other two were deleted upstream.
            put(task, "20", userWithPromiseIds(20, 100L));
            commit();
            assertThat(fetch("20").getFieldValues("promises.id"))
                    .as("deleted children must NOT linger as ghosts").containsExactly(100L);
        } finally {
            task.stop();
        }
    }

    @Test
    void deletingOneUserLeavesTheOthersIntact() throws Exception {
        SolrSinkTask task = productionTask();
        try {
            put(task, "30", realUserDocument(30));
            put(task, "31", realUserDocument(31));
            put(task, "32", realUserDocument(32));
            commit();
            assertThat(numFound("*:*")).isEqualTo(3);

            // DELETE the middle one via tombstone.
            put(task, "31", null);
            commit();

            assertThat(numFound("id:31")).isEqualTo(0);
            assertThat(numFound("id:30")).isEqualTo(1);
            assertThat(numFound("id:32")).isEqualTo(1);
            assertThat(numFound("*:*")).isEqualTo(2);
        } finally {
            task.stop();
        }
    }

}
