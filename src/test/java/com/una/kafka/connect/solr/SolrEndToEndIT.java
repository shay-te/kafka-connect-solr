package com.una.kafka.connect.solr;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Drives the full SolrSinkTask -> SolrWriter -> SolrBulkProcessor -> SolrJ
 * pipeline against a real Solr 9.4 container. Each test queries by unique IDs
 * so cross-test pollution doesn't matter; the container is shared.
 */
class SolrEndToEndIT {

    private static GenericContainer<?> container;
    private static String baseUrl;

    @BeforeAll
    static void start() throws Exception {
        container = new GenericContainer<>(DockerImageName.parse("solr:9.4"))
                .withExposedPorts(8983)
                .withCommand("solr-precreate", "e2e");
        container.start();
        baseUrl = "http://" + container.getHost() + ":" + container.getMappedPort(8983) + "/solr";
        SolrTestSupport.awaitCoreReady(baseUrl, "e2e");
    }

    @AfterAll
    static void stop() {
        if (container != null) container.stop();
    }

    private Map<String, String> baseProps() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, baseUrl);
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "e2e");
        p.put(SolrSinkConfig.BATCH_SIZE_CONFIG, "5");
        p.put(SolrSinkConfig.LINGER_MS_CONFIG, "10");
        p.put(SolrSinkConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG, "2");
        p.put(SolrSinkConfig.MAX_RETRIES_CONFIG, "0");
        p.put(SolrSinkConfig.COMMIT_WITHIN_MS_CONFIG, "100");
        // solr-precreate enables schemaless field add-on-the-fly; do not double-handle.
        p.put(SolrSinkConfig.SCHEMA_AUTO_EVOLVE_CONFIG, "false");
        p.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "UNUSED");
        return p;
    }

    /** Poll Solr until `query` returns {@code expected} hits, or fail after 5s. */
    private static void awaitHits(String query, long expected) throws Exception {
        long deadline = System.currentTimeMillis() + 5_000L;
        long last = -1;
        try (SolrClient verifier = new Http2SolrClient.Builder(baseUrl).build()) {
            while (System.currentTimeMillis() < deadline) {
                last = verifier.query("e2e", new SolrQuery(query).setRows(0))
                        .getResults().getNumFound();
                if (last == expected) return;
                Thread.sleep(50);
            }
        }
        assertThat(last).as("expected %d hits for '%s'", expected, query).isEqualTo(expected);
    }

    @Test
    void writesStructDocumentsAndReadsThemBackById() throws Exception {
        SolrSinkTask task = new SolrSinkTask();
        task.start(baseProps());
        try {
            Schema s = SchemaBuilder.struct()
                    .field("first_name", Schema.STRING_SCHEMA)
                    .field("age", Schema.INT32_SCHEMA)
                    .build();
            List<SinkRecord> batch = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                Struct v = new Struct(s).put("first_name", "Ada-" + i).put("age", 30 + i);
                batch.add(new SinkRecord("e2e", 0, Schema.STRING_SCHEMA,
                        "writes-" + i, s, v, i));
            }
            task.put(batch);
            task.flush(new HashMap<>());
            awaitHits("id:writes-*", 10L);
        } finally {
            task.stop();
        }
    }

    @Test
    void tombstoneDeletesPreviouslyIndexedDoc() throws Exception {
        Map<String, String> p = baseProps();
        p.put(SolrSinkConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG, "delete");
        SolrSinkTask task = new SolrSinkTask();
        task.start(p);
        try {
            Schema s = SchemaBuilder.struct().field("first_name", Schema.STRING_SCHEMA).build();
            Struct v = new Struct(s).put("first_name", "soon-to-be-deleted");
            task.put(Collections.singletonList(
                    new SinkRecord("e2e", 0, Schema.STRING_SCHEMA, "tomb-doc", s, v, 1L)));
            task.flush(new HashMap<>());
            awaitHits("id:tomb-doc", 1L);

            // Tombstone: same key, null value.
            task.put(Collections.singletonList(
                    new SinkRecord("e2e", 0, Schema.STRING_SCHEMA, "tomb-doc", null, null, 2L)));
            task.flush(new HashMap<>());
            awaitHits("id:tomb-doc", 0L);
        } finally {
            task.stop();
        }
    }

    @Test
    void asyncFlushPathDrainsAndCommits() throws Exception {
        Map<String, String> p = baseProps();
        p.put(SolrSinkConfig.FLUSH_SYNCHRONOUSLY_CONFIG, "false");
        SolrSinkTask task = new SolrSinkTask();
        task.start(p);
        try {
            Schema s = SchemaBuilder.struct().field("first_name", Schema.STRING_SCHEMA).build();
            List<SinkRecord> batch = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                Struct v = new Struct(s).put("first_name", "async-" + i);
                batch.add(new SinkRecord("e2e", 1, Schema.STRING_SCHEMA,
                        "async-" + i, s, v, 100L + i));
            }
            task.put(batch);
            // Sync drain via flush, then exercise the async preCommit path.
            task.flush(new HashMap<>());
            task.preCommit(new HashMap<>());
            awaitHits("id:async-*", 5L);
        } finally {
            task.stop();
        }
    }

    @Test
    void retriesOnTransientFailureExhaustsAndThrows() throws Exception {
        // Unreachable port forces network errors; retries exhaust and we get RetriableException.
        Map<String, String> p = baseProps();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://" + container.getHost() + ":1/solr");
        p.put(SolrSinkConfig.MAX_RETRIES_CONFIG, "2");
        p.put(SolrSinkConfig.RETRY_BACKOFF_MS_CONFIG, "5");
        p.put(SolrSinkConfig.FLUSH_TIMEOUT_MS_CONFIG, "5000");
        SolrSinkTask task = new SolrSinkTask();
        task.start(p);
        try {
            Schema s = SchemaBuilder.struct().field("first_name", Schema.STRING_SCHEMA).build();
            Struct v = new Struct(s).put("first_name", "bob");
            task.put(Collections.singletonList(
                    new SinkRecord("e2e", 0, Schema.STRING_SCHEMA, "retry-1", s, v, 1L)));

            assertThatThrownBy(() -> task.flush(new HashMap<>()))
                    .isInstanceOf(RetriableException.class);
        } finally {
            task.stop();
        }
    }

    @Test
    void streamingClientWritesAndDrainsByExactId() throws Exception {
        Map<String, String> p = baseProps();
        p.put(SolrSinkConfig.STREAMING_ENABLED_CONFIG, "true");
        p.put(SolrSinkConfig.STREAMING_QUEUE_SIZE_CONFIG, "100");
        p.put(SolrSinkConfig.STREAMING_THREADS_CONFIG, "2");
        SolrSinkTask task = new SolrSinkTask();
        task.start(p);
        try {
            Schema s = SchemaBuilder.struct().field("first_name", Schema.STRING_SCHEMA).build();
            List<SinkRecord> batch = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                Struct v = new Struct(s).put("first_name", "stream-" + i);
                batch.add(new SinkRecord("e2e", 0, Schema.STRING_SCHEMA,
                        "stream-" + i, s, v, 200L + i));
            }
            task.put(batch);
            task.flush(new HashMap<>());
            awaitHits("id:stream-*", 5L);
        } finally {
            task.stop();
        }
    }
}
