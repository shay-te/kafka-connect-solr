package com.una.kafka.connect.solr;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link SolrWriter} with real, nested CDC-shaped records against a real in-process Solr: the
 * stored {@code source.field} JSON of a Struct with nested lists/maps, version headers that are
 * absent or not numeric, tombstone deletes keyed by a number, key-ignore settings, schemaless
 * maps the converter must skip parts of, and a client whose close() fails.
 */
class SolrWriterRecordShapesEmbeddedTest {

    private static final String CORE = EmbeddedSolrSupport.CORE;
    private static final ObjectMapper JSON = new ObjectMapper();

    private static final Schema ADDRESS = SchemaBuilder.struct().name("address").optional()
            .field("city", Schema.STRING_SCHEMA)
            .field("zip", Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    private static final Schema ORDER = SchemaBuilder.struct().name("order")
            .field("sku", Schema.STRING_SCHEMA)
            .field("qty", Schema.INT32_SCHEMA)
            .build();
    private static final Schema USER = SchemaBuilder.struct().name("user")
            .field("id", Schema.STRING_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .field("address", ADDRESS)
            .field("orders", SchemaBuilder.array(ORDER).optional().build())
            .field("attributes", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional().build())
            .field("tags", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
            .build();

    private EmbeddedSolrServer solr;

    @BeforeEach
    void setUp() throws Exception {
        solr = EmbeddedSolrSupport.start();
    }

    @AfterEach
    void tearDown() {
        EmbeddedSolrSupport.stop(solr);
    }

    private SolrSinkConfig config(String... kv) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://embedded/solr");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, CORE);
        p.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "UNUSED");
        p.put(SolrSinkConfig.BATCH_SIZE_CONFIG, "100");
        p.put(SolrSinkConfig.LINGER_MS_CONFIG, "600000");
        for (int i = 0; i < kv.length; i += 2) p.put(kv[i], kv[i + 1]);
        return new SolrSinkConfig(p);
    }

    private SolrWriter writer(SolrSinkConfig cfg) {
        return new SolrWriter(new EmbeddedSolrServer(solr.getCoreContainer(), CORE), cfg);
    }

    private static Struct user(String id) {
        Map<String, String> attributes = new LinkedHashMap<>();
        attributes.put("tier", "gold");
        attributes.put("region", "north");
        return new Struct(USER)
                .put("id", id)
                .put("name", "Noa " + id)
                .put("address", new Struct(ADDRESS).put("city", "Nazareth").put("zip", "16000"))
                .put("orders", Arrays.asList(
                        new Struct(ORDER).put("sku", "SKU-100").put("qty", 2),
                        new Struct(ORDER).put("sku", "SKU-200").put("qty", 1)))
                .put("attributes", attributes)
                .put("tags", Arrays.asList("vip", "beta"));
    }

    private static SinkRecord record(Object key, Schema schema, Object value, long offset, ConnectHeaders headers) {
        return new SinkRecord("users", 0, key == null ? null : Schema.OPTIONAL_STRING_SCHEMA, key,
                schema, value, offset, null, null, headers);
    }

    private SolrDocumentList query(String q) throws Exception {
        solr.commit(CORE);
        return solr.query(CORE, new SolrQuery(q)).getResults();
    }

    @Test
    void sourceFieldStoresTheNestedOriginalShapeOfAStruct() throws Exception {
        try (SolrWriter w = writer(config(SolrSinkConfig.SOURCE_FIELD_CONFIG, "_source"))) {
            w.write(record("u1", USER, user("u1"), 1, new ConnectHeaders()));
            w.flush();
        }
        SolrDocument doc = query("id:u1").get(0);
        assertThat(doc.getFieldValues("orders.sku")).containsExactly("SKU-100", "SKU-200");
        Object stored = doc.getFirstValue("_source");
        JsonNode source = JSON.readTree(String.valueOf(stored));
        assertThat(source.path("address").path("city").asText()).isEqualTo("Nazareth");
        assertThat(source.path("orders").get(1).path("sku").asText()).isEqualTo("SKU-200");
        assertThat(source.path("orders").get(0).path("qty").asInt()).isEqualTo(2);
        assertThat(source.path("attributes").path("tier").asText()).isEqualTo("gold");
        assertThat(source.path("tags").get(1).asText()).isEqualTo("beta");
    }

    @Test
    void schemalessValueEmbeddingAStructCannotBeSerializedForTheSourceField() throws Exception {
        // An SMT can leave a Connect Struct inside a schemaless map: the doc converts, but the
        // raw-JSON source field cannot serialize it, so the record goes through the malformed policy.
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("id", "m1");
        value.put("profile", user("m1"));
        try (SolrWriter w = writer(config(
                SolrSinkConfig.SOURCE_FIELD_CONFIG, "_source",
                SolrSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, "warn"))) {
            w.write(record("m1", null, value, 1, new ConnectHeaders()));
            w.flush();
            assertThat(w.recordsWritten()).isZero();
        }
        assertThat(query("id:m1")).isEmpty();

        try (SolrWriter w = writer(config(SolrSinkConfig.SOURCE_FIELD_CONFIG, "_source"))) {
            assertThatThrownBy(() -> w.write(record("m1", null, value, 1, new ConnectHeaders())))
                    .isInstanceOf(DataException.class)
                    .hasMessageContaining("source field");
        }
    }

    @Test
    void versionHeadersThatAreEmptyOrNotNumericLeaveTheDocUnversioned() throws Exception {
        try (SolrWriter w = writer(config(SolrSinkConfig.EXTERNAL_VERSION_HEADER_CONFIG, "doc_version"))) {
            ConnectHeaders nullValue = new ConnectHeaders();
            nullValue.addString("doc_version", null);
            ConnectHeaders flag = new ConnectHeaders();
            flag.addBoolean("doc_version", true);
            ConnectHeaders timestamp = new ConnectHeaders();
            timestamp.addTimestamp("doc_version", new java.util.Date(1_700_000_000_000L));
            ConnectHeaders letters = new ConnectHeaders();
            letters.addBytes("doc_version", "12x".getBytes(StandardCharsets.US_ASCII));
            ConnectHeaders innerSpace = new ConnectHeaders();
            innerSpace.addBytes("doc_version", "1 2".getBytes(StandardCharsets.US_ASCII));

            w.write(record("v1", USER, user("v1"), 1, nullValue));
            w.write(record("v2", USER, user("v2"), 2, flag));
            w.write(record("v3", USER, user("v3"), 3, timestamp));
            w.write(record("v4", USER, user("v4"), 4, letters));
            w.write(record("v5", USER, user("v5"), 5, innerSpace));
            w.flush();
            assertThat(w.recordsWritten()).isEqualTo(5);
        }
        // Solr assigned its own versions: none of the headers was forced onto _version_.
        SolrDocumentList docs = query("id:v*");
        assertThat(docs).hasSize(5);
        for (SolrDocument d : docs) {
            assertThat((Long) d.getFirstValue("_version_")).isGreaterThan(1_700_000_000_000L);
        }
        assertThat(SolrWriter.parseAsciiLong("12x".getBytes(StandardCharsets.US_ASCII))).isNull();
        assertThat(SolrWriter.parseAsciiLong("1/2".getBytes(StandardCharsets.US_ASCII))).isNull();
    }

    @Test
    void tombstoneWithANumericKeyDeletesTheDocumentIndexedUnderThatKey() throws Exception {
        try (SolrWriter w = writer(config(SolrSinkConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG, "delete"))) {
            Struct u = user("ignored-by-key");
            w.write(new SinkRecord("users", 0, Schema.INT64_SCHEMA, 4242L, USER, u, 1));
            w.flush();
            assertThat(query("id:4242")).hasSize(1);

            w.write(new SinkRecord("users", 0, Schema.INT64_SCHEMA, 4242L, null, null, 2));
            w.flush();
        }
        assertThat(query("id:4242")).isEmpty();
    }

    @Test
    void keyIgnoreSettingsDecideBetweenTheKafkaKeyAndTheRecordCoordinates() throws Exception {
        try (SolrWriter w = writer(config(SolrSinkConfig.KEY_IGNORE_CONFIG, "true"))) {
            w.write(record("k1", USER, user("k1"), 7, new ConnectHeaders()));
            w.flush();
        }
        assertThat(query("id:users-0-7")).hasSize(1);

        // Per-topic override that names another topic: this topic keeps its Kafka key.
        try (SolrWriter w = writer(config(SolrSinkConfig.TOPIC_KEY_IGNORE_CONFIG, "audit_log"))) {
            w.write(record("k2", USER, user("k2"), 8, new ConnectHeaders()));
            w.flush();
        }
        assertThat(query("id:k2")).hasSize(1);
    }

    @Test
    void schemalessMapsDropNullListItemsAndNeverDuplicateTheId() throws Exception {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("id", "copy-of-id-in-the-value");
        value.put("name", "Dana");
        value.put("tags", new ArrayList<>(Arrays.asList("a", null, "b")));
        try (SolrWriter w = writer(config(SolrSinkConfig.COMPACT_MAP_ENTRIES_CONFIG, "false"))) {
            w.write(record("s1", null, value, 1, new ConnectHeaders()));
            w.flush();
            assertThat(w.recordsWritten()).isEqualTo(1);
        }
        SolrDocument doc = query("id:s1").get(0);
        assertThat(doc.getFieldValues("tags")).containsExactly("a", "b");
        assertThat(doc.getFieldValues("id")).containsExactly("s1");
        assertThat(doc.getFirstValue("name")).isEqualTo("Dana");
    }

    @Test
    void recordFieldIdWithAnEmptyPathIsMalformedAndSkipped() throws Exception {
        try (SolrWriter w = writer(config(
                SolrSinkConfig.ID_STRATEGY_CONFIG, "RECORD_FIELD",
                SolrSinkConfig.ID_FIELD_CONFIG, ".",
                SolrSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, "warn"))) {
            w.write(record("x1", USER, user("x1"), 1, new ConnectHeaders()));
            w.flush();
            assertThat(w.recordsWritten()).isZero();
        }
        assertThat(query("*:*")).isEmpty();

        // The converter's public id derivation on a tombstone has no value to read from.
        SolrRecordConverter converter = new SolrRecordConverter(config(
                SolrSinkConfig.ID_STRATEGY_CONFIG, "RECORD_FIELD"));
        SinkRecord tombstone = record("x1", null, null, 2, new ConnectHeaders());
        assertThatThrownBy(() -> converter.deriveId(tombstone)).isInstanceOf(DataException.class);
    }

    @Test
    void aClientThatFailsToCloseDoesNotFailTheWriterClose() throws Exception {
        List<String> closed = new ArrayList<>();
        // Only close() is overridden: every request still goes to the real core.
        EmbeddedSolrServer failingClose = new EmbeddedSolrServer(solr.getCoreContainer(), CORE) {
            @Override
            public void close() throws IOException {
                super.close();
                closed.add("closed");
                throw new IOException("connection pool already torn down");
            }
        };
        SolrWriter w = new SolrWriter(failingClose, config());
        w.write(record("c1", USER, user("c1"), 1, new ConnectHeaders()));
        w.close(); // final flush, then the failing client close is only logged
        assertThat(closed).containsExactly("closed");
        assertThat(query("id:c1")).hasSize(1);
    }
}
