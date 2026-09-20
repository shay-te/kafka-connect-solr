package com.una.kafka.connect.solr;

import org.apache.kafka.connect.data.ConnectSchema;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Schema auto-evolve against a real, mutable managed schema that has NO catch-all field, so a
 * document is only accepted if the connector really added its fields through the Schema API.
 */
class SolrSchemaEvolveManagedEmbeddedTest {

    private static final String CORE = EmbeddedSolrSupport.CORE;

    private static final Schema ADDRESS = SchemaBuilder.struct().name("address").optional()
            .field("city", Schema.STRING_SCHEMA)
            .build();
    private static final Schema USER_V1 = SchemaBuilder.struct().name("user").version(1)
            .field("id", Schema.STRING_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .field("age", Schema.INT32_SCHEMA)
            .field("score", Schema.FLOAT64_SCHEMA)
            .field("active", Schema.BOOLEAN_SCHEMA)
            .field("joined", Timestamp.builder().build())
            .field("address", ADDRESS)
            .field("tags", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
            .build();
    // A later CDC schema version adds a column.
    private static final Schema USER_V2 = SchemaBuilder.struct().name("user").version(2)
            .field("id", Schema.STRING_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .field("nickname", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    private EmbeddedSolrServer solr;

    @BeforeEach
    void setUp() throws Exception {
        solr = EmbeddedSolrSupport.start("embedded-solr-managed");
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
        p.put(SolrSinkConfig.SCHEMA_AUTO_EVOLVE_CONFIG, "true");
        p.put(SolrSinkConfig.BATCH_SIZE_CONFIG, "100");
        p.put(SolrSinkConfig.LINGER_MS_CONFIG, "600000");
        for (int i = 0; i < kv.length; i += 2) p.put(kv[i], kv[i + 1]);
        return new SolrSinkConfig(p);
    }

    private SolrWriter writer(SolrSinkConfig cfg) {
        return new SolrWriter(new EmbeddedSolrServer(solr.getCoreContainer(), CORE), cfg);
    }

    private Set<String> solrFields() throws Exception {
        return new SchemaRequest.Fields().process(solr, CORE).getFields().stream()
                .map(f -> String.valueOf(f.get("name"))).collect(Collectors.toSet());
    }

    private Map<String, Object> solrField(String name) throws Exception {
        return new SchemaRequest.Field(name).process(solr, CORE).getField();
    }

    private long found(String q) throws Exception {
        solr.commit(CORE);
        return solr.query(CORE, new SolrQuery(q)).getResults().getNumFound();
    }

    private static Struct userV1(String id, int age) {
        return new Struct(USER_V1)
                .put("id", id)
                .put("name", "Yael " + id)
                .put("age", age)
                .put("score", 4.5)
                .put("active", true)
                .put("joined", new java.util.Date(1_690_000_000_000L))
                .put("address", new Struct(ADDRESS).put("city", "Beersheba"))
                .put("tags", Arrays.asList("early", "adopter"));
    }

    @Test
    void newFieldsAreAddedWithSolrTypesAndTheDocumentIsAccepted() throws Exception {
        try (SolrWriter w = writer(config())) {
            w.write(new SinkRecord("users", 0, Schema.STRING_SCHEMA, "e1", USER_V1, userV1("e1", 34), 1));
            w.flush();
            assertThat(w.recordsWritten()).isEqualTo(1);
        }
        assertThat(solrField("age").get("type")).isEqualTo("pint");
        assertThat(solrField("score").get("type")).isEqualTo("pdouble");
        assertThat(solrField("active").get("type")).isEqualTo("boolean");
        assertThat(solrField("joined").get("type")).isEqualTo("pdate");
        assertThat(solrField("address.city").get("type")).isEqualTo("string");
        assertThat(solrField("tags").get("multiValued")).isEqualTo(true);
        assertThat(found("age:34 AND active:true AND address.city:Beersheba")).isEqualTo(1);
    }

    @Test
    void aPerTopicIgnoreListNamingAnotherTopicStillEvolvesThisOne() throws Exception {
        try (SolrWriter w = writer(config(SolrSinkConfig.TOPIC_SCHEMA_IGNORE_CONFIG, "audit_log"))) {
            w.write(new SinkRecord("users", 0, Schema.STRING_SCHEMA, "e2", USER_V1, userV1("e2", 41), 1));
            Struct v2 = new Struct(USER_V2).put("id", "e3").put("name", "Omer").put("nickname", "omi");
            w.write(new SinkRecord("users", 0, Schema.STRING_SCHEMA, "e3", USER_V2, v2, 2));
            // v1 again: already evolved for this collection, no Schema API round trip.
            w.write(new SinkRecord("users", 0, Schema.STRING_SCHEMA, "e4", USER_V1, userV1("e4", 29), 3));
            w.flush();
            assertThat(w.recordsWritten()).isEqualTo(3);
        }
        assertThat(solrFields()).contains("nickname", "age");
        assertThat(found("nickname:omi")).isEqualTo(1);
        assertThat(found("age:[20 TO 50]")).isEqualTo(2);
    }

    @Test
    void aTopicOfPlainStringsHasNothingToEvolveAndIsSkippedAsMalformed() throws Exception {
        try (SolrWriter w = writer(config(SolrSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, "warn"))) {
            w.write(new SinkRecord("users", 0, Schema.STRING_SCHEMA, "raw", Schema.STRING_SCHEMA,
                    "not a document", 1));
            w.flush();
            assertThat(w.recordsWritten()).isZero();
        }
        assertThat(solrFields()).containsExactlyInAnyOrder("id", "_version_");
    }

    @Test
    void aSchemaTheConverterCannotTypeStopsEvolutionForThatRecordOnly() throws Exception {
        // A third-party converter handed over a field schema without a type: fields before it are
        // added, the rest are not, and the failure is logged instead of killing the task.
        Schema broken = SchemaBuilder.struct().name("broken")
                .field("before_field", Schema.STRING_SCHEMA)
                .field("untyped", new ConnectSchema(null))
                .field("after_field", Schema.STRING_SCHEMA)
                .build();
        SolrSchemaManager manager = new SolrSchemaManager(solr, config());
        manager.evolveIfNeeded(CORE, broken);
        assertThat(solrFields()).contains("before_field").doesNotContain("after_field", "untyped");
    }
}
