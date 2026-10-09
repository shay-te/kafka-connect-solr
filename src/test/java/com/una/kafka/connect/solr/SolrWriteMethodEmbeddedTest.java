package com.una.kafka.connect.solr;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What each write.method does to a document Solr already holds: INDEX replaces it, UPSERT sets only the
 * fields the record carries and creates a missing document, ATOMIC_UPDATE does so on an existing one only.
 */
class SolrWriteMethodEmbeddedTest {

    private static final Schema USER = SchemaBuilder.struct()
            .field("name", Schema.STRING_SCHEMA).field("city", Schema.STRING_SCHEMA).build();
    private static final Schema NAME_ONLY = SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA).build();
    private static final Schema ADDRESS = SchemaBuilder.struct().optional()
            .field("city", Schema.OPTIONAL_STRING_SCHEMA).field("zip", Schema.OPTIONAL_STRING_SCHEMA).build();
    private static final Schema TAG = SchemaBuilder.struct().field("label", Schema.OPTIONAL_STRING_SCHEMA).build();
    private static final Schema RESIDENT = SchemaBuilder.struct()
            .field("name", Schema.OPTIONAL_STRING_SCHEMA).field("address", ADDRESS)
            .field("tags", SchemaBuilder.array(TAG).optional().build()).build();

    private EmbeddedSolrServer solr;

    @BeforeEach
    void startSolr() throws Exception {
        solr = EmbeddedSolrSupport.start();
    }

    @AfterEach
    void stopSolr() {
        EmbeddedSolrSupport.stop(solr);
    }

    /** Converts and sends one record the way the sink does. */
    private void write(String writeMethod, String id, Struct value) {
        write(writeMethod, id, value.schema(), value);
    }

    private static SolrSinkConfig config(String writeMethod) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://embedded/solr");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, EmbeddedSolrSupport.CORE);
        p.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "UNUSED");
        p.put(SolrSinkConfig.WRITE_METHOD_CONFIG, writeMethod);
        p.put(SolrSinkConfig.MAX_RETRIES_CONFIG, "0");
        return new SolrSinkConfig(p);
    }

    /** Sends one record through the sink's writer, which decides whether anything reaches Solr. */
    private void writeThroughSink(String writeMethod, String id, Schema schema, Object value) {
        try (SolrWriter writer = new SolrWriter(
                new EmbeddedSolrServer(solr.getCoreContainer(), EmbeddedSolrSupport.CORE), config(writeMethod))) {
            writer.write(new SinkRecord("users", 0, Schema.STRING_SCHEMA, id, schema, value, 1L));
            writer.flush();
        }
    }

    private void write(String writeMethod, String id, Schema schema, Object value) {
        SolrSinkConfig config = config(writeMethod);
        SinkRecord record = new SinkRecord("users", 0, Schema.STRING_SCHEMA, id, schema, value, 1L);
        try (SolrBulkProcessor bulk = new SolrBulkProcessor(solr, config)) {
            bulk.upsert(EmbeddedSolrSupport.CORE, new SolrRecordConverter(config).convert(record),
                    new OffsetState(new TopicPartition("users", 0), 1L));
            bulk.flushSync();
        }
    }

    private SolrDocument stored(String id) throws Exception {
        solr.commit(EmbeddedSolrSupport.CORE);
        SolrDocumentList found = solr.query(EmbeddedSolrSupport.CORE, new SolrQuery("id:" + id)).getResults();
        return found.isEmpty() ? null : found.get(0);
    }

    private static Struct user(String name, String city) {
        return new Struct(USER).put("name", name).put("city", city);
    }

    private static Struct renamed(String name) {
        return new Struct(NAME_ONLY).put("name", name);
    }

    private static Struct resident(String name, String city, String zip) {
        Struct address = city == null && zip == null ? null : new Struct(ADDRESS).put("city", city).put("zip", zip);
        return new Struct(RESIDENT).put("name", name).put("address", address);
    }

    @Test
    void indexReplacesTheWholeDocument() throws Exception {
        write("INDEX", "u1", user("Ada", "London"));
        write("INDEX", "u1", renamed("Grace"));

        assertThat(stored("u1").getFirstValue("name")).isEqualTo("Grace");
        assertThat(stored("u1").getFieldValue("city")).isNull();
    }

    @Test
    void upsertKeepsTheFieldsTheRecordDoesNotCarry() throws Exception {
        write("INDEX", "u2", user("Ada", "London"));
        write("UPSERT", "u2", renamed("Grace"));

        assertThat(stored("u2").getFirstValue("name")).isEqualTo("Grace");
        assertThat(stored("u2").getFirstValue("city")).isEqualTo("London");
    }

    @Test
    void upsertCreatesAMissingDocument() throws Exception {
        write("UPSERT", "u3", renamed("Grace"));

        assertThat(stored("u3").getFirstValue("name")).isEqualTo("Grace");
    }

    @Test
    void atomicUpdateChangesAnExistingDocumentAndNeverCreatesOne() throws Exception {
        write("INDEX", "u4", user("Ada", "London"));
        write("ATOMIC_UPDATE", "u4", renamed("Grace"));

        assertThat(stored("u4").getFirstValue("name")).isEqualTo("Grace");
        assertThat(stored("u4").getFirstValue("city")).isEqualTo("London");
        assertThatThrownBy(() -> write("ATOMIC_UPDATE", "u5", renamed("Grace")))
                .isInstanceOf(ConnectException.class).hasMessageContaining("Solr rejected a document");
        assertThat((Object) stored("u5")).as("a record for a missing document creates nothing").isNull();
    }

    @Test
    void upsertAndAtomicUpdateRemoveTheFieldsARecordCarriesAsNull() throws Exception {
        for (String writeMethod : new String[] {"UPSERT", "ATOMIC_UPDATE"}) {
            String id = "n-" + writeMethod;
            write("INDEX", id, resident("Ada", "London", "N1"));

            write(writeMethod, id, resident(null, "Paris", null));
            assertThat(stored(id).getFieldNames()).as(writeMethod).doesNotContain("name", "address.zip");
            assertThat(stored(id).getFirstValue("address.city")).as(writeMethod).isEqualTo("Paris");

            write(writeMethod, id, resident("Grace", null, null));
            assertThat(stored(id).getFieldNames()).as(writeMethod + ": a null struct clears its fields")
                    .doesNotContain("address.city", "address.zip");
            assertThat(stored(id).getFirstValue("name")).as(writeMethod).isEqualTo("Grace");
        }
    }

    @Test
    void upsertRemovesASchemalessFieldTheRecordCarriesAsNull() throws Exception {
        write("INDEX", "s1", user("Ada", "London"));
        Map<String, Object> value = new HashMap<>();
        value.put("name", "Grace");
        value.put("city", null);

        write("UPSERT", "s1", null, value);

        assertThat(stored("s1").getFieldNames()).doesNotContain("city");
        assertThat(stored("s1").getFirstValue("name")).isEqualTo("Grace");
    }

    @Test
    void aRecordThatSetsNoFieldLeavesTheStoredDocumentAlone() throws Exception {
        for (String writeMethod : new String[] {"UPSERT", "ATOMIC_UPDATE"}) {
            String id = "e-" + writeMethod;
            write("INDEX", id, user("Ada", "London"));

            // Only the id: Solr reads a document without an atomic op as a full replacement.
            writeThroughSink(writeMethod, id, null, Map.of("id", id));

            assertThat(stored(id).getFirstValue("city")).as(writeMethod).isEqualTo("London");
        }
    }

    @Test
    void aNullInsideAnArrayElementKeepsTheValuesOtherElementsCarry() throws Exception {
        Struct value = resident("Ada", null, null).put("tags", java.util.List.of(
                new Struct(TAG).put("label", null), new Struct(TAG).put("label", "vip"), new Struct(TAG).put("label", null)));

        write("UPSERT", "a1", value);

        assertThat(stored("a1").getFieldValues("tags.label")).containsExactly("vip");
    }
}
