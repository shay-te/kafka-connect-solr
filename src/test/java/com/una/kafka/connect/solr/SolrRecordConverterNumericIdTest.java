package com.una.kafka.connect.solr;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the {@code id.coerce.to.string} flag: default behaviour is unchanged (numeric
 * keys arrive at Solr as Strings), but with the flag off the raw Long / Integer is
 * passed through so SolrJ can encode it as a primitive in {@code javabin}.
 *
 * <p>Requirement: when the flag is off, the Solr collection's id field schema must be a
 * numeric type (plong / pint / pdouble); otherwise Solr rejects the doc at index time.</p>
 */
class SolrRecordConverterNumericIdTest {

    private SolrSinkConfig cfg(Map<String, String> overrides) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "c");
        p.putAll(overrides);
        return new SolrSinkConfig(p);
    }

    private SinkRecord recWithKey(Schema keySchema, Object key, Schema valueSchema, Struct value) {
        return new SinkRecord("c", 0, keySchema, key, valueSchema, value, 1L);
    }

    @Test
    void defaultCoerceTrueKafkaKeyLongBecomesString() {
        // Default config: id.coerce.to.string=true (backward compatible).
        SolrRecordConverter converter = new SolrRecordConverter(cfg(new HashMap<>()));
        Schema valueSchema = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        Struct value = new Struct(valueSchema).put("x", "y");
        SinkRecord record = recWithKey(Schema.INT64_SCHEMA, 12345L, valueSchema, value);
        SolrInputDocument doc = converter.convert(record);
        assertThat(doc.getFieldValue("id"))
                .isInstanceOf(String.class)
                .isEqualTo("12345");
    }

    @Test
    void coerceFalseKafkaKeyLongPassesThrough() {
        Map<String, String> overrides = new HashMap<>();
        overrides.put(SolrSinkConfig.ID_COERCE_TO_STRING_CONFIG, "false");
        SolrRecordConverter converter = new SolrRecordConverter(cfg(overrides));
        Schema valueSchema = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        Struct value = new Struct(valueSchema).put("x", "y");
        SinkRecord record = recWithKey(Schema.INT64_SCHEMA, 12345L, valueSchema, value);
        SolrInputDocument doc = converter.convert(record);
        // No allocation of "12345" — the raw Long lands in the doc.
        assertThat(doc.getFieldValue("id"))
                .isInstanceOf(Long.class)
                .isEqualTo(12345L);
    }

    @Test
    void coerceFalseKafkaKeyIntegerPassesThrough() {
        Map<String, String> overrides = new HashMap<>();
        overrides.put(SolrSinkConfig.ID_COERCE_TO_STRING_CONFIG, "false");
        SolrRecordConverter converter = new SolrRecordConverter(cfg(overrides));
        Schema valueSchema = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        Struct value = new Struct(valueSchema).put("x", "y");
        SinkRecord record = recWithKey(Schema.INT32_SCHEMA, 42, valueSchema, value);
        SolrInputDocument doc = converter.convert(record);
        assertThat(doc.getFieldValue("id"))
                .isInstanceOf(Integer.class)
                .isEqualTo(42);
    }

    @Test
    void coerceFalseStringKeyStillArrivesAsString() {
        // Flag only affects numeric keys; String keys remain String.
        Map<String, String> overrides = new HashMap<>();
        overrides.put(SolrSinkConfig.ID_COERCE_TO_STRING_CONFIG, "false");
        SolrRecordConverter converter = new SolrRecordConverter(cfg(overrides));
        Schema valueSchema = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        Struct value = new Struct(valueSchema).put("x", "y");
        SinkRecord record = recWithKey(Schema.STRING_SCHEMA, "user-abc", valueSchema, value);
        SolrInputDocument doc = converter.convert(record);
        assertThat(doc.getFieldValue("id"))
                .isInstanceOf(String.class)
                .isEqualTo("user-abc");
    }

    @Test
    void coerceFalseRecordFieldLongPassesThrough() {
        Map<String, String> overrides = new HashMap<>();
        overrides.put(SolrSinkConfig.ID_COERCE_TO_STRING_CONFIG, "false");
        overrides.put(SolrSinkConfig.ID_STRATEGY_CONFIG, "RECORD_FIELD");
        overrides.put(SolrSinkConfig.ID_FIELD_CONFIG, "uid");
        SolrRecordConverter converter = new SolrRecordConverter(cfg(overrides));
        Schema valueSchema = SchemaBuilder.struct().field("uid", Schema.INT64_SCHEMA).build();
        Struct value = new Struct(valueSchema).put("uid", 999L);
        SinkRecord record = new SinkRecord("c", 0, Schema.STRING_SCHEMA, "k", valueSchema, value, 1L);
        SolrInputDocument doc = converter.convert(record);
        assertThat(doc.getFieldValue("id"))
                .isInstanceOf(Long.class)
                .isEqualTo(999L);
    }

    @Test
    void coerceFalseRecordFieldMapValueLongPassesThrough() {
        // Schemaless: extractField pulls from a Map; numeric value must still pass through.
        Map<String, String> overrides = new HashMap<>();
        overrides.put(SolrSinkConfig.ID_COERCE_TO_STRING_CONFIG, "false");
        overrides.put(SolrSinkConfig.ID_STRATEGY_CONFIG, "RECORD_FIELD");
        overrides.put(SolrSinkConfig.ID_FIELD_CONFIG, "uid");
        SolrRecordConverter converter = new SolrRecordConverter(cfg(overrides));
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("uid", 7777L);
        value.put("other", "v");
        SinkRecord record = new SinkRecord("c", 0, Schema.STRING_SCHEMA, "k", null, value, 1L);
        SolrInputDocument doc = converter.convert(record);
        assertThat(doc.getFieldValue("id"))
                .isInstanceOf(Long.class)
                .isEqualTo(7777L);
    }

    @Test
    void coerceFalseTpoStrategyStillProducesString() {
        // TOPIC_PARTITION_OFFSET concatenates topic-partition-offset into a String regardless
        // of the flag — there is no numeric form of "topic-3-99".
        Map<String, String> overrides = new HashMap<>();
        overrides.put(SolrSinkConfig.ID_COERCE_TO_STRING_CONFIG, "false");
        overrides.put(SolrSinkConfig.ID_STRATEGY_CONFIG, "TOPIC_PARTITION_OFFSET");
        SolrRecordConverter converter = new SolrRecordConverter(cfg(overrides));
        Schema valueSchema = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        Struct value = new Struct(valueSchema).put("x", "y");
        SinkRecord record = new SinkRecord("topic-a", 3, null, null, valueSchema, value, 99L);
        SolrInputDocument doc = converter.convert(record);
        assertThat(doc.getFieldValue("id"))
                .isInstanceOf(String.class)
                .isEqualTo("topic-a-3-99");
    }

    @Test
    void coerceFalseUuidStrategyStillProducesString() {
        Map<String, String> overrides = new HashMap<>();
        overrides.put(SolrSinkConfig.ID_COERCE_TO_STRING_CONFIG, "false");
        overrides.put(SolrSinkConfig.ID_STRATEGY_CONFIG, "UUID");
        SolrRecordConverter converter = new SolrRecordConverter(cfg(overrides));
        Schema valueSchema = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        Struct value = new Struct(valueSchema).put("x", "y");
        SinkRecord record = new SinkRecord("c", 0, Schema.STRING_SCHEMA, "k", valueSchema, value, 1L);
        SolrInputDocument doc = converter.convert(record);
        assertThat(doc.getFieldValue("id")).isInstanceOf(String.class);
    }
}
