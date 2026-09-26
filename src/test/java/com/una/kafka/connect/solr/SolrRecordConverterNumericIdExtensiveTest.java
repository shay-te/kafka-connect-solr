package com.una.kafka.connect.solr;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Extensive single-threaded coverage of the {@code id.coerce.to.string} flag —
 * exhausts the Number subclasses, byte[] keys, null guards, interaction with
 * other id-related config, atomic update wrapping, and mapping version stamping.
 *
 * <p>Multi-threaded coverage lives in {@link SolrRecordConverterConcurrencyTest}.</p>
 */
class SolrRecordConverterNumericIdExtensiveTest {

    private SolrSinkConfig coerceFalse() {
        Map<String, String> overrides = new HashMap<>();
        overrides.put(SolrSinkConfig.ID_COERCE_TO_STRING_CONFIG, "false");
        return cfg(overrides);
    }

    private SolrSinkConfig cfg(Map<String, String> overrides) {
        Map<String, String> props = new HashMap<>();
        props.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        props.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "c");
        props.putAll(overrides);
        return new SolrSinkConfig(props);
    }

    private SinkRecord recordWithKey(Schema keySchema, Object key) {
        Schema valueSchema = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        Struct value = new Struct(valueSchema).put("x", "y");
        return new SinkRecord("c", 0, keySchema, key, valueSchema, value, 1L);
    }

    // --- Every Number subtype ---

    @Test
    void longKeyPassesThroughAsLong() {
        SolrRecordConverter converter = new SolrRecordConverter(coerceFalse());
        SolrInputDocument doc = converter.convert(recordWithKey(Schema.INT64_SCHEMA, 9_999_999_999L));
        assertThat(doc.getFieldValue("id")).isEqualTo(9_999_999_999L).isInstanceOf(Long.class);
    }

    @Test
    void integerKeyPassesThroughAsInteger() {
        SolrRecordConverter converter = new SolrRecordConverter(coerceFalse());
        SolrInputDocument doc = converter.convert(recordWithKey(Schema.INT32_SCHEMA, 2_000_000_000));
        assertThat(doc.getFieldValue("id")).isEqualTo(2_000_000_000).isInstanceOf(Integer.class);
    }

    @Test
    void shortKeyPassesThroughAsShort() {
        SolrRecordConverter converter = new SolrRecordConverter(coerceFalse());
        SolrInputDocument doc = converter.convert(recordWithKey(Schema.INT16_SCHEMA, (short) 32_000));
        assertThat(doc.getFieldValue("id")).isEqualTo((short) 32_000).isInstanceOf(Short.class);
    }

    @Test
    void byteKeyPassesThroughAsByte() {
        SolrRecordConverter converter = new SolrRecordConverter(coerceFalse());
        SolrInputDocument doc = converter.convert(recordWithKey(Schema.INT8_SCHEMA, (byte) 127));
        assertThat(doc.getFieldValue("id")).isEqualTo((byte) 127).isInstanceOf(Byte.class);
    }

    @Test
    void floatKeyPassesThroughAsFloat() {
        SolrRecordConverter converter = new SolrRecordConverter(coerceFalse());
        SolrInputDocument doc = converter.convert(recordWithKey(Schema.FLOAT32_SCHEMA, 3.14f));
        assertThat(doc.getFieldValue("id")).isEqualTo(3.14f).isInstanceOf(Float.class);
    }

    @Test
    void doubleKeyPassesThroughAsDouble() {
        SolrRecordConverter converter = new SolrRecordConverter(coerceFalse());
        SolrInputDocument doc = converter.convert(recordWithKey(Schema.FLOAT64_SCHEMA, 2.718281828));
        assertThat(doc.getFieldValue("id")).isEqualTo(2.718281828).isInstanceOf(Double.class);
    }

    @Test
    void bigDecimalKeyPassesThroughUnchanged() {
        // Solr typically can't accept BigDecimal in a plong field, but the converter must
        // not crash and must not lose precision by silently stringifying.
        SolrRecordConverter converter = new SolrRecordConverter(coerceFalse());
        BigDecimal key = new BigDecimal("12345.6789");
        SolrInputDocument doc = converter.convert(recordWithKey(Schema.OPTIONAL_BYTES_SCHEMA, key));
        assertThat(doc.getFieldValue("id")).isEqualTo(key).isInstanceOf(BigDecimal.class);
    }

    @Test
    void bigIntegerKeyPassesThroughUnchanged() {
        SolrRecordConverter converter = new SolrRecordConverter(coerceFalse());
        BigInteger key = new BigInteger("123456789012345678901234567890");
        SolrInputDocument doc = converter.convert(recordWithKey(Schema.OPTIONAL_BYTES_SCHEMA, key));
        assertThat(doc.getFieldValue("id")).isEqualTo(key).isInstanceOf(BigInteger.class);
    }

    @Test
    void byteArrayKeyPassesThroughUnchanged() {
        // byte[] keys with coerce=false flow through as-is; SolrJ encodes as binary.
        // (User must configure Solr to accept binary ids or this fails at index time.)
        SolrRecordConverter converter = new SolrRecordConverter(coerceFalse());
        byte[] key = new byte[]{1, 2, 3};
        SolrInputDocument doc = converter.convert(recordWithKey(Schema.BYTES_SCHEMA, key));
        assertThat(doc.getFieldValue("id")).isSameAs(key);
    }

    // --- Null and zero values ---

    @Test
    void nullKeyStillThrowsRegardlessOfFlag() {
        SolrRecordConverter converter = new SolrRecordConverter(coerceFalse());
        Schema valueSchema = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        Struct value = new Struct(valueSchema).put("x", "y");
        SinkRecord record = new SinkRecord("c", 0, Schema.OPTIONAL_INT64_SCHEMA, null, valueSchema, value, 1L);
        assertThatThrownBy(() -> converter.convert(record))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("KAFKA_KEY but record key is null");
    }

    @Test
    void zeroLongKeyPassesThroughCorrectly() {
        // Defensive: 0 shouldn't be confused with null.
        SolrRecordConverter converter = new SolrRecordConverter(coerceFalse());
        SolrInputDocument doc = converter.convert(recordWithKey(Schema.INT64_SCHEMA, 0L));
        assertThat(doc.getFieldValue("id")).isEqualTo(0L);
    }

    @Test
    void negativeLongKeyPassesThroughCorrectly() {
        SolrRecordConverter converter = new SolrRecordConverter(coerceFalse());
        SolrInputDocument doc = converter.convert(recordWithKey(Schema.INT64_SCHEMA, -987654321L));
        assertThat(doc.getFieldValue("id")).isEqualTo(-987654321L);
    }

    @Test
    void minMaxLongBoundariesPassThroughCorrectly() {
        SolrRecordConverter converter = new SolrRecordConverter(coerceFalse());
        SolrInputDocument minDoc = converter.convert(recordWithKey(Schema.INT64_SCHEMA, Long.MIN_VALUE));
        assertThat(minDoc.getFieldValue("id")).isEqualTo(Long.MIN_VALUE);
        SolrInputDocument maxDoc = converter.convert(recordWithKey(Schema.INT64_SCHEMA, Long.MAX_VALUE));
        assertThat(maxDoc.getFieldValue("id")).isEqualTo(Long.MAX_VALUE);
    }

    // --- Existing-behavior preservation ---

    @Test
    void coerceTrueLongKeyBecomesStringRepresentation() {
        SolrRecordConverter converter = new SolrRecordConverter(cfg(new HashMap<>()));
        SolrInputDocument doc = converter.convert(recordWithKey(Schema.INT64_SCHEMA, 42L));
        assertThat(doc.getFieldValue("id")).isEqualTo("42").isInstanceOf(String.class);
    }

    @Test
    void coerceTrueStringKeyArrivesUnchanged() {
        SolrRecordConverter converter = new SolrRecordConverter(cfg(new HashMap<>()));
        SolrInputDocument doc = converter.convert(recordWithKey(Schema.STRING_SCHEMA, "user-42"));
        assertThat(doc.getFieldValue("id")).isEqualTo("user-42");
    }

    // --- Interaction with key.ignore, ATOMIC_UPDATE, mapping version ---

    @Test
    void keyIgnoreDowngradesToTpoEvenWhenCoerceFalse() {
        // KAFKA_KEY + keyIgnore=true downgrades to TOPIC_PARTITION_OFFSET which is always String.
        Map<String, String> overrides = new HashMap<>();
        overrides.put(SolrSinkConfig.ID_COERCE_TO_STRING_CONFIG, "false");
        overrides.put(SolrSinkConfig.KEY_IGNORE_CONFIG, "true");
        SolrRecordConverter converter = new SolrRecordConverter(cfg(overrides));
        Schema valueSchema = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        Struct value = new Struct(valueSchema).put("x", "y");
        SinkRecord record = new SinkRecord("topic-a", 3, Schema.INT64_SCHEMA, 99L, valueSchema, value, 7L);
        SolrInputDocument doc = converter.convert(record);
        assertThat(doc.getFieldValue("id")).isEqualTo("topic-a-3-7").isInstanceOf(String.class);
    }

    @Test
    void atomicUpdateWithNumericIdLeavesIdUnwrappedAndNumeric() {
        // Atomic-update wraps every non-id field in {"set": value}. The id itself must NOT
        // be wrapped, AND must stay as the raw numeric type.
        Map<String, String> overrides = new HashMap<>();
        overrides.put(SolrSinkConfig.ID_COERCE_TO_STRING_CONFIG, "false");
        overrides.put(SolrSinkConfig.WRITE_METHOD_CONFIG, "ATOMIC_UPDATE");
        SolrRecordConverter converter = new SolrRecordConverter(cfg(overrides));
        Schema valueSchema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .build();
        Struct value = new Struct(valueSchema).put("name", "Ada").put("age", 36);
        SinkRecord record = new SinkRecord("c", 0, Schema.INT64_SCHEMA, 555L, valueSchema, value, 1L);
        SolrInputDocument doc = converter.convert(record);

        assertThat(doc.getFieldValue("id")).isEqualTo(555L).isInstanceOf(Long.class);
        assertThat(doc.getFieldValue("name")).isInstanceOf(Map.class);
        assertThat(((Map<?, ?>) doc.getFieldValue("name")).get("set")).isEqualTo("Ada");
        assertThat(((Map<?, ?>) doc.getFieldValue("age")).get("set")).isEqualTo(36);
    }

    @Test
    void mappingVersionStillStringWhenCoerceFalse() {
        Map<String, String> overrides = new HashMap<>();
        overrides.put(SolrSinkConfig.ID_COERCE_TO_STRING_CONFIG, "false");
        overrides.put(SolrSinkConfig.MAPPING_VERSION_CONFIG, "v3");
        SolrRecordConverter converter = new SolrRecordConverter(cfg(overrides));
        SolrInputDocument doc = converter.convert(recordWithKey(Schema.INT64_SCHEMA, 42L));
        assertThat(doc.getFieldValue("id")).isEqualTo(42L);
        assertThat(doc.getFieldValue("_mapping_version")).isEqualTo("v3");
    }

    // --- RECORD_FIELD strategy edge cases ---

    @Test
    void recordFieldDeeplyNestedNumericPassesThrough() {
        Map<String, String> overrides = new HashMap<>();
        overrides.put(SolrSinkConfig.ID_COERCE_TO_STRING_CONFIG, "false");
        overrides.put(SolrSinkConfig.ID_STRATEGY_CONFIG, "RECORD_FIELD");
        overrides.put(SolrSinkConfig.ID_FIELD_CONFIG, "outer.inner");
        SolrRecordConverter converter = new SolrRecordConverter(cfg(overrides));
        Schema innerSchema = SchemaBuilder.struct().field("inner", Schema.INT64_SCHEMA).build();
        Schema outerSchema = SchemaBuilder.struct().field("outer", innerSchema).build();
        Struct innerValue = new Struct(innerSchema).put("inner", 12345L);
        Struct outerValue = new Struct(outerSchema).put("outer", innerValue);
        SinkRecord record = new SinkRecord("c", 0, Schema.STRING_SCHEMA, "k", outerSchema, outerValue, 1L);
        SolrInputDocument doc = converter.convert(record);
        assertThat(doc.getFieldValue("id")).isEqualTo(12345L).isInstanceOf(Long.class);
    }

    @Test
    void recordFieldNullValueStillThrowsWithCoerceFalse() {
        Map<String, String> overrides = new HashMap<>();
        overrides.put(SolrSinkConfig.ID_COERCE_TO_STRING_CONFIG, "false");
        overrides.put(SolrSinkConfig.ID_STRATEGY_CONFIG, "RECORD_FIELD");
        overrides.put(SolrSinkConfig.ID_FIELD_CONFIG, "uid");
        SolrRecordConverter converter = new SolrRecordConverter(cfg(overrides));
        Schema valueSchema = SchemaBuilder.struct()
                .field("uid", SchemaBuilder.int64().optional().build())
                .build();
        Struct value = new Struct(valueSchema); // uid not set, defaults to null
        SinkRecord record = new SinkRecord("c", 0, Schema.STRING_SCHEMA, "k", valueSchema, value, 1L);
        assertThatThrownBy(() -> converter.convert(record))
                .isInstanceOf(DataException.class)
                .hasMessageContaining("'uid' is null");
    }

    @Test
    void schemalessTopLevelMapWithNumericIdPassesThrough() {
        Map<String, String> overrides = new HashMap<>();
        overrides.put(SolrSinkConfig.ID_COERCE_TO_STRING_CONFIG, "false");
        overrides.put(SolrSinkConfig.ID_STRATEGY_CONFIG, "RECORD_FIELD");
        overrides.put(SolrSinkConfig.ID_FIELD_CONFIG, "pk");
        SolrRecordConverter converter = new SolrRecordConverter(cfg(overrides));
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("pk", 98765L);
        value.put("name", "Alice");
        SinkRecord record = new SinkRecord("c", 0, Schema.STRING_SCHEMA, "k", null, value, 1L);
        SolrInputDocument doc = converter.convert(record);
        assertThat(doc.getFieldValue("id")).isEqualTo(98765L).isInstanceOf(Long.class);
    }

    // --- Repeated convert calls keep the per-record id correct ---

    @Test
    void repeatedConvertsKeepEachIdDistinct() {
        // Defensive: confirm there's no cross-record bleed via the planCache or ThreadLocals.
        SolrRecordConverter converter = new SolrRecordConverter(coerceFalse());
        for (long offset = 0; offset < 1_000; offset++) {
            SolrInputDocument doc = converter.convert(recordWithKey(Schema.INT64_SCHEMA, offset));
            assertThat(doc.getFieldValue("id")).isEqualTo(offset);
        }
    }
}
