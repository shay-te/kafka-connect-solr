package com.una.kafka.connect.solr;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Coverage tests for SolrRecordConverter branches not exercised by the
 * functional / parity tests. Hits the addScalar dispatch table, populateFromMap
 * prefix variations, and the encoderFor BYTES + STRUCT + Decimal paths.
 */
class SolrRecordConverterCoverageTest {

    private SolrSinkConfig cfg(Map<String, String> extra) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "c");
        p.putAll(extra);
        return new SolrSinkConfig(p);
    }

    private SinkRecord rec(Schema s, Struct v) {
        return new SinkRecord("c", 0, Schema.STRING_SCHEMA, "k", s, v, 1L);
    }

    @Test
    void deriveIdUuidProducesUuidFormat() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.ID_STRATEGY_CONFIG, "UUID");
        SolrRecordConverter c = new SolrRecordConverter(cfg(o));
        Schema s = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        Struct v = new Struct(s).put("x", "y");
        SolrInputDocument doc = c.convert(rec(s, v));
        String id = (String) doc.getFieldValue("id");
        // UUID v4 string: 36 chars including dashes.
        assertThat(id).hasSize(36).matches("[0-9a-f-]{36}");
    }

    @Test
    void deriveIdRecordFieldNonStringConvertsToString() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.ID_STRATEGY_CONFIG, "RECORD_FIELD");
        o.put(SolrSinkConfig.ID_FIELD_CONFIG, "uid");
        SolrRecordConverter c = new SolrRecordConverter(cfg(o));
        Schema s = SchemaBuilder.struct().field("uid", Schema.INT32_SCHEMA).build();
        Struct v = new Struct(s).put("uid", 42);
        SolrInputDocument doc = c.convert(rec(s, v));
        assertThat(doc.getFieldValue("id")).isEqualTo("42");
    }

    @Test
    void deriveIdRecordFieldNullThrows() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.ID_STRATEGY_CONFIG, "RECORD_FIELD");
        o.put(SolrSinkConfig.ID_FIELD_CONFIG, "uid");
        SolrRecordConverter c = new SolrRecordConverter(cfg(o));
        Schema s = SchemaBuilder.struct().field("uid", SchemaBuilder.string().optional().build()).build();
        Struct v = new Struct(s);
        assertThatThrownBy(() -> c.convert(rec(s, v))).isInstanceOf(DataException.class);
    }

    @Test
    void deriveIdKafkaKeyNullThrows() {
        SolrRecordConverter c = new SolrRecordConverter(cfg(new HashMap<>()));
        Schema s = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        Struct v = new Struct(s).put("x", "y");
        SinkRecord r = new SinkRecord("c", 0, Schema.STRING_SCHEMA, null, s, v, 1L);
        assertThatThrownBy(() -> c.convert(r)).isInstanceOf(DataException.class);
    }

    @Test
    void deriveIdKafkaKeyNonStringConvertsToString() {
        SolrRecordConverter c = new SolrRecordConverter(cfg(new HashMap<>()));
        Schema s = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        Struct v = new Struct(s).put("x", "y");
        SinkRecord r = new SinkRecord("c", 0, Schema.INT64_SCHEMA, 7L, s, v, 1L);
        SolrInputDocument doc = c.convert(r);
        assertThat(doc.getFieldValue("id")).isEqualTo("7");
    }

    @Test
    void topLevelMapValueWithCompactMapEntries() {
        // Default compactMapEntries=true; top-level Map should dotted-flatten.
        SolrRecordConverter c = new SolrRecordConverter(cfg(new HashMap<>()));
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("a", "1");
        m.put("b", 2);
        SinkRecord r = new SinkRecord("c", 0, Schema.STRING_SCHEMA, "k", null, m, 1L);
        SolrInputDocument doc = c.convert(r);
        assertThat(doc.getFieldValue("a")).isEqualTo("1");
        assertThat(doc.getFieldValue("b")).isEqualTo(2);
    }

    @Test
    void topLevelMapValueWithCompactDisabled() {
        // Non-compact + top-level Map = same dotted-flatten (no prefix to wrap).
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.COMPACT_MAP_ENTRIES_CONFIG, "false");
        SolrRecordConverter c = new SolrRecordConverter(cfg(o));
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("a", "1");
        SinkRecord r = new SinkRecord("c", 0, Schema.STRING_SCHEMA, "k", null, m, 1L);
        SolrInputDocument doc = c.convert(r);
        assertThat(doc.getFieldValue("a")).isEqualTo("1");
    }

    @Test
    void nestedMapWithCompactDisabledProducesKeyValueEntries() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.COMPACT_MAP_ENTRIES_CONFIG, "false");
        SolrRecordConverter c = new SolrRecordConverter(cfg(o));
        Map<String, Object> attrs = new LinkedHashMap<>();
        attrs.put("color", "red");
        attrs.put("size", "L");
        Schema mapSchema = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build();
        Schema schema = SchemaBuilder.struct().field("attrs", mapSchema).build();
        Struct v = new Struct(schema).put("attrs", attrs);

        SolrInputDocument doc = c.convert(rec(schema, v));
        // Each entry becomes one map under field "attrs" — multi-value.
        Collection<Object> vals = doc.getField("attrs").getValues();
        assertThat(vals).hasSize(2);
        assertThat(vals).allSatisfy(o2 -> assertThat(o2).isInstanceOf(Map.class));
    }

    @Test
    void nestedStructFlattens() {
        Schema inner = SchemaBuilder.struct().field("city", Schema.STRING_SCHEMA).build();
        Schema outer = SchemaBuilder.struct().field("addr", inner).build();
        Struct addr = new Struct(inner).put("city", "Tel Aviv");
        Struct value = new Struct(outer).put("addr", addr);

        SolrRecordConverter c = new SolrRecordConverter(cfg(new HashMap<>()));
        SolrInputDocument doc = c.convert(rec(outer, value));
        assertThat(doc.getFieldValue("addr.city")).isEqualTo("Tel Aviv");
    }

    @Test
    void bytesFieldEncodesAsBase64ForBoth_byteArrayAndByteBuffer() {
        Schema schema = SchemaBuilder.struct()
                .field("rawBytes", Schema.BYTES_SCHEMA)
                .field("bufBytes", Schema.BYTES_SCHEMA)
                .build();
        Struct v = new Struct(schema)
                .put("rawBytes", new byte[]{1, 2, 3})
                .put("bufBytes", ByteBuffer.wrap(new byte[]{4, 5, 6}));
        SolrRecordConverter c = new SolrRecordConverter(cfg(new HashMap<>()));
        SolrInputDocument doc = c.convert(rec(schema, v));
        assertThat(doc.getFieldValue("rawBytes")).isEqualTo("AQID");
        assertThat(doc.getFieldValue("bufBytes")).isEqualTo("BAUG");
    }

    @Test
    void decimalFieldEncodesAsString() {
        Schema dec = Decimal.schema(2);
        Schema schema = SchemaBuilder.struct().field("price", dec).build();
        Struct v = new Struct(schema).put("price", new BigDecimal("9.99"));
        SolrRecordConverter c = new SolrRecordConverter(cfg(new HashMap<>()));
        SolrInputDocument doc = c.convert(rec(schema, v));
        assertThat(doc.getFieldValue("price")).isEqualTo("9.99");
    }

    @Test
    void timeAndDateLogicalTypesEncodeAsIso() {
        Schema schema = SchemaBuilder.struct()
                .field("when", Time.SCHEMA)
                .field("day", Date.SCHEMA)
                .field("stamp", Timestamp.SCHEMA)
                .build();
        java.util.Date when = new java.util.Date(60_000L);
        java.util.Date day = new java.util.Date(0L);
        java.util.Date stamp = new java.util.Date(1_000L);
        Struct v = new Struct(schema).put("when", when).put("day", day).put("stamp", stamp);

        SolrRecordConverter c = new SolrRecordConverter(cfg(new HashMap<>()));
        SolrInputDocument doc = c.convert(rec(schema, v));
        assertThat(doc.getFieldValue("when")).isEqualTo("1970-01-01T00:01:00Z");
        assertThat(doc.getFieldValue("day")).isEqualTo("1970-01-01T00:00:00Z");
        assertThat(doc.getFieldValue("stamp")).isEqualTo("1970-01-01T00:00:01Z");
    }

    @Test
    void unsupportedTopLevelValueTypeThrows() {
        SolrRecordConverter c = new SolrRecordConverter(cfg(new HashMap<>()));
        // Top-level value is neither Struct nor Map - a String falls through to the throw.
        SinkRecord r = new SinkRecord("c", 0, Schema.STRING_SCHEMA, "k", Schema.STRING_SCHEMA, "scalar", 1L);
        assertThatThrownBy(() -> c.convert(r)).isInstanceOf(DataException.class);
    }

    @Test
    void atomicUpdateWrapsEveryFieldExceptId() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.WRITE_METHOD_CONFIG, "ATOMIC_UPDATE");
        SolrRecordConverter c = new SolrRecordConverter(cfg(o));
        Schema schema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .build();
        Struct v = new Struct(schema).put("name", "Ada").put("age", 36);
        SolrInputDocument doc = c.convert(rec(schema, v));

        // id stays as-is.
        assertThat(doc.getFieldValue("id")).isEqualTo("k");
        // Other fields wrapped.
        assertThat(doc.getFieldValue("name")).isInstanceOf(Map.class);
        assertThat(((Map<?, ?>) doc.getFieldValue("name")).get("set")).isEqualTo("Ada");
        assertThat(((Map<?, ?>) doc.getFieldValue("age")).get("set")).isEqualTo(36);
    }

    @Test
    void mappingVersionStampsEveryDoc() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.MAPPING_VERSION_CONFIG, "v3");
        SolrRecordConverter c = new SolrRecordConverter(cfg(o));
        Schema schema = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        Struct v = new Struct(schema).put("x", "y");
        SolrInputDocument doc = c.convert(rec(schema, v));
        assertThat(doc.getFieldValue("_mapping_version")).isEqualTo("v3");
    }

    @Test
    void topicPartitionOffsetIdStrategy() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.ID_STRATEGY_CONFIG, "TOPIC_PARTITION_OFFSET");
        SolrRecordConverter c = new SolrRecordConverter(cfg(o));
        Schema schema = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        Struct v = new Struct(schema).put("x", "y");
        SinkRecord r = new SinkRecord("topic-a", 3, null, null, schema, v, 99L);
        SolrInputDocument doc = c.convert(r);
        assertThat(doc.getFieldValue("id")).isEqualTo("topic-a-3-99");
    }
}
