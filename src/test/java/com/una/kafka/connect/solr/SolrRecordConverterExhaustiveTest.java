package com.una.kafka.connect.solr;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drives the remaining SolrRecordConverter branches:
 *  - keyIgnored + KAFKA_KEY downgrade to TPO
 *  - empty-field struct planFor short-circuit
 *  - nested-struct populate via planFor with prefix
 *  - array encoder: non-List Collection input + null items skipped
 *  - addScalar dispatch via untyped scalarEncoder (map without schema)
 *  - map encoder via populateFromMap with prefix and non-compact mode
 *  - atomic update null-field skip
 *  - extractField null and missing-path returns
 *  - 1-arg deriveId overload
 */
class SolrRecordConverterExhaustiveTest {

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
    void deriveIdSingleArgOverloadDelegatesToKeyIgnoreGlobal() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.KEY_IGNORE_CONFIG, "true");
        SolrRecordConverter c = new SolrRecordConverter(cfg(o));
        Schema s = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        Struct v = new Struct(s).put("x", "y");
        SinkRecord r = new SinkRecord("t", 4, null, "ignored", s, v, 7L);
        // 1-arg overload: keyIgnoreGlobal=true forces TPO fallback even though strategy=KAFKA_KEY.
        assertThat(c.deriveId(r)).isEqualTo("t-4-7");
    }

    @Test
    void keyIgnoredDowngradesKafkaKeyToTpoInConvert() {
        SolrRecordConverter c = new SolrRecordConverter(cfg(new HashMap<>()));
        Schema s = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        Struct v = new Struct(s).put("x", "y");
        SinkRecord r = new SinkRecord("topic", 2, Schema.STRING_SCHEMA, "should-ignore", s, v, 33L);
        // keyIgnored=true with default strategy KAFKA_KEY -> TPO downgrade.
        SolrInputDocument doc = c.convert(r, true);
        assertThat(doc.getFieldValue("id")).isEqualTo("topic-2-33");
    }

    @Test
    void emptyStructFieldsTriggersEmptyPlan() {
        SolrRecordConverter c = new SolrRecordConverter(cfg(new HashMap<>()));
        Schema empty = SchemaBuilder.struct().build();
        Struct v = new Struct(empty);
        SinkRecord r = new SinkRecord("c", 0, Schema.STRING_SCHEMA, "k", empty, v, 1L);
        SolrInputDocument doc = c.convert(r);
        // id always present; nothing else.
        assertThat(doc.getFieldNames()).containsExactly("id");
    }

    @Test
    void arrayFieldWithNullItemsSkipped() {
        Schema arr = SchemaBuilder.array(SchemaBuilder.string().optional().build()).build();
        Schema schema = SchemaBuilder.struct().field("tags", arr).build();
        Struct v = new Struct(schema).put("tags", Arrays.asList("a", null, "b"));

        SolrRecordConverter c = new SolrRecordConverter(cfg(new HashMap<>()));
        SolrInputDocument doc = c.convert(rec(schema, v));
        Collection<Object> tags = doc.getField("tags").getValues();
        // Null was skipped; we kept only non-null elements.
        assertThat(tags).containsExactly("a", "b");
    }

    @Test
    void arrayOfBytesEncodesEveryItemAsBase64() {
        Schema arr = SchemaBuilder.array(Schema.BYTES_SCHEMA).build();
        Schema schema = SchemaBuilder.struct().field("blobs", arr).build();
        List<Object> items = new ArrayList<>();
        items.add(new byte[]{1, 2, 3});
        items.add(ByteBuffer.wrap(new byte[]{4, 5}));
        Struct v = new Struct(schema).put("blobs", items);

        SolrRecordConverter c = new SolrRecordConverter(cfg(new HashMap<>()));
        SolrInputDocument doc = c.convert(rec(schema, v));
        assertThat(doc.getField("blobs").getValues()).containsExactly("AQID", "BAU=");
    }

    @Test
    void arrayOfTimestampsEncodesEveryItemAsIso() {
        Schema arr = SchemaBuilder.array(Timestamp.SCHEMA).build();
        Schema schema = SchemaBuilder.struct().field("times", arr).build();
        List<java.util.Date> items = Arrays.asList(new java.util.Date(0L), new java.util.Date(1_000L));
        Struct v = new Struct(schema).put("times", items);

        SolrRecordConverter c = new SolrRecordConverter(cfg(new HashMap<>()));
        SolrInputDocument doc = c.convert(rec(schema, v));
        assertThat(doc.getField("times").getValues())
                .containsExactly("1970-01-01T00:00:00Z", "1970-01-01T00:00:01Z");
    }

    @Test
    void arrayOfDecimalsEncodesEveryItemAsString() {
        Schema arr = SchemaBuilder.array(Decimal.schema(2)).build();
        Schema schema = SchemaBuilder.struct().field("prices", arr).build();
        List<BigDecimal> items = Arrays.asList(new BigDecimal("1.50"), new BigDecimal("3.25"));
        Struct v = new Struct(schema).put("prices", items);

        SolrRecordConverter c = new SolrRecordConverter(cfg(new HashMap<>()));
        SolrInputDocument doc = c.convert(rec(schema, v));
        assertThat(doc.getField("prices").getValues()).containsExactly("1.50", "3.25");
    }

    @Test
    void arrayOfNamedNonLogicalSchemaFallsThroughToDefault() {
        // Name set but not a logical type — switch falls through to scalarEncoderFor default.
        Schema element = SchemaBuilder.string().name("custom.NonLogical").build();
        Schema arr = SchemaBuilder.array(element).build();
        Schema schema = SchemaBuilder.struct().field("xs", arr).build();
        Struct v = new Struct(schema).put("xs", Arrays.asList("hi", "there"));

        SolrRecordConverter c = new SolrRecordConverter(cfg(new HashMap<>()));
        SolrInputDocument doc = c.convert(rec(schema, v));
        assertThat(doc.getField("xs").getValues()).containsExactly("hi", "there");
    }

    @Test
    void mapFieldWithCompactDottedPrefix() {
        // compactMapEntries=true (default) + nested map = dotted prefix.
        Schema mapSch = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build();
        Schema schema = SchemaBuilder.struct().field("attrs", mapSch).build();
        Map<String, String> attrs = new LinkedHashMap<>();
        attrs.put("color", "red");
        Struct v = new Struct(schema).put("attrs", attrs);

        SolrRecordConverter c = new SolrRecordConverter(cfg(new HashMap<>()));
        SolrInputDocument doc = c.convert(rec(schema, v));
        assertThat(doc.getFieldValue("attrs.color")).isEqualTo("red");
    }

    @Test
    void atomicUpdateSkipsNullFieldValue() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.WRITE_METHOD_CONFIG, "ATOMIC_UPDATE");
        SolrRecordConverter c = new SolrRecordConverter(cfg(o));
        Schema schema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("opt", SchemaBuilder.string().optional().build())
                .build();
        Struct v = new Struct(schema).put("name", "Ada");
        // opt left null — should not appear in doc at all because addScalar skips null.
        SolrInputDocument doc = c.convert(rec(schema, v));
        assertThat(doc.getField("opt")).isNull();
        assertThat(((Map<?, ?>) doc.getFieldValue("name")).get("set")).isEqualTo("Ada");
    }

    @Test
    void recordFieldIdViaMapValueExtractsViaMapPath() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.ID_STRATEGY_CONFIG, "RECORD_FIELD");
        o.put(SolrSinkConfig.ID_FIELD_CONFIG, "uid");
        SolrRecordConverter c = new SolrRecordConverter(cfg(o));
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("uid", "abc123");
        value.put("other", "v");
        SinkRecord r = new SinkRecord("c", 0, Schema.STRING_SCHEMA, "k", null, value, 1L);
        SolrInputDocument doc = c.convert(r);
        assertThat(doc.getFieldValue("id")).isEqualTo("abc123");
    }

    @Test
    void recordFieldIdNestedPathTraversesStructAndMap() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.ID_STRATEGY_CONFIG, "RECORD_FIELD");
        o.put(SolrSinkConfig.ID_FIELD_CONFIG, "outer.inner");
        SolrRecordConverter c = new SolrRecordConverter(cfg(o));
        Schema inner = SchemaBuilder.struct().field("inner", Schema.STRING_SCHEMA).build();
        Schema outer = SchemaBuilder.struct().field("outer", inner).build();
        Struct innerVal = new Struct(inner).put("inner", "id-7");
        Struct outerVal = new Struct(outer).put("outer", innerVal);
        SinkRecord r = new SinkRecord("c", 0, Schema.STRING_SCHEMA, "k", outer, outerVal, 1L);
        SolrInputDocument doc = c.convert(r);
        assertThat(doc.getFieldValue("id")).isEqualTo("id-7");
    }

    @Test
    void recordFieldIdScalarMidPathReturnsNullAndThrows() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.ID_STRATEGY_CONFIG, "RECORD_FIELD");
        o.put(SolrSinkConfig.ID_FIELD_CONFIG, "x.deeper");
        SolrRecordConverter c = new SolrRecordConverter(cfg(o));
        Schema schema = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        // x is a string, but the path tries to descend into it — extractField returns null.
        Struct v = new Struct(schema).put("x", "leaf");
        SinkRecord r = new SinkRecord("c", 0, Schema.STRING_SCHEMA, "k", schema, v, 1L);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> c.convert(r))
                .isInstanceOf(org.apache.kafka.connect.errors.DataException.class);
    }
}
