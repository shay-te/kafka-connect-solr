package com.una.kafka.connect.solr;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drives the remaining SolrRecordConverter lambda dispatch paths: arrays of
 * struct / map elements, addScalar's logical-name and Date/byte[]/ByteBuffer
 * cases reached via top-level Map input, plus the populateFromMap key-value
 * wrap path when no schema is present.
 */
class SolrRecordConverterBranchTest {

    private SolrSinkConfig cfg(Map<String, String> extra) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "c");
        p.putAll(extra);
        return new SolrSinkConfig(p);
    }

    private SinkRecord recMap(Map<?, ?> value) {
        return new SinkRecord("c", 0, Schema.STRING_SCHEMA, "k", null, value, 1L);
    }

    @Test
    void topLevelMapWithTimestampValueRoutesThroughAddScalar() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("when", new java.util.Date(1_000L));
        SolrRecordConverter c = new SolrRecordConverter(cfg(new HashMap<>()));
        SolrInputDocument doc = c.convert(recMap(m));
        assertThat(doc.getFieldValue("when")).isEqualTo("1970-01-01T00:00:01Z");
    }

    @Test
    void topLevelMapWithByteArrayValueRoutesThroughAddScalar() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("blob", new byte[]{1, 2, 3});
        m.put("buf", ByteBuffer.wrap(new byte[]{4, 5, 6}));
        SolrRecordConverter c = new SolrRecordConverter(cfg(new HashMap<>()));
        SolrInputDocument doc = c.convert(recMap(m));
        assertThat(doc.getFieldValue("blob")).isEqualTo("AQID");
        assertThat(doc.getFieldValue("buf")).isEqualTo("BAUG");
    }

    @Test
    void topLevelMapWithNestedMapValueRoutesViaAddScalar() {
        // compact=true (default) flattens via prefix; nested map under key "addr".
        Map<String, Object> inner = new LinkedHashMap<>();
        inner.put("city", "Berlin");
        Map<String, Object> outer = new LinkedHashMap<>();
        outer.put("addr", inner);
        SolrRecordConverter c = new SolrRecordConverter(cfg(new HashMap<>()));
        SolrInputDocument doc = c.convert(recMap(outer));
        assertThat(doc.getFieldValue("addr.city")).isEqualTo("Berlin");
    }

    @Test
    void arrayOfStructsRoutesThroughScalarEncoderStructBranch() {
        // ARRAY of STRUCT: each element flows through scalarEncoderFor's STRUCT lambda.
        Schema element = SchemaBuilder.struct().field("city", Schema.STRING_SCHEMA).build();
        Schema arr = SchemaBuilder.array(element).build();
        Schema schema = SchemaBuilder.struct().field("addresses", arr).build();
        Struct a = new Struct(element).put("city", "A");
        Struct b = new Struct(element).put("city", "B");
        Struct v = new Struct(schema).put("addresses", Arrays.asList(a, b));

        SolrRecordConverter c = new SolrRecordConverter(cfg(new HashMap<>()));
        SinkRecord r = new SinkRecord("c", 0, Schema.STRING_SCHEMA, "k", schema, v, 1L);
        SolrInputDocument doc = c.convert(r);
        // Each struct's city flattens — scalarEncoderFor STRUCT branch ran for each.
        Collection<Object> cities = doc.getField("addresses.city").getValues();
        assertThat(cities).containsExactly("A", "B");
    }

    @Test
    void arrayOfMapsRoutesThroughScalarEncoderMapBranch() {
        // ARRAY of MAP: scalarEncoderFor's MAP lambda fires for each element.
        Schema mapSch = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build();
        Schema arr = SchemaBuilder.array(mapSch).build();
        Schema schema = SchemaBuilder.struct().field("attrs", arr).build();
        Map<String, String> e1 = new LinkedHashMap<>();
        e1.put("k1", "v1");
        Map<String, String> e2 = new LinkedHashMap<>();
        e2.put("k2", "v2");
        Struct v = new Struct(schema).put("attrs", Arrays.asList(e1, e2));

        SolrRecordConverter c = new SolrRecordConverter(cfg(new HashMap<>()));
        SinkRecord r = new SinkRecord("c", 0, Schema.STRING_SCHEMA, "k", schema, v, 1L);
        SolrInputDocument doc = c.convert(r);
        // Compact dotted prefix produces attrs.k1 / attrs.k2.
        assertThat(doc.getFieldValue("attrs.k1")).isEqualTo("v1");
        assertThat(doc.getFieldValue("attrs.k2")).isEqualTo("v2");
    }

    @Test
    void topLevelMapWithStructValueRoutesViaAddScalar() {
        // Top-level Map containing a Struct value — addScalar's Struct branch fires.
        Schema inner = SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA).build();
        Struct innerStruct = new Struct(inner).put("name", "Ada");
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("user", innerStruct);

        SolrRecordConverter c = new SolrRecordConverter(cfg(new HashMap<>()));
        SolrInputDocument doc = c.convert(recMap(m));
        assertThat(doc.getFieldValue("user.name")).isEqualTo("Ada");
    }

    @Test
    void atomicUpdateNullFieldValueIsSkipped() {
        // Specifically: an atomic update doc that contains a SolrInputField whose value is null
        // is not wrapped in a "set" map.
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.WRITE_METHOD_CONFIG, "ATOMIC_UPDATE");
        SolrRecordConverter c = new SolrRecordConverter(cfg(o));
        // Build a value Map that includes an explicit null entry.
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("present", "yes");
        m.put("missing", null);
        SolrInputDocument doc = c.convert(recMap(m));
        // present wrapped, missing was never added (addScalar returns when value is null).
        assertThat(doc.getFieldValue("present")).isInstanceOf(Map.class);
        assertThat(doc.getField("missing")).isNull();
    }

    @Test
    void extractFieldEmptyPathReturnsNullAndThrows() {
        // id.field = "" — split("\\.") yields [""], so the path traversal returns null
        // and we throw DataException.
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.ID_STRATEGY_CONFIG, "RECORD_FIELD");
        o.put(SolrSinkConfig.ID_FIELD_CONFIG, "");
        SolrRecordConverter c = new SolrRecordConverter(cfg(o));
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("x", "y");
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> c.convert(recMap(m)))
                .isInstanceOf(org.apache.kafka.connect.errors.DataException.class);
    }

    @Test
    void decimalAndTimestampInsideTopLevelMap() {
        // No schema; addScalar with schema==null + special types — Date routes via instanceof.
        // Note: without a Decimal logical-type schema, BigDecimal passes through unconverted.
        // Production callers that need string-encoded decimals should provide the schema —
        // see SolrDataTypeParityTest for the schema-driven path.
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("d", new BigDecimal("1.23"));
        m.put("t", new java.util.Date(0L));
        SolrRecordConverter c = new SolrRecordConverter(cfg(new HashMap<>()));
        SolrInputDocument doc = c.convert(recMap(m));
        assertThat(doc.getFieldValue("d")).isInstanceOf(BigDecimal.class);
        assertThat(doc.getFieldValue("t")).isEqualTo("1970-01-01T00:00:00Z");
    }
}
