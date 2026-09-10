package com.una.kafka.connect.solr;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drives the null-side branches of every per-field encoder lambda in
 * SolrRecordConverter — Date, Decimal, STRUCT, MAP, BYTES, etc. — by feeding
 * a Struct where each typed field is explicitly left null.
 */
class SolrRecordConverterNullBranchTest {

    private SolrSinkConfig cfg(Map<String, String> extras) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "c");
        p.putAll(extras);
        return new SolrSinkConfig(p);
    }

    private SinkRecord rec(Schema s, Struct v) {
        return new SinkRecord("c", 0, Schema.STRING_SCHEMA, "k", s, v, 1L);
    }

    @Test
    void nullTimestampValueSkipsEncoder() {
        Schema schema = SchemaBuilder.struct()
                .field("when", SchemaBuilder.type(Timestamp.SCHEMA.type()).name(Timestamp.LOGICAL_NAME).optional().build())
                .build();
        Struct v = new Struct(schema);
        // when is null — encoder lambda's `if (v != null)` false-side hit.
        SolrRecordConverter c = new SolrRecordConverter(cfg(new HashMap<>()));
        SolrInputDocument doc = c.convert(rec(schema, v));
        assertThat(doc.getField("when")).isNull();
    }

    @Test
    void nullDecimalValueSkipsEncoder() {
        Schema dec = Decimal.builder(2).optional().build();
        Schema schema = SchemaBuilder.struct().field("price", dec).build();
        Struct v = new Struct(schema);
        SolrRecordConverter c = new SolrRecordConverter(cfg(new HashMap<>()));
        SolrInputDocument doc = c.convert(rec(schema, v));
        assertThat(doc.getField("price")).isNull();
    }

    @Test
    void nullNestedStructValueSkipsRecurse() {
        Schema inner = SchemaBuilder.struct().field("city", Schema.STRING_SCHEMA).optional().build();
        Schema outer = SchemaBuilder.struct().field("addr", inner).build();
        Struct v = new Struct(outer); // addr left null
        SolrRecordConverter c = new SolrRecordConverter(cfg(new HashMap<>()));
        SolrInputDocument doc = c.convert(rec(outer, v));
        assertThat(doc.getField("addr.city")).isNull();
    }

    @Test
    void nullMapFieldSkipsRecurse() {
        Schema mapSch = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).optional().build();
        Schema schema = SchemaBuilder.struct().field("attrs", mapSch).build();
        Struct v = new Struct(schema); // attrs null
        SolrRecordConverter c = new SolrRecordConverter(cfg(new HashMap<>()));
        SolrInputDocument doc = c.convert(rec(schema, v));
        assertThat(doc.getField("attrs")).isNull();
    }

    @Test
    void nullArrayFieldSkipsRecurse() {
        Schema arr = SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build();
        Schema schema = SchemaBuilder.struct().field("tags", arr).build();
        Struct v = new Struct(schema);
        SolrRecordConverter c = new SolrRecordConverter(cfg(new HashMap<>()));
        SolrInputDocument doc = c.convert(rec(schema, v));
        assertThat(doc.getField("tags")).isNull();
    }

    @Test
    void nullBytesFieldSkipsBothBranches() {
        Schema bytes = SchemaBuilder.bytes().optional().build();
        Schema schema = SchemaBuilder.struct().field("blob", bytes).build();
        Struct v = new Struct(schema);
        SolrRecordConverter c = new SolrRecordConverter(cfg(new HashMap<>()));
        SolrInputDocument doc = c.convert(rec(schema, v));
        assertThat(doc.getField("blob")).isNull();
    }

    @Test
    void nullScalarValueSkippedInDefaultEncoder() {
        Schema schema = SchemaBuilder.struct()
                .field("name", SchemaBuilder.string().optional().build())
                .build();
        Struct v = new Struct(schema);
        SolrRecordConverter c = new SolrRecordConverter(cfg(new HashMap<>()));
        SolrInputDocument doc = c.convert(rec(schema, v));
        assertThat(doc.getField("name")).isNull();
    }

    @Test
    void extractFieldWithNullValueReturnsNull() {
        // RECORD_FIELD id strategy with null value path triggers extractField's null guard.
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.ID_STRATEGY_CONFIG, "RECORD_FIELD");
        o.put(SolrSinkConfig.ID_FIELD_CONFIG, "nested.id");
        SolrRecordConverter c = new SolrRecordConverter(cfg(o));
        Map<String, Object> m = new HashMap<>();
        m.put("nested", null);   // null mid-path -> extractField returns null -> throws
        SinkRecord r = new SinkRecord("c", 0, Schema.STRING_SCHEMA, "k", null, m, 1L);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> c.convert(r))
                .isInstanceOf(org.apache.kafka.connect.errors.DataException.class);
    }

    @Test
    void compactMapEntriesFalseTopLevelMap() {
        // compactMapEntries=false + top-level Map (prefix="") hits the second branch of populateFromMap.
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.COMPACT_MAP_ENTRIES_CONFIG, "false");
        SolrRecordConverter c = new SolrRecordConverter(cfg(o));
        Map<String, Object> m = new HashMap<>();
        m.put("a", "1");
        SinkRecord r = new SinkRecord("c", 0, Schema.STRING_SCHEMA, "k", null, m, 1L);
        SolrInputDocument doc = c.convert(r);
        assertThat(doc.getFieldValue("a")).isEqualTo("1");
    }
}
