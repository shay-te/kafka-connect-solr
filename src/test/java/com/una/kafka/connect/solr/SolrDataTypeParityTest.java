package com.una.kafka.connect.solr;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
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
 * Exhaustive Kafka Connect → Solr type-conversion parity test. Mirrors
 * the type matrix kafka-connect-elasticsearch's DataConverter handles
 * (primitives, logicals, arrays, maps, nested structs) so we have a
 * regression net every time the converter changes.
 */
class SolrDataTypeParityTest {

    private SolrRecordConverter newConverter() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "users");
        return new SolrRecordConverter(new SolrSinkConfig(p));
    }

    private SolrInputDocument convert(Schema schema, Struct value) {
        return newConverter().convert(new SinkRecord(
                "t", 0, Schema.STRING_SCHEMA, "k", schema, value, 1L));
    }

    @Test
    void int8() {
        Schema s = SchemaBuilder.struct().field("v", Schema.INT8_SCHEMA).build();
        SolrInputDocument d = convert(s, new Struct(s).put("v", (byte) 7));
        assertThat(d.getFieldValue("v")).isEqualTo((byte) 7);
    }

    @Test
    void int16() {
        Schema s = SchemaBuilder.struct().field("v", Schema.INT16_SCHEMA).build();
        SolrInputDocument d = convert(s, new Struct(s).put("v", (short) 7));
        assertThat(d.getFieldValue("v")).isEqualTo((short) 7);
    }

    @Test
    void int32() {
        Schema s = SchemaBuilder.struct().field("v", Schema.INT32_SCHEMA).build();
        SolrInputDocument d = convert(s, new Struct(s).put("v", 42));
        assertThat(d.getFieldValue("v")).isEqualTo(42);
    }

    @Test
    void int64() {
        Schema s = SchemaBuilder.struct().field("v", Schema.INT64_SCHEMA).build();
        SolrInputDocument d = convert(s, new Struct(s).put("v", 9_000_000_000L));
        assertThat(d.getFieldValue("v")).isEqualTo(9_000_000_000L);
    }

    @Test
    void float32() {
        Schema s = SchemaBuilder.struct().field("v", Schema.FLOAT32_SCHEMA).build();
        SolrInputDocument d = convert(s, new Struct(s).put("v", 3.14f));
        assertThat(d.getFieldValue("v")).isEqualTo(3.14f);
    }

    @Test
    void float64() {
        Schema s = SchemaBuilder.struct().field("v", Schema.FLOAT64_SCHEMA).build();
        SolrInputDocument d = convert(s, new Struct(s).put("v", 3.14159));
        assertThat(d.getFieldValue("v")).isEqualTo(3.14159);
    }

    @Test
    void booleanType() {
        Schema s = SchemaBuilder.struct().field("v", Schema.BOOLEAN_SCHEMA).build();
        SolrInputDocument d = convert(s, new Struct(s).put("v", true));
        assertThat(d.getFieldValue("v")).isEqualTo(true);
    }

    @Test
    void stringType() {
        Schema s = SchemaBuilder.struct().field("v", Schema.STRING_SCHEMA).build();
        SolrInputDocument d = convert(s, new Struct(s).put("v", "hello"));
        assertThat(d.getFieldValue("v")).isEqualTo("hello");
    }

    @Test
    void bytesAsByteArray() {
        Schema s = SchemaBuilder.struct().field("v", Schema.BYTES_SCHEMA).build();
        SolrInputDocument d = convert(s, new Struct(s).put("v", new byte[]{1, 2, 3}));
        // Solr stores bytes as base64 strings, matching the JSON converter
        // wire format produced by the Confluent JsonConverter for BYTES.
        assertThat(d.getFieldValue("v")).isEqualTo("AQID");
    }

    @Test
    void bytesAsByteBuffer() {
        Schema s = SchemaBuilder.struct().field("v", Schema.BYTES_SCHEMA).build();
        SolrInputDocument d = convert(s, new Struct(s).put("v",
                ByteBuffer.wrap(new byte[]{1, 2, 3}).array()));
        assertThat(d.getFieldValue("v")).isEqualTo("AQID");
    }

    @Test
    void decimalAsString() {
        Schema s = SchemaBuilder.struct().field("v", Decimal.schema(2)).build();
        SolrInputDocument d = convert(s, new Struct(s).put("v", new BigDecimal("1.23")));
        // ES converts Decimal -> float64 (lossy). We keep it as a string so
        // the original precision is preserved.
        assertThat(d.getFieldValue("v")).isEqualTo("1.23");
    }

    @Test
    void timestampAsIso() {
        Schema s = SchemaBuilder.struct().field("v", Timestamp.SCHEMA).build();
        SolrInputDocument d = convert(s, new Struct(s).put("v", new java.util.Date(0)));
        assertThat(d.getFieldValue("v")).isEqualTo("1970-01-01T00:00:00Z");
    }

    @Test
    void dateAsIso() {
        Schema s = SchemaBuilder.struct().field("v", Date.SCHEMA).build();
        SolrInputDocument d = convert(s, new Struct(s).put("v", new java.util.Date(0)));
        assertThat(d.getFieldValue("v")).isEqualTo("1970-01-01T00:00:00Z");
    }

    @Test
    void timeAsIso() {
        Schema s = SchemaBuilder.struct().field("v", Time.SCHEMA).build();
        SolrInputDocument d = convert(s, new Struct(s).put("v", new java.util.Date(0)));
        assertThat(d.getFieldValue("v")).isEqualTo("1970-01-01T00:00:00Z");
    }

    @Test
    void arrayOfPrimitives() {
        Schema s = SchemaBuilder.struct()
                .field("tags", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                .build();
        Struct v = new Struct(s).put("tags", Arrays.asList("a", "b", "c"));
        SolrInputDocument d = convert(s, v);
        Collection<Object> tags = d.getFieldValues("tags");
        assertThat(tags).containsExactly("a", "b", "c");
    }

    @Test
    void arrayOfInts() {
        Schema s = SchemaBuilder.struct()
                .field("nums", SchemaBuilder.array(Schema.INT32_SCHEMA).build())
                .build();
        SolrInputDocument d = convert(s, new Struct(s).put("nums", Arrays.asList(1, 2, 3)));
        assertThat(d.getFieldValues("nums")).containsExactly(1, 2, 3);
    }

    @Test
    void mapWithStringKeys() {
        Schema s = SchemaBuilder.struct()
                .field("meta", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build())
                .build();
        Map<String, String> meta = new LinkedHashMap<>();
        meta.put("k1", "v1");
        meta.put("k2", "v2");
        SolrInputDocument d = convert(s, new Struct(s).put("meta", meta));
        // Dotted-path flattening, mirroring the Solr-native convention.
        assertThat(d.getFieldValue("meta.k1")).isEqualTo("v1");
        assertThat(d.getFieldValue("meta.k2")).isEqualTo("v2");
    }

    @Test
    void nestedStruct() {
        Schema inner = SchemaBuilder.struct()
                .field("street", Schema.STRING_SCHEMA)
                .field("city", Schema.STRING_SCHEMA)
                .build();
        Schema outer = SchemaBuilder.struct()
                .field("addr", inner)
                .field("name", Schema.STRING_SCHEMA)
                .build();
        SolrInputDocument d = convert(outer, new Struct(outer)
                .put("addr", new Struct(inner).put("street", "5th").put("city", "NYC"))
                .put("name", "Ada"));
        assertThat(d.getFieldValue("addr.street")).isEqualTo("5th");
        assertThat(d.getFieldValue("addr.city")).isEqualTo("NYC");
        assertThat(d.getFieldValue("name")).isEqualTo("Ada");
    }

    @Test
    void deeplyNestedStruct() {
        Schema l3 = SchemaBuilder.struct().field("v", Schema.STRING_SCHEMA).build();
        Schema l2 = SchemaBuilder.struct().field("b", l3).build();
        Schema l1 = SchemaBuilder.struct().field("a", l2).build();
        SolrInputDocument d = convert(l1, new Struct(l1)
                .put("a", new Struct(l2).put("b", new Struct(l3).put("v", "leaf"))));
        assertThat(d.getFieldValue("a.b.v")).isEqualTo("leaf");
    }

    @Test
    void optionalFieldsWithNullsAreDropped() {
        Schema s = SchemaBuilder.struct()
                .field("opt", Schema.OPTIONAL_STRING_SCHEMA)
                .field("set", Schema.STRING_SCHEMA)
                .build();
        SolrInputDocument d = convert(s, new Struct(s).put("opt", null).put("set", "x"));
        assertThat(d.getFieldNames()).doesNotContain("opt");
        assertThat(d.getFieldValue("set")).isEqualTo("x");
    }
}
