package com.una.kafka.connect.solr;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SolrRecordConverterTest {

    private SolrSinkConfig newConfig(Map<String, String> extra) {
        Map<String, String> props = new HashMap<>();
        props.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://solr:8983/solr");
        props.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "users");
        props.putAll(extra);
        return new SolrSinkConfig(props);
    }

    @Test
    void convertsStructWithKafkaKeyId() {
        SolrRecordConverter converter = new SolrRecordConverter(newConfig(new HashMap<>()));
        Schema schema = SchemaBuilder.struct()
                .field("first_name", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .build();
        Struct value = new Struct(schema).put("first_name", "Ada").put("age", 36);

        SolrInputDocument doc = converter.convert(
                new SinkRecord("users", 0, Schema.STRING_SCHEMA, "user-7", schema, value, 42));
        assertThat(doc.getFieldValue("id")).isEqualTo("user-7");
        assertThat(doc.getFieldValue("first_name")).isEqualTo("Ada");
        assertThat(doc.getFieldValue("age")).isEqualTo(36);
    }

    @Test
    void tombstoneReturnsNull() {
        SolrRecordConverter converter = new SolrRecordConverter(newConfig(new HashMap<>()));
        SinkRecord r = new SinkRecord("users", 0, Schema.STRING_SCHEMA, "k", null, null, 1L);
        assertThat(converter.convert(r)).isNull();
    }

    @Test
    void mapValueConvertsToDocument() {
        SolrRecordConverter converter = new SolrRecordConverter(newConfig(new HashMap<>()));
        Map<String, Object> v = new LinkedHashMap<>();
        v.put("a", "1");
        v.put("b", 2);
        SinkRecord r = new SinkRecord("users", 0, Schema.STRING_SCHEMA, "k", null, v, 1L);
        SolrInputDocument doc = converter.convert(r);
        assertThat(doc.getFieldValue("id")).isEqualTo("k");
        assertThat(doc.getFieldValue("a")).isEqualTo("1");
        assertThat(doc.getFieldValue("b")).isEqualTo(2);
    }

    @Test
    void unsupportedValueTypeThrows() {
        SolrRecordConverter converter = new SolrRecordConverter(newConfig(new HashMap<>()));
        SinkRecord r = new SinkRecord("users", 0, Schema.STRING_SCHEMA, "k", null, 42, 1L);
        assertThatThrownBy(() -> converter.convert(r)).isInstanceOf(DataException.class);
    }

    @Test
    void topicPartitionOffsetIdStrategy() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.ID_STRATEGY_CONFIG, "TOPIC_PARTITION_OFFSET");
        SolrRecordConverter converter = new SolrRecordConverter(newConfig(p));
        Schema schema = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        SolrInputDocument doc = converter.convert(new SinkRecord("t", 3, null, null,
                schema, new Struct(schema).put("x", "y"), 99));
        assertThat(doc.getFieldValue("id")).isEqualTo("t-3-99");
    }

    @Test
    void uuidIdStrategy() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.ID_STRATEGY_CONFIG, "UUID");
        SolrRecordConverter converter = new SolrRecordConverter(newConfig(p));
        Schema schema = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        SolrInputDocument doc = converter.convert(new SinkRecord("t", 0, null, null,
                schema, new Struct(schema).put("x", "y"), 1L));
        assertThat(doc.getFieldValue("id")).asString().hasSize(36);
    }

    @Test
    void recordFieldIdStrategyFromStruct() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.ID_STRATEGY_CONFIG, "RECORD_FIELD");
        p.put(SolrSinkConfig.ID_FIELD_CONFIG, "uid");
        SolrRecordConverter converter = new SolrRecordConverter(newConfig(p));
        Schema schema = SchemaBuilder.struct().field("uid", Schema.STRING_SCHEMA).build();
        SolrInputDocument doc = converter.convert(new SinkRecord("t", 0, null, null,
                schema, new Struct(schema).put("uid", "abc"), 1L));
        assertThat(doc.getFieldValue("id")).isEqualTo("abc");
    }

    @Test
    void recordFieldIdStrategyFromMapWithDotPath() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.ID_STRATEGY_CONFIG, "RECORD_FIELD");
        p.put(SolrSinkConfig.ID_FIELD_CONFIG, "outer.inner");
        SolrRecordConverter converter = new SolrRecordConverter(newConfig(p));
        Map<String, Object> inner = new HashMap<>();
        inner.put("inner", 7);
        Map<String, Object> v = new HashMap<>();
        v.put("outer", inner);
        SolrInputDocument doc = converter.convert(new SinkRecord("t", 0, null, null, null, v, 1L));
        assertThat(doc.getFieldValue("id")).isEqualTo("7");
    }

    @Test
    void recordFieldStrategyMissingFieldThrows() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.ID_STRATEGY_CONFIG, "RECORD_FIELD");
        p.put(SolrSinkConfig.ID_FIELD_CONFIG, "missing");
        SolrRecordConverter converter = new SolrRecordConverter(newConfig(p));
        Schema schema = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        assertThatThrownBy(() -> converter.convert(new SinkRecord("t", 0, null, null,
                schema, new Struct(schema).put("x", "y"), 1L)))
                .isInstanceOf(DataException.class);
    }

    @Test
    void kafkaKeyStrategyNullKeyThrows() {
        SolrRecordConverter converter = new SolrRecordConverter(newConfig(new HashMap<>()));
        Schema schema = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        assertThatThrownBy(() -> converter.convert(new SinkRecord("t", 0, null, null,
                schema, new Struct(schema).put("x", "y"), 1L)))
                .isInstanceOf(DataException.class);
    }

    @Test
    void atomicUpdateWrapsSetOps() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.WRITE_METHOD_CONFIG, "ATOMIC_UPDATE");
        SolrRecordConverter converter = new SolrRecordConverter(newConfig(p));
        Schema schema = SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA).build();
        Struct value = new Struct(schema).put("name", "z");
        SolrInputDocument doc = converter.convert(
                new SinkRecord("t", 0, Schema.STRING_SCHEMA, "k", schema, value, 1));
        Object name = doc.getFieldValue("name");
        assertThat(name).isInstanceOf(Map.class);
        assertThat(((Map<?, ?>) name).get("set")).isEqualTo("z");
    }

    @Test
    void logicalTypesAreSerialized() {
        SolrRecordConverter converter = new SolrRecordConverter(newConfig(new HashMap<>()));
        Schema schema = SchemaBuilder.struct()
                .field("ts", Timestamp.SCHEMA)
                .field("price", Decimal.schema(2))
                .field("blob", Schema.BYTES_SCHEMA)
                .field("bb", Schema.BYTES_SCHEMA)
                .field("d", Schema.OPTIONAL_INT64_SCHEMA)
                .build();
        Struct value = new Struct(schema)
                .put("ts", new Date(0))
                .put("price", new BigDecimal("1.23"))
                .put("blob", new byte[]{1, 2, 3})
                .put("bb", ByteBuffer.wrap(new byte[]{4, 5}).array())
                .put("d", null);

        SolrInputDocument doc = converter.convert(
                new SinkRecord("t", 0, Schema.STRING_SCHEMA, "k", schema, value, 1L));
        assertThat(doc.getFieldValue("ts")).asString().contains("T");
        assertThat(doc.getFieldValue("price")).isEqualTo("1.23");
        assertThat(doc.getFieldValue("blob")).isNotNull();
        assertThat(doc.getFieldValue("bb")).isNotNull();
        // null was dropped (no field added)
        assertThat(doc.getFieldNames()).doesNotContain("d");
    }

    @Test
    void rawDateIsAlsoSerialized() {
        SolrRecordConverter converter = new SolrRecordConverter(newConfig(new HashMap<>()));
        Map<String, Object> v = new HashMap<>();
        v.put("when", new Date(0));
        SolrInputDocument doc = converter.convert(
                new SinkRecord("t", 0, Schema.STRING_SCHEMA, "k", null, v, 1L));
        assertThat(doc.getFieldValue("when")).asString().contains("T");
    }

    @Test
    void arrayAndNestedStructHandled() {
        SolrRecordConverter converter = new SolrRecordConverter(newConfig(new HashMap<>()));
        Schema inner = SchemaBuilder.struct().field("city", Schema.STRING_SCHEMA).build();
        Schema tagsArr = SchemaBuilder.array(Schema.STRING_SCHEMA).build();
        Schema schema = SchemaBuilder.struct()
                .field("tags", tagsArr)
                .field("addr", inner)
                .build();

        Struct value = new Struct(schema)
                .put("tags", Arrays.asList("a", "b"))
                .put("addr", new Struct(inner).put("city", "NYC"));

        SolrInputDocument doc = converter.convert(
                new SinkRecord("t", 0, Schema.STRING_SCHEMA, "k", schema, value, 1L));
        Collection<Object> tags = doc.getFieldValues("tags");
        assertThat(tags).containsExactly("a", "b");
        assertThat(doc.getFieldValue("addr.city")).isEqualTo("NYC");
    }
}
