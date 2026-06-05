package com.una.kafka.connect.solr;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

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
        SolrSinkConfig cfg = newConfig(new HashMap<>());
        SolrRecordConverter converter = new SolrRecordConverter(cfg);

        Schema schema = SchemaBuilder.struct()
                .field("first_name", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .build();
        Struct value = new Struct(schema).put("first_name", "Ada").put("age", 36);

        SinkRecord rec = new SinkRecord("users", 0,
                Schema.STRING_SCHEMA, "user-7", schema, value, 42);

        SolrInputDocument doc = converter.convert(rec);
        assertThat(doc.getFieldValue("id")).isEqualTo("user-7");
        assertThat(doc.getFieldValue("first_name")).isEqualTo("Ada");
        assertThat(doc.getFieldValue("age")).isEqualTo(36);
    }

    @Test
    void topicPartitionOffsetIdStrategy() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.ID_STRATEGY_CONFIG, "TOPIC_PARTITION_OFFSET");
        SolrRecordConverter converter = new SolrRecordConverter(newConfig(p));

        Schema schema = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        Struct value = new Struct(schema).put("x", "y");
        SinkRecord rec = new SinkRecord("t", 3, null, null, schema, value, 99);

        SolrInputDocument doc = converter.convert(rec);
        assertThat(doc.getFieldValue("id")).isEqualTo("t-3-99");
    }

    @Test
    void atomicUpdateWrapsSetOps() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.WRITE_METHOD_CONFIG, "ATOMIC_UPDATE");
        SolrRecordConverter converter = new SolrRecordConverter(newConfig(p));

        Schema schema = SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA).build();
        Struct value = new Struct(schema).put("name", "z");
        SinkRecord rec = new SinkRecord("t", 0, Schema.STRING_SCHEMA, "k", schema, value, 1);

        SolrInputDocument doc = converter.convert(rec);
        Object name = doc.getFieldValue("name");
        assertThat(name).isInstanceOf(Map.class);
        assertThat(((Map<?, ?>) name).get("set")).isEqualTo("z");
    }
}
