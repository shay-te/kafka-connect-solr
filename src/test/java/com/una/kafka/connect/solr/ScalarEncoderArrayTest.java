package com.una.kafka.connect.solr;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ScalarEncoderArrayTest {

    private SolrRecordConverter converter() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "c");
        return new SolrRecordConverter(new SolrSinkConfig(p));
    }

    private static SolrInputDocument convertWith(SolrRecordConverter c, Schema schema, Struct value) {
        return c.convert(new SinkRecord("c", 0, Schema.STRING_SCHEMA, "k", schema, value, 1L));
    }

    @Test
    void arrayOfStringsKeepsAllElements() {
        Schema arr = SchemaBuilder.array(Schema.STRING_SCHEMA).build();
        Schema schema = SchemaBuilder.struct().field("tags", arr).build();
        Struct v = new Struct(schema).put("tags", Arrays.asList("a", "b", "c"));

        SolrInputDocument doc = convertWith(converter(), schema, v);
        SolrInputField tags = doc.getField("tags");
        assertThat(tags.getValues()).containsExactly("a", "b", "c");
    }

    @Test
    void arrayOfInts() {
        Schema arr = SchemaBuilder.array(Schema.INT32_SCHEMA).build();
        Schema schema = SchemaBuilder.struct().field("nums", arr).build();
        Struct v = new Struct(schema).put("nums", Arrays.asList(1, 2, 3));

        SolrInputDocument doc = convertWith(converter(), schema, v);
        assertThat(doc.getField("nums").getValues()).containsExactly(1, 2, 3);
    }

    @Test
    void arrayOfDecimalsEncodesAsStrings() {
        Schema decimal = Decimal.schema(2);
        Schema arr = SchemaBuilder.array(decimal).build();
        Schema schema = SchemaBuilder.struct().field("prices", arr).build();
        Struct v = new Struct(schema).put("prices", Arrays.asList(new BigDecimal("9.99"), new BigDecimal("12.50")));

        SolrInputDocument doc = convertWith(converter(), schema, v);
        assertThat(doc.getField("prices").getValues()).containsExactly("9.99", "12.50");
    }

    @Test
    void arrayOfTimestampsEncodesAsIso() {
        Schema arr = SchemaBuilder.array(Timestamp.SCHEMA).build();
        Schema schema = SchemaBuilder.struct().field("when", arr).build();
        Date d1 = new Date(0);
        Date d2 = new Date(86400000L);
        Struct v = new Struct(schema).put("when", Arrays.asList(d1, d2));

        SolrInputDocument doc = convertWith(converter(), schema, v);
        Collection<Object> vals = doc.getField("when").getValues();
        assertThat(vals).containsExactly("1970-01-01T00:00:00Z", "1970-01-02T00:00:00Z");
    }

    @Test
    void arrayOfBytesEncodesAsBase64() {
        Schema arr = SchemaBuilder.array(Schema.BYTES_SCHEMA).build();
        Schema schema = SchemaBuilder.struct().field("blobs", arr).build();
        byte[] b1 = {1, 2, 3};
        byte[] b2 = {4, 5, 6};
        Struct v = new Struct(schema).put("blobs", Arrays.asList(b1, ByteBuffer.wrap(b2)));

        SolrInputDocument doc = convertWith(converter(), schema, v);
        Collection<Object> vals = doc.getField("blobs").getValues();
        assertThat(vals).containsExactly("AQID", "BAUG");
    }

    @Test
    void arrayElementsSkipNulls() {
        Schema arr = SchemaBuilder.array(SchemaBuilder.string().optional().build()).build();
        Schema schema = SchemaBuilder.struct().field("tags", arr).build();
        Struct v = new Struct(schema).put("tags", Arrays.asList("a", null, "c"));

        SolrInputDocument doc = convertWith(converter(), schema, v);
        assertThat(doc.getField("tags").getValues()).containsExactly("a", "c");
    }

    @Test
    void linkedListFallsBackToIteratorPath() {
        // Default Collections used internally by Connect are usually ArrayList,
        // but the encoder must still work when the value is any Collection.
        Schema arr = SchemaBuilder.array(Schema.STRING_SCHEMA).build();
        Schema schema = SchemaBuilder.struct().field("tags", arr).build();
        List<String> list = new LinkedList<>(Arrays.asList("x", "y"));
        Struct v = new Struct(schema).put("tags", list);

        SolrInputDocument doc = convertWith(converter(), schema, v);
        assertThat(doc.getField("tags").getValues()).containsExactly("x", "y");
    }
}
