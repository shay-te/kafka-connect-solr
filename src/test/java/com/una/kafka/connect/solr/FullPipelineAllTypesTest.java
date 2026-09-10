package com.una.kafka.connect.solr;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The whole pipeline: a Struct carrying EVERY Kafka Connect data type — the 8 primitives, the 4
 * logical types, BYTES as both byte[] and ByteBuffer, arrays, nested structs, and a map — pushed
 * through {@link SolrRecordConverter}. Real values in, exact Solr fields asserted out.
 *
 * <p>`location` arrives as the "lat,lon" string Postgres now emits via the location_text
 * generated column (the WKB-decoding SMT was removed once the DB produced the value directly).</p>
 */
class FullPipelineAllTypesTest {

    /** Empire State Building, in the "lat,lon" form the location_text column emits. */
    private static final String POINT_LAT_LON = "40.7484,-73.9857";
    private static final Pattern LAT_LON = Pattern.compile("^-?\\d+(\\.\\d+)?,-?\\d+(\\.\\d+)?$");

    private SolrSinkConfig config() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://solr:8983/solr");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "users");
        return new SolrSinkConfig(p);
    }

    @Test
    void everyDataTypeSurvivesConversion() {
        Schema geoPoint = SchemaBuilder.struct().name("Geo")
                .field("lat", Schema.FLOAT64_SCHEMA).field("lon", Schema.FLOAT64_SCHEMA).build();

        Schema schema = SchemaBuilder.struct()
                .field("location",   Schema.STRING_SCHEMA)       // "lat,lon" from location_text
                .field("i8",         Schema.INT8_SCHEMA)
                .field("i16",        Schema.INT16_SCHEMA)
                .field("i32",        Schema.INT32_SCHEMA)
                .field("i64",        Schema.INT64_SCHEMA)
                .field("f32",        Schema.FLOAT32_SCHEMA)
                .field("f64",        Schema.FLOAT64_SCHEMA)
                .field("flag",       Schema.BOOLEAN_SCHEMA)
                .field("name",       Schema.STRING_SCHEMA)
                .field("avatar",     Schema.BYTES_SCHEMA)
                .field("signature",  Schema.BYTES_SCHEMA)
                .field("balance",    Decimal.schema(2))
                .field("created_at", Timestamp.SCHEMA)
                .field("birth",      Date.SCHEMA)
                .field("login",      Time.SCHEMA)
                .field("tags",       SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                .field("home",       geoPoint)                    // nested struct
                .field("attrs",      SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build())
                .build();

        byte[] avatar = "avatar".getBytes();
        Struct value = new Struct(schema)
                .put("location",   POINT_LAT_LON)
                .put("i8",         (byte) 7)
                .put("i16",        (short) 1234)
                .put("i32",        42)
                .put("i64",        9_000_000_000_000_000L)
                .put("f32",        0.5f)
                .put("f64",        2.71828d)
                .put("flag",       true)
                .put("name",       "Ada 💙 λόγος")
                .put("avatar",     avatar)
                .put("signature",  ByteBuffer.wrap(new byte[]{9, 8, 7}))
                .put("balance",    new BigDecimal("1234.56"))
                .put("created_at", java.util.Date.from(Instant.parse("2026-07-06T12:00:00Z")))
                .put("birth",      java.util.Date.from(Instant.parse("1815-12-10T00:00:00Z")))
                .put("login",      new java.util.Date(3_600_000L))
                .put("tags",       List.of("x", "y", "z"))
                .put("home",       new Struct(geoPoint).put("lat", 40.7).put("lon", -74.0))
                .put("attrs",      Map.of("floor", 102));

        // --- the real converter turns it into a Solr document ---
        SolrRecordConverter converter = new SolrRecordConverter(config());
        SolrInputDocument doc = converter.convert(
                new SinkRecord("users", 0, Schema.STRING_SCHEMA, "u1", schema, value, 5L));

        // --- EVERY type asserted in the emitted document ---
        assertThat(doc.getFieldValue("id")).isEqualTo("u1");
        assertThat(LAT_LON.matcher((String) doc.getFieldValue("location")).matches()).isTrue();
        assertThat(doc.getFieldValue("i8")).isEqualTo((byte) 7);
        assertThat(doc.getFieldValue("i16")).isEqualTo((short) 1234);
        assertThat(doc.getFieldValue("i32")).isEqualTo(42);
        assertThat(doc.getFieldValue("i64")).isEqualTo(9_000_000_000_000_000L);
        assertThat(doc.getFieldValue("f32")).isEqualTo(0.5f);
        assertThat(doc.getFieldValue("f64")).isEqualTo(2.71828d);
        assertThat(doc.getFieldValue("flag")).isEqualTo(true);
        assertThat(doc.getFieldValue("name")).isEqualTo("Ada 💙 λόγος");           // unicode intact
        assertThat(doc.getFieldValue("avatar")).isEqualTo(Base64.getEncoder().encodeToString(avatar));
        assertThat(doc.getFieldValue("signature")).isEqualTo(Base64.getEncoder().encodeToString(new byte[]{9, 8, 7}));
        assertThat(doc.getFieldValue("balance")).isEqualTo("1234.56");             // Decimal -> string
        assertThat(doc.getFieldValue("created_at")).isEqualTo("2026-07-06T12:00:00Z");
        assertThat(doc.getFieldValue("birth").toString()).startsWith("1815-12-10T00:00:00Z");
        assertThat(doc.getFieldValue("login")).isEqualTo("1970-01-01T01:00:00Z");
        assertThat(doc.getFieldValues("tags")).containsExactly("x", "y", "z");
        assertThat(doc.getFieldValue("home.lat")).isEqualTo(40.7);                 // nested struct flattened
        assertThat(doc.getFieldValue("home.lon")).isEqualTo(-74.0);
        assertThat(doc.getFieldValue("attrs.floor")).isEqualTo(102);               // map flattened
    }
}
