package com.una.kafka.connect.solr.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * After the migration of WKB→"lat,lon" conversion to Postgres (see
 * scripts/migrations/2026_06_08_add_user_location_text.sql + the renameLocation SMT in
 * scripts/debezium_setup.py), the Solr-sink no longer wires {@link WKBToLatLon} into its
 * transform chain. The class still lives in this repo so it can be re-enabled in a
 * rollback scenario.
 *
 * <p>These tests pin the behavior the rollback path depends on:
 *
 * <ol>
 *   <li>If WKBToLatLon ever runs against a record where {@code location} is ALREADY a
 *       "lat,lon" string (the new post-Postgres-conversion format), it MUST pass the
 *       record through unchanged. Re-enabling the SMT during rollback while some records
 *       still carry the new format must not corrupt them.</li>
 *   <li>Surrounding fields of every Kafka Connect primitive type must survive the
 *       struct-path rewrite. The original tests only exercised STRING fields alongside
 *       the geo field; this confirms int8/16/32/64, float32/64, and boolean siblings
 *       round-trip too.</li>
 *   <li>Raw byte[] values in the geo field are not currently decoded (only String/Map
 *       are supported by extractWkbString). This is documented limitation; if Debezium
 *       ever changes to emit byte[] for geometry columns, this test fails loudly.</li>
 * </ol>
 */
class WKBToLatLonPostMigrationTest {

    private WKBToLatLon<SourceRecord> newTransform() {
        WKBToLatLon<SourceRecord> t = new WKBToLatLon<>();
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("field", "location");
        t.configure(cfg);
        return t;
    }

    // ============== Rollback safety: already-converted records pass through ==============

    @Test
    void alreadyConvertedLatLonStringInMapPassesThrough() {
        // Postgres-side conversion emits location as "lat,lon" text. If WKBToLatLon is
        // re-enabled during rollback, isHex("40.7128,-74.0060") is false (',', '.' not hex),
        // Base64 decoder throws on ',', decode returns null, decodeLatLon returns null,
        // apply returns the record value unchanged. assertSame proves no allocation.
        WKBToLatLon<SourceRecord> t = newTransform();
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("id", 42);
        value.put("location", "40.7128,-74.0060");
        value.put("name", "Ada");

        SourceRecord r = new SourceRecord(null, null, "t", 0, null, value);
        assertThat(t.apply(r).value()).isSameAs(value);
    }

    @Test
    void alreadyConvertedLatLonStringInStructPassesThrough() {
        WKBToLatLon<SourceRecord> t = newTransform();
        Schema schema = SchemaBuilder.struct()
                .field("location", Schema.STRING_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .build();
        Struct value = new Struct(schema)
                .put("location", "40.7128,-74.0060")
                .put("name", "Ada");

        SourceRecord r = new SourceRecord(null, null, "t", 0, schema, value);
        assertThat(t.apply(r)).isSameAs(r);
    }

    @Test
    void negativeOnlyCoordsAlsoPassThrough() {
        // Edge case: all-negative coords. Sign means '-' which is not a base64 char in
        // the default decoder, so decode returns null -> unchanged.
        WKBToLatLon<SourceRecord> t = newTransform();
        Map<String, Object> value = new HashMap<>();
        value.put("location", "-33.8688,-151.2093");
        SourceRecord r = new SourceRecord(null, null, "t", 0, null, value);
        assertThat(t.apply(r).value()).isSameAs(value);
    }

    @Test
    void integerOnlyCoordsAlsoPassThrough() {
        // Both coords are whole numbers — no decimal point. The string still contains ','
        // and '-', so it can't be hex or base64. Pass through.
        WKBToLatLon<SourceRecord> t = newTransform();
        Map<String, Object> value = new HashMap<>();
        value.put("location", "40,-74");
        SourceRecord r = new SourceRecord(null, null, "t", 0, null, value);
        assertThat(t.apply(r).value()).isSameAs(value);
    }

    // ============== Data-type pass-through: all primitive sibling fields survive ==============

    @Test
    void structPathPreservesAllPrimitiveSiblingFields() {
        // Empire State Building hex WKB — same fixture the original test uses.
        final String validWkb = "0101000000F1F44A5986F652C03333333333584440";

        WKBToLatLon<SourceRecord> t = newTransform();
        Schema schema = SchemaBuilder.struct()
                .field("id",         Schema.INT64_SCHEMA)
                .field("age",        Schema.INT32_SCHEMA)
                .field("rating",     Schema.INT16_SCHEMA)
                .field("flags",      Schema.INT8_SCHEMA)
                .field("height",     Schema.FLOAT64_SCHEMA)
                .field("score",      Schema.FLOAT32_SCHEMA)
                .field("active",     Schema.BOOLEAN_SCHEMA)
                .field("first_name", Schema.STRING_SCHEMA)
                .field("location",   Schema.STRING_SCHEMA)
                .build();
        Struct value = new Struct(schema)
                .put("id",         9_000_000_000_000_000L)   // bigint range
                .put("age",        36)
                .put("rating",     (short) 5)
                .put("flags",      (byte) 1)
                .put("height",     1.65)
                .put("score",      0.875f)
                .put("active",     true)
                .put("first_name", "Ada")
                .put("location",   validWkb);

        SourceRecord r = new SourceRecord(null, null, "t", 0, schema, value);
        Struct updated = (Struct) t.apply(r).value();

        // location field rewritten; every other type must survive untouched.
        assertThat(updated.schema().field("location").schema()).isEqualTo(Schema.OPTIONAL_STRING_SCHEMA);
        assertThat(updated.getString("location")).contains(",");
        assertThat(updated.getInt64("id")).isEqualTo(9_000_000_000_000_000L);
        assertThat(updated.getInt32("age")).isEqualTo(36);
        assertThat(updated.getInt16("rating")).isEqualTo((short) 5);
        assertThat(updated.getInt8("flags")).isEqualTo((byte) 1);
        assertThat(updated.getFloat64("height")).isEqualTo(1.65);
        assertThat(updated.getFloat32("score")).isEqualTo(0.875f);
        assertThat(updated.getBoolean("active")).isTrue();
        assertThat(updated.getString("first_name")).isEqualTo("Ada");
    }

    // ============== Documented limitation: raw byte[] in geo field is not decoded ==============

    @Test
    void byteArrayRawValueIsNotCurrentlyDecoded() {
        // extractWkbString accepts only String or Map (with "wkb" key). Raw byte[] —
        // which Debezium COULD emit for a Postgres bytea/geometry column under different
        // converter settings — is silently passed through unchanged. Pin this as the
        // current contract so we notice if it ever needs to change.
        WKBToLatLon<SourceRecord> t = newTransform();
        Map<String, Object> value = new HashMap<>();
        value.put("location", new byte[]{0x01, 0x01, 0x00, 0x00, 0x00});
        SourceRecord r = new SourceRecord(null, null, "t", 0, null, value);
        assertThat(t.apply(r).value()).isSameAs(value);
    }

    @Test
    void numericFieldValueIsNotDecoded() {
        // Defensive: a Long where a WKB string was expected. Documents that the SMT
        // won't crash on type mismatch — it leaves the record alone.
        WKBToLatLon<SourceRecord> t = newTransform();
        Map<String, Object> value = new HashMap<>();
        value.put("location", 12345L);
        SourceRecord r = new SourceRecord(null, null, "t", 0, null, value);
        assertThat(t.apply(r).value()).isSameAs(value);
    }

    @Test
    void booleanFieldValueIsNotDecoded() {
        WKBToLatLon<SourceRecord> t = newTransform();
        Map<String, Object> value = new HashMap<>();
        value.put("location", Boolean.TRUE);
        SourceRecord r = new SourceRecord(null, null, "t", 0, null, value);
        assertThat(t.apply(r).value()).isSameAs(value);
    }
}
