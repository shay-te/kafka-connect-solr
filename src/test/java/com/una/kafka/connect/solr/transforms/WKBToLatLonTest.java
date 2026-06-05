package com.una.kafka.connect.solr.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class WKBToLatLonTest {

    // Point(-73.9857 40.7484) (Empire State Building, NYC) encoded as little-endian hex WKB.
    private static final String EMPIRE_STATE_WKB = "0101000000F1F44A5986F652C03333333333584440";

    private WKBToLatLon<SourceRecord> newTransform(Map<String, Object> overrides) {
        WKBToLatLon<SourceRecord> t = new WKBToLatLon<>();
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("field", "geo");
        cfg.putAll(overrides);
        t.configure(cfg);
        return t;
    }

    @Test
    void nullRecordPassesThrough() {
        WKBToLatLon<SourceRecord> t = newTransform(new HashMap<>());
        SourceRecord r = new SourceRecord(null, null, "t", 0, null, null);
        assertThat(t.apply(r).value()).isNull();
    }

    @Test
    void mapPathHexString() {
        WKBToLatLon<SourceRecord> t = newTransform(new HashMap<>());
        Map<String, Object> value = new HashMap<>();
        value.put("geo", EMPIRE_STATE_WKB);
        value.put("name", "x");

        SourceRecord r = new SourceRecord(null, null, "t", 0, null, value);
        SourceRecord out = t.apply(r);

        @SuppressWarnings("unchecked")
        Map<String, Object> updated = (Map<String, Object>) out.value();
        assertThat(updated).doesNotContainKey("geo");
        assertThat(updated.get("location")).asString().contains(",");
        assertThat(updated.get("name")).isEqualTo("x");
    }

    @Test
    void mapPathNestedWkbObject() {
        WKBToLatLon<SourceRecord> t = newTransform(new HashMap<>());
        Map<String, Object> nested = new HashMap<>();
        nested.put("wkb", EMPIRE_STATE_WKB);
        Map<String, Object> value = new HashMap<>();
        value.put("geo", nested);

        SourceRecord r = new SourceRecord(null, null, "t", 0, null, value);
        @SuppressWarnings("unchecked")
        Map<String, Object> updated = (Map<String, Object>) t.apply(r).value();
        assertThat(updated.get("location")).asString().contains(",");
    }

    @Test
    void mapPathMissingFieldReturnsRecord() {
        WKBToLatLon<SourceRecord> t = newTransform(new HashMap<>());
        Map<String, Object> value = new HashMap<>();
        value.put("other", "x");

        SourceRecord r = new SourceRecord(null, null, "t", 0, null, value);
        assertThat(t.apply(r).value()).isSameAs(value);
    }

    @Test
    void mapPathBadWkbReturnsRecord() {
        WKBToLatLon<SourceRecord> t = newTransform(new HashMap<>());
        Map<String, Object> value = new HashMap<>();
        value.put("geo", "ZZZZ");
        SourceRecord r = new SourceRecord(null, null, "t", 0, null, value);
        assertThat(t.apply(r).value()).isSameAs(value);
    }

    @Test
    void structPath() {
        WKBToLatLon<SourceRecord> t = newTransform(new HashMap<>());
        Schema schema = SchemaBuilder.struct()
                .field("geo", Schema.STRING_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .build();
        Struct value = new Struct(schema).put("geo", EMPIRE_STATE_WKB).put("name", "x");
        SourceRecord r = new SourceRecord(null, null, "t", 0, schema, value);

        Struct updated = (Struct) t.apply(r).value();
        assertThat(updated.schema().field("geo")).isNull();
        assertThat(updated.getString("location")).contains(",");
        assertThat(updated.getString("name")).isEqualTo("x");
    }

    @Test
    void structPathMissingField() {
        WKBToLatLon<SourceRecord> t = newTransform(new HashMap<>());
        Schema schema = SchemaBuilder.struct().field("other", Schema.STRING_SCHEMA).build();
        Struct value = new Struct(schema).put("other", "x");
        SourceRecord r = new SourceRecord(null, null, "t", 0, schema, value);
        assertThat(t.apply(r)).isSameAs(r);
    }

    @Test
    void primitiveValueIsReturnedUnchanged() {
        WKBToLatLon<SourceRecord> t = newTransform(new HashMap<>());
        SourceRecord r = new SourceRecord(null, null, "t", 0, Schema.STRING_SCHEMA, "not-a-struct");
        assertThat(t.apply(r)).isSameAs(r);
    }

    @Test
    void targetFieldOverride() {
        Map<String, Object> overrides = new HashMap<>();
        overrides.put("target.field", "pos");
        WKBToLatLon<SourceRecord> t = newTransform(overrides);
        Map<String, Object> value = new HashMap<>();
        value.put("geo", EMPIRE_STATE_WKB);
        SourceRecord r = new SourceRecord(null, null, "t", 0, null, value);
        @SuppressWarnings("unchecked")
        Map<String, Object> updated = (Map<String, Object>) t.apply(r).value();
        assertThat(updated).containsKey("pos");
    }

    @Test
    void configDefAndCloseAreSafe() {
        WKBToLatLon<SourceRecord> t = newTransform(new HashMap<>());
        assertThat(t.config()).isNotNull();
        t.close();
    }

    @Test
    void base64FallbackDecodes() {
        // Same point but base64-encoded instead of hex.
        String b64 = java.util.Base64.getEncoder().encodeToString(
                hex(EMPIRE_STATE_WKB));
        WKBToLatLon<SourceRecord> t = newTransform(new HashMap<>());
        Map<String, Object> value = new HashMap<>();
        value.put("geo", b64);
        SourceRecord r = new SourceRecord(null, null, "t", 0, null, value);
        @SuppressWarnings("unchecked")
        Map<String, Object> updated = (Map<String, Object>) t.apply(r).value();
        assertThat(updated.get("location")).asString().contains(",");
    }

    private static byte[] hex(String s) {
        byte[] data = new byte[s.length() / 2];
        for (int i = 0; i < s.length(); i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i + 1), 16));
        }
        return data;
    }
}
