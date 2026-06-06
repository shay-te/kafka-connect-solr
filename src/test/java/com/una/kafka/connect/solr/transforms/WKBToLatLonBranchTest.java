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
 * Drives the remaining null-branches in WKBToLatLon: null wkb field, invalid wkb
 * payload, schema-cache hit on the second call, non-string non-map raw, empty
 * string, odd-length hex, lowercase/uppercase/non-hex char permutations.
 */
class WKBToLatLonBranchTest {

    private static final String VALID_WKB_HEX = "0101000000F1F44A5986F652C03333333333584440";

    private WKBToLatLon<SourceRecord> newTransform() {
        WKBToLatLon<SourceRecord> t = new WKBToLatLon<>();
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("field", "geo");
        t.configure(cfg);
        return t;
    }

    @Test
    void mapWithNullGeoFieldReturnsUnchanged() {
        WKBToLatLon<SourceRecord> t = newTransform();
        Map<String, Object> value = new HashMap<>();
        value.put("geo", null);   // present but null
        SourceRecord r = new SourceRecord(null, null, "t", 0, null, value);
        assertThat(t.apply(r).value()).isSameAs(value);
    }

    @Test
    void structWithNullGeoFieldReturnsUnchanged() {
        WKBToLatLon<SourceRecord> t = newTransform();
        Schema schema = SchemaBuilder.struct()
                .field("geo", SchemaBuilder.string().optional().build())
                .field("name", Schema.STRING_SCHEMA)
                .build();
        Struct value = new Struct(schema).put("name", "x");   // geo left null
        SourceRecord r = new SourceRecord(null, null, "t", 0, schema, value);
        assertThat(t.apply(r)).isSameAs(r);
    }

    @Test
    void structWithInvalidWkbReturnsUnchanged() {
        WKBToLatLon<SourceRecord> t = newTransform();
        Schema schema = SchemaBuilder.struct().field("geo", Schema.STRING_SCHEMA).build();
        Struct value = new Struct(schema).put("geo", "ZZZZ");  // invalid wkb
        SourceRecord r = new SourceRecord(null, null, "t", 0, schema, value);
        assertThat(t.apply(r)).isSameAs(r);
    }

    @Test
    void mapWithEmbeddedNullWkbInnerObject() {
        WKBToLatLon<SourceRecord> t = newTransform();
        Map<String, Object> nested = new HashMap<>();
        nested.put("wkb", null);   // inner wkb is null -> extractWkbString returns null
        Map<String, Object> value = new HashMap<>();
        value.put("geo", nested);
        SourceRecord r = new SourceRecord(null, null, "t", 0, null, value);
        assertThat(t.apply(r).value()).isSameAs(value);
    }

    @Test
    void mapWithNonStringNonMapRawValueReturnsUnchanged() {
        WKBToLatLon<SourceRecord> t = newTransform();
        Map<String, Object> value = new HashMap<>();
        value.put("geo", 12345);    // Integer falls through extractWkbString to return null
        SourceRecord r = new SourceRecord(null, null, "t", 0, null, value);
        assertThat(t.apply(r).value()).isSameAs(value);
    }

    @Test
    void mapWithEmptyWkbStringReturnsUnchanged() {
        // Empty string: isHex returns false (n==0), Base64 succeeds returning empty,
        // and decodeLatLon's WKBReader throws -> latLon=null -> unchanged.
        WKBToLatLon<SourceRecord> t = newTransform();
        Map<String, Object> value = new HashMap<>();
        value.put("geo", "");
        SourceRecord r = new SourceRecord(null, null, "t", 0, null, value);
        assertThat(t.apply(r).value()).isSameAs(value);
    }

    @Test
    void mapWithOddLengthWkbStringReturnsUnchanged() {
        // Odd length: isHex short-circuits (n & 1 != 0) -> falls back to Base64,
        // which may decode or fail; either way result is no LatLon.
        WKBToLatLon<SourceRecord> t = newTransform();
        Map<String, Object> value = new HashMap<>();
        value.put("geo", "ABC");   // 3 chars - odd
        SourceRecord r = new SourceRecord(null, null, "t", 0, null, value);
        assertThat(t.apply(r).value()).isSameAs(value);
    }

    @Test
    void mapWithInvalidHexCharInEvenStringReturnsUnchanged() {
        // 4-char string with one non-hex char: isHex returns false -> Base64 fallback.
        WKBToLatLon<SourceRecord> t = newTransform();
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("geo", "!!!!");
        SourceRecord r = new SourceRecord(null, null, "t", 0, null, value);
        assertThat(t.apply(r).value()).isSameAs(value);
    }

    @Test
    void schemaCacheHitOnRepeatedStructPath() {
        // Second call with the same schema should hit the cache and skip the rebuild.
        WKBToLatLon<SourceRecord> t = newTransform();
        Schema schema = SchemaBuilder.struct()
                .field("geo", Schema.STRING_SCHEMA)
                .field("name", Schema.STRING_SCHEMA)
                .build();
        Struct a = new Struct(schema).put("geo", VALID_WKB_HEX).put("name", "first");
        Struct b = new Struct(schema).put("geo", VALID_WKB_HEX).put("name", "second");
        SourceRecord ra = new SourceRecord(null, null, "t", 0, schema, a);
        SourceRecord rb = new SourceRecord(null, null, "t", 0, schema, b);

        // First call populates the cache; second call hits it.
        assertThat(((Struct) t.apply(ra).value()).getString("location")).contains(",");
        assertThat(((Struct) t.apply(rb).value()).getString("location")).contains(",");
    }

    @Test
    void lowercaseHexAlsoDecodes() {
        // Drives the (c >= 'a' && c <= 'f') branch in isHex.
        WKBToLatLon<SourceRecord> t = newTransform();
        Map<String, Object> value = new HashMap<>();
        value.put("geo", VALID_WKB_HEX.toLowerCase());
        SourceRecord r = new SourceRecord(null, null, "t", 0, null, value);
        @SuppressWarnings("unchecked")
        Map<String, Object> updated = (Map<String, Object>) t.apply(r).value();
        assertThat(updated.get("location")).asString().contains(",");
    }
}
