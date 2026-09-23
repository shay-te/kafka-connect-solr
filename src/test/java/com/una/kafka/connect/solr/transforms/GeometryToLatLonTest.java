package com.una.kafka.connect.solr.transforms;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The PostGIS point Debezium emits for {@code public.user.location} reaches the pipeline as
 * {@code "lat,lon"} — built from the real shape of a Debezium record after {@code unwrap}.
 */
class GeometryToLatLonTest {

    /** {@code io.debezium.data.geometry.Geometry}, as Debezium's Postgres connector declares it. */
    private static final Schema GEOMETRY = SchemaBuilder.struct()
        .name("io.debezium.data.geometry.Geometry").optional()
        .field("wkb", Schema.BYTES_SCHEMA)
        .field("srid", Schema.OPTIONAL_INT32_SCHEMA)
        .build();

    private static final Schema USER = SchemaBuilder.struct().name("ob_love.public.user.Value")
        .field("id", Schema.INT64_SCHEMA)
        .field("email", Schema.OPTIONAL_STRING_SCHEMA)
        .field("location", GEOMETRY)
        .build();

    // Taken from PostGIS itself, not composed by hand:
    //   encode(ST_AsBinary(ST_MakePoint(-75.8519, 40.69)), 'hex')
    // and PostGIS renders the same point as `ST_Y || ',' || ST_X` = "40.69,-75.8519" — the exact
    // string the old trigger-maintained location_text column held, so the wire format is unchanged.
    private static final String POSTGIS_POINT_HEX = "010100000098dd938785f652c0b81e85eb51584440";
    //   encode(ST_AsEWKB(ST_SetSRID(ST_MakePoint(34.7818, 32.0853), 4326)), 'hex')
    private static final String POSTGIS_EWKB_4326_HEX = "0101000020e6100000a301bc0512644140a52c431ceb0a4040";

    private GeometryToLatLon<SourceRecord> transform;

    @BeforeEach
    void setUp() {
        transform = new GeometryToLatLon<>();
        transform.configure(Collections.emptyMap());
    }

    // ---- WKB -----------------------------------------------------------------------------------

    @Test
    void aPostgisPointBecomesLatThenLon() {
        assertEquals("40.69,-75.8519", GeometryToLatLon.pointToLatLon(hex(POSTGIS_POINT_HEX)));
    }

    @Test
    void bigEndianPointsReadTheSame() {
        assertEquals("32.0853,34.7818",
                     GeometryToLatLon.pointToLatLon(point(ByteOrder.BIG_ENDIAN, 1, null, 34.7818, 32.0853)));
    }

    @Test
    void anEwkbPointCarryingItsSridIsRead() {
        assertEquals("32.0853,34.7818", GeometryToLatLon.pointToLatLon(hex(POSTGIS_EWKB_4326_HEX)));
        assertEquals("32.0853,34.7818", GeometryToLatLon.pointToLatLon(
            point(ByteOrder.BIG_ENDIAN, 0x20000001, 4326, 34.7818, 32.0853)));
    }

    @Test
    void zAndMCoordinatesAreIgnored() {
        assertEquals("1.5,2.5", GeometryToLatLon.pointToLatLon(
            point(ByteOrder.LITTLE_ENDIAN, 1001, null, 2.5, 1.5, 99.0)));            // ISO POINT Z
        assertEquals("1.5,2.5", GeometryToLatLon.pointToLatLon(
            point(ByteOrder.LITTLE_ENDIAN, 0x80000001, null, 2.5, 1.5, 99.0)));      // EWKB POINT Z
    }

    @Test
    void coordinatesAreNeverWrittenInScientificNotation() {
        assertEquals("0.00001,-0.00002", GeometryToLatLon.pointToLatLon(
            point(ByteOrder.LITTLE_ENDIAN, 1, null, -0.00002, 0.00001)));
        assertEquals("40,0", GeometryToLatLon.pointToLatLon(point(ByteOrder.LITTLE_ENDIAN, 1, null, 0.0, 40.0)));
    }

    @Test
    void anythingThatIsNotAReadablePointIsNull() {
        assertNull(GeometryToLatLon.pointToLatLon(null));
        assertNull(GeometryToLatLon.pointToLatLon(new byte[5]), "truncated");
        byte[] badOrder = hex(POSTGIS_POINT_HEX);
        badOrder[0] = 7;
        assertNull(GeometryToLatLon.pointToLatLon(badOrder), "unknown byte-order marker");
        assertNull(GeometryToLatLon.pointToLatLon(point(ByteOrder.LITTLE_ENDIAN, 2, null, 1.0, 2.0)), "LINESTRING");
        assertNull(GeometryToLatLon.pointToLatLon(point(ByteOrder.LITTLE_ENDIAN, 1, null, Double.NaN, Double.NaN)),
                   "POINT EMPTY");
        byte[] sridButShort = new byte[21];
        ByteBuffer.wrap(sridButShort).order(ByteOrder.LITTLE_ENDIAN).put((byte) 1).putInt(0x20000001);
        assertNull(GeometryToLatLon.pointToLatLon(sridButShort), "EWKB whose SRID leaves no room for coordinates");
    }

    // ---- the record ------------------------------------------------------------------------------

    @Test
    void theLocationFieldBecomesAnOptionalStringAndEverythingElseIsKept() {
        SourceRecord converted = transform.apply(userRecord(geometry(hex(POSTGIS_POINT_HEX))));
        Struct value = (Struct) converted.value();

        assertEquals("40.69,-75.8519", value.getString("location"));
        assertEquals(Schema.OPTIONAL_STRING_SCHEMA, converted.valueSchema().field("location").schema());
        assertEquals(7L, value.getInt64("id"));
        assertEquals("u@example.com", value.getString("email"));
        assertEquals(USER.name(), converted.valueSchema().name());
        assertEquals("ob_love.public.user", converted.topic());
        assertEquals(Integer.valueOf(0), converted.kafkaPartition());
        assertEquals(Collections.singletonMap("id", 7), converted.key());
        assertEquals(Long.valueOf(123L), converted.timestamp());
    }

    @Test
    void aUserWithoutALocationKeepsANullLocation() {
        Struct value = (Struct) transform.apply(userRecord(null)).value();
        assertNull(value.getString("location"));
    }

    @Test
    void anUnreadablePointIsIndexedWithoutALocationRatherThanStoppingCapture() {
        Struct value = (Struct) transform.apply(userRecord(geometry(new byte[3]))).value();
        assertNull(value.getString("location"));
    }

    @Test
    void aGeometryWithoutWkbIsIndexedWithoutALocation() {
        Schema noWkb = SchemaBuilder.struct().optional().field("srid", Schema.OPTIONAL_INT32_SCHEMA).build();
        Schema user = SchemaBuilder.struct().field("id", Schema.INT64_SCHEMA).field("location", noWkb).build();
        Struct value = new Struct(user).put("id", 1L).put("location", new Struct(noWkb).put("srid", 4326));
        Struct converted = (Struct) transform.apply(record(user, value)).value();
        assertNull(converted.getString("location"));
    }

    @Test
    void aLocationThatIsNotAStructIsIndexedWithoutALocation() {
        Schema user = SchemaBuilder.struct().field("id", Schema.INT64_SCHEMA)
            .field("location", Schema.OPTIONAL_STRING_SCHEMA).build();
        Struct value = new Struct(user).put("id", 1L).put("location", "already text");
        assertNull(((Struct) transform.apply(record(user, value)).value()).getString("location"));
    }

    @Test
    void wkbHandedOverAsAByteBufferIsReadToo() {
        Struct geometry = new Struct(GEOMETRY).put("wkb", ByteBuffer.wrap(hex(POSTGIS_POINT_HEX))).put("srid", 4326);
        assertEquals("40.69,-75.8519", ((Struct) transform.apply(userRecord(geometry)).value()).getString("location"));
    }

    @Test
    void anotherTablesRecordPassesThroughUntouched() {
        Schema comment = SchemaBuilder.struct().field("id", Schema.INT64_SCHEMA).build();
        SourceRecord record = record(comment, new Struct(comment).put("id", 3L));
        assertSame(record, transform.apply(record));
    }

    @Test
    void tombstonesAndSchemalessValuesPassThroughUntouched() {
        SourceRecord tombstone = new SourceRecord(null, null, "ob_love.public.user", 0, null, null);
        assertSame(tombstone, transform.apply(tombstone));
        SourceRecord schemaless = new SourceRecord(null, null, "t", 0, null,
                                                   Collections.singletonMap("location", "x"));
        assertSame(schemaless, transform.apply(schemaless));
    }

    @Test
    void theConvertedSchemaIsBuiltOncePerInputSchema() {
        Schema first = transform.apply(userRecord(geometry(hex(POSTGIS_POINT_HEX)))).valueSchema();
        Schema second = transform.apply(userRecord(null)).valueSchema();
        assertSame(first, second);
    }

    @Test
    void theFieldIsConfigurable() {
        Map<String, String> props = new HashMap<>();
        props.put(GeometryToLatLon.FIELD_CONFIG, "home");
        transform.configure(props);
        Schema user = SchemaBuilder.struct().field("home", GEOMETRY).build();
        Struct value = new Struct(user).put("home", geometry(hex(POSTGIS_POINT_HEX)));
        assertEquals("40.69,-75.8519", ((Struct) transform.apply(record(user, value)).value()).getString("home"));
    }

    @Test
    void itDeclaresItsOneSettingWithTheDefault() {
        assertEquals(GeometryToLatLon.FIELD_DEFAULT,
                     transform.config().defaultValues().get(GeometryToLatLon.FIELD_CONFIG));
        transform.close();
    }

    // ---- helpers --------------------------------------------------------------------------------

    private static SourceRecord userRecord(Struct location) {
        Struct value = new Struct(USER).put("id", 7L).put("email", "u@example.com").put("location", location);
        return record(USER, value);
    }

    private static SourceRecord record(Schema schema, Struct value) {
        return new SourceRecord(null, null, "ob_love.public.user", 0, null,
                                Collections.singletonMap("id", 7), schema, value, 123L);
    }

    private static Struct geometry(byte[] wkb) {
        return new Struct(GEOMETRY).put("wkb", wkb).put("srid", 4326);
    }

    private static byte[] point(ByteOrder order, int type, Integer srid, double... coordinates) {
        ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + (srid == null ? 0 : 4) + 8 * coordinates.length).order(order);
        buffer.put((byte) (order == ByteOrder.BIG_ENDIAN ? 0 : 1)).putInt(type);
        if (srid != null) {
            buffer.putInt(srid);
        }
        for (double coordinate : coordinates) {
            buffer.putDouble(coordinate);
        }
        return buffer.array();
    }

    private static byte[] hex(String hex) {
        byte[] bytes = new byte[hex.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) Integer.parseInt(hex.substring(2 * i, 2 * i + 2), 16);
        }
        return bytes;
    }
}
