package com.una.kafka.connect.solr.transforms;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A PostGIS POINT, as Debezium emits it, becomes Solr's {@code "lat,lon"} string — on the fly.
 *
 * <p>Debezium's Postgres connector emits a PostGIS {@code geometry} as
 * {@code io.debezium.data.geometry.Geometry}: a struct of {@code wkb} (BYTES) and {@code srid}.
 * Solr's {@code location} field ({@code LatLonPointSpatialField}) wants {@code "lat,lon"}. This
 * replaces the field's value with that string, and its schema with an optional STRING, so everything
 * downstream — the streamer, the Solr sink — receives exactly what it always has.
 *
 * <p>It runs on the Debezium SOURCE connector, after {@code unwrap}, so the database carries nothing
 * extra: no copy column, no trigger, no backfill. It replaced a trigger-maintained
 * {@code location_text} column that needed two migrations and a full-table backfill (2026-09-23).
 *
 * <p>A record without the field passes through untouched, so one chain serves every captured table.
 * A value that is not a readable POINT becomes {@code null} with a warning — one bad row must never
 * stop change capture for the whole database.
 */
public class GeometryToLatLon<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String FIELD_CONFIG = "field";
    public static final String FIELD_DEFAULT = "location";
    static final String WKB_FIELD = "wkb";

    public static final ConfigDef CONFIG_DEF = new ConfigDef().define(
        FIELD_CONFIG, ConfigDef.Type.STRING, FIELD_DEFAULT, ConfigDef.Importance.MEDIUM,
        "The value field holding the PostGIS point; replaced by \"lat,lon\".");

    private static final Logger LOG = LoggerFactory.getLogger(GeometryToLatLon.class);

    // WKB type word: EWKB carries flag bits above the geometry code, ISO WKB adds 1000/2000/3000 for Z/M.
    private static final int EWKB_SRID_FLAG = 0x20000000;
    private static final int EWKB_FLAG_MASK = 0x0FFFFFFF;
    private static final int ISO_DIMENSION_STEP = 1000;
    private static final int POINT = 1;
    private static final int XDR_BIG_ENDIAN = 0;
    private static final int NDR_LITTLE_ENDIAN = 1;
    private static final int COORDINATE_BYTES = 2 * Double.BYTES;

    private final Cache<Schema, Schema> schemaCache = new SynchronizedCache<>(new LRUCache<>(16));
    private String field = FIELD_DEFAULT;

    @Override
    public void configure(Map<String, ?> props) {
        field = new SimpleConfig(CONFIG_DEF, props).getString(FIELD_CONFIG);
    }

    @Override
    public R apply(R record) {
        if (!(record.value() instanceof Struct)) {
            return record;                  // tombstones and schemaless values are not ours to touch
        }
        Struct value = (Struct) record.value();
        if (value.schema().field(field) == null) {
            return record;                  // another table: nothing to convert
        }
        Schema updatedSchema = schemaCache.get(value.schema());
        if (updatedSchema == null) {
            updatedSchema = withStringField(value.schema());
            schemaCache.put(value.schema(), updatedSchema);
        }
        Struct updated = new Struct(updatedSchema);
        for (Field each : value.schema().fields()) {
            Object fieldValue = value.get(each);
            updated.put(each.name(), each.name().equals(field) ? latLon(fieldValue, record) : fieldValue);
        }
        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(),
                                record.key(), updatedSchema, updated, record.timestamp());
    }

    private Schema withStringField(Schema schema) {
        SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        for (Field each : schema.fields()) {
            builder.field(each.name(),
                          each.name().equals(field) ? Schema.OPTIONAL_STRING_SCHEMA : each.schema());
        }
        return builder.build();
    }

    private String latLon(Object geometry, R record) {
        if (geometry == null) {
            return null;
        }
        String converted = geometry instanceof Struct ? pointToLatLon(wkbOf((Struct) geometry)) : null;
        if (converted == null) {
            LOG.warn("GeometryToLatLon: `{}` is not a readable POINT (topic {}, key {}); indexed "
                     + "without a location", field, record.topic(), record.key());
        }
        return converted;
    }

    private static byte[] wkbOf(Struct geometry) {
        if (geometry.schema().field(WKB_FIELD) == null) {
            return null;
        }
        Object wkb = geometry.get(WKB_FIELD);
        if (wkb instanceof ByteBuffer) {
            ByteBuffer buffer = ((ByteBuffer) wkb).duplicate();
            byte[] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            return bytes;
        }
        return (byte[]) wkb;
    }

    /**
     * {@code "lat,lon"} for a WKB/EWKB POINT (XDR or NDR, 2D or with Z/M), else {@code null}.
     * X is longitude and Y latitude, as PostGIS stores them; the string is Y first, as Solr reads it.
     */
    static String pointToLatLon(byte[] wkb) {
        if (wkb == null || wkb.length < 1 + Integer.BYTES + COORDINATE_BYTES) {
            return null;
        }
        ByteBuffer buffer = ByteBuffer.wrap(wkb);
        byte byteOrder = buffer.get();
        if (byteOrder != XDR_BIG_ENDIAN && byteOrder != NDR_LITTLE_ENDIAN) {
            return null;
        }
        buffer.order(byteOrder == XDR_BIG_ENDIAN ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN);
        int type = buffer.getInt();
        if ((type & EWKB_FLAG_MASK) % ISO_DIMENSION_STEP != POINT) {
            return null;
        }
        if ((type & EWKB_SRID_FLAG) != 0) {
            if (buffer.remaining() < Integer.BYTES + COORDINATE_BYTES) {
                return null;
            }
            buffer.getInt();                // the SRID; Debezium sends it separately anyway
        }
        double longitude = buffer.getDouble();
        double latitude = buffer.getDouble();
        if (Double.isNaN(longitude) || Double.isNaN(latitude)) {
            return null;                    // POINT EMPTY
        }
        return plain(latitude) + "," + plain(longitude);
    }

    /** Never scientific notation: {@code 1.0E-5} is a valid double and not a coordinate Solr reads. */
    private static String plain(double coordinate) {
        return BigDecimal.valueOf(coordinate).stripTrailingZeros().toPlainString();
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        // nothing held
    }
}
