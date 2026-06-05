package com.una.kafka.connect.solr.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKBReader;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * SMT that takes a WKB-encoded geometry field (the format Debezium emits for
 * PostGIS columns) and replaces it with a Solr-friendly "lat,lon" string in
 * the {@code location} field. Solr's LatLonPointSpatialField expects exactly
 * that representation. This is the Solr counterpart of
 * {@code com.una.WKBToGeoPoint} already used by the Elasticsearch sink.
 */
public class WKBToLatLon<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String FIELD_CONFIG = "field";
    private static final String TARGET_FIELD_CONFIG = "target.field";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELD_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                    "The field name containing WKB data.")
            .define(TARGET_FIELD_CONFIG, ConfigDef.Type.STRING, "location", ConfigDef.Importance.LOW,
                    "The field name to write the lat,lon string into.");

    private String fieldName;
    private String targetField;

    @Override
    public void configure(Map<String, ?> configs) {
        SimpleConfig cfg = new SimpleConfig(CONFIG_DEF, configs);
        this.fieldName = cfg.getString(FIELD_CONFIG);
        this.targetField = cfg.getString(TARGET_FIELD_CONFIG);
    }

    @Override
    public R apply(R record) {
        Object value = record.value();
        if (value == null) {
            return record;
        }
        if (value instanceof Struct) {
            return applyStruct(record, (Struct) value);
        } else if (value instanceof Map) {
            return applyMap(record, (Map<String, Object>) value);
        }
        return record;
    }

    @SuppressWarnings("unchecked")
    private R applyMap(R record, Map<String, Object> value) {
        Object raw = value.get(fieldName);
        String wkbString = extractWkbString(raw);
        if (wkbString == null) {
            return record;
        }
        double[] latLon = tryDecode(wkbString);
        if (latLon == null) {
            return record;
        }
        Map<String, Object> updated = new HashMap<>(value);
        updated.put(targetField, latLon[0] + "," + latLon[1]);
        updated.remove(fieldName);
        return record.newRecord(record.topic(), record.kafkaPartition(),
                record.keySchema(), record.key(),
                record.valueSchema(), updated, record.timestamp());
    }

    private R applyStruct(R record, Struct value) {
        Object raw = value.get(fieldName);
        String wkbString = extractWkbString(raw);
        if (wkbString == null) {
            return record;
        }
        double[] latLon = tryDecode(wkbString);
        if (latLon == null) {
            return record;
        }
        SchemaBuilder b = SchemaBuilder.struct()
                .name(value.schema().name())
                .version(value.schema().version())
                .doc(value.schema().doc());
        for (Field f : value.schema().fields()) {
            if (!f.name().equals(fieldName)) {
                b.field(f.name(), f.schema());
            }
        }
        b.field(targetField, Schema.OPTIONAL_STRING_SCHEMA);
        Schema updatedSchema = b.build();
        Struct updated = new Struct(updatedSchema);
        for (Field f : value.schema().fields()) {
            if (!f.name().equals(fieldName)) {
                updated.put(f.name(), value.get(f));
            }
        }
        updated.put(targetField, latLon[0] + "," + latLon[1]);
        return record.newRecord(record.topic(), record.kafkaPartition(),
                record.keySchema(), record.key(),
                updatedSchema, updated, record.timestamp());
    }

    private String extractWkbString(Object raw) {
        if (raw == null) {
            return null;
        }
        if (raw instanceof String) {
            return (String) raw;
        }
        if (raw instanceof Map) {
            Object wkb = ((Map<?, ?>) raw).get("wkb");
            return wkb == null ? null : String.valueOf(wkb);
        }
        return null;
    }

    private double[] tryDecode(String wkbString) {
        byte[] wkb = decode(wkbString);
        try {
            Geometry geom = new WKBReader().read(wkb);
            double lat = geom.getCoordinate().y;
            double lon = geom.getCoordinate().x;
            return new double[]{lat, lon};
        } catch (Exception e) {
            return null;
        }
    }

    private byte[] decode(String wkbString) {
        try {
            int len = wkbString.length();
            byte[] data = new byte[len / 2];
            for (int i = 0; i < len; i += 2) {
                data[i / 2] = (byte) ((Character.digit(wkbString.charAt(i), 16) << 4)
                        + Character.digit(wkbString.charAt(i + 1), 16));
            }
            return data;
        } catch (Exception e) {
            return Base64.getDecoder().decode(wkbString);
        }
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        // no-op
    }
}
