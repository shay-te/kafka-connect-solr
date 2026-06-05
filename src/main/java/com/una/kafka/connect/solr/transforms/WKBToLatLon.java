package com.una.kafka.connect.solr.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKBReader;

import java.util.Base64;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

public class WKBToLatLon<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final String FIELD_CONFIG = "field";
    private static final String TARGET_FIELD_CONFIG = "target.field";

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(FIELD_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                    "The field name containing WKB data.")
            .define(TARGET_FIELD_CONFIG, ConfigDef.Type.STRING, "location", ConfigDef.Importance.LOW,
                    "The field name to write the lat,lon string into.");

    // WKBReader is stateful and not thread-safe; one per Connect task thread.
    private static final ThreadLocal<WKBReader> WKB_READER =
            ThreadLocal.withInitial(WKBReader::new);
    private static final ThreadLocal<StringBuilder> LATLON_BUILDER =
            ThreadLocal.withInitial(() -> new StringBuilder(48));

    private String fieldName;
    private String targetField;

    // CDC pipelines reuse Schema instances; cache the rewritten output schema by identity.
    private final Map<Schema, Schema> schemaCache = new IdentityHashMap<>();

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
            @SuppressWarnings("unchecked")
            Map<String, Object> m = (Map<String, Object>) value;
            return applyMap(record, m);
        }
        return record;
    }

    private R applyMap(R record, Map<String, Object> value) {
        Object raw = value.get(fieldName);
        String wkbString = extractWkbString(raw);
        if (wkbString == null) {
            return record;
        }
        String latLon = decodeLatLon(wkbString);
        if (latLon == null) {
            return record;
        }
        Map<String, Object> updated = new HashMap<>(value);
        updated.put(targetField, latLon);
        updated.remove(fieldName);
        return record.newRecord(record.topic(), record.kafkaPartition(),
                record.keySchema(), record.key(),
                record.valueSchema(), updated, record.timestamp());
    }

    private R applyStruct(R record, Struct value) {
        Schema inSchema = value.schema();
        Field srcField = inSchema.field(fieldName);
        if (srcField == null) {
            return record;
        }
        Object raw = value.get(srcField);
        String wkbString = extractWkbString(raw);
        if (wkbString == null) {
            return record;
        }
        String latLon = decodeLatLon(wkbString);
        if (latLon == null) {
            return record;
        }
        Schema outSchema = schemaCache.get(inSchema);
        List<Field> inFields = inSchema.fields();
        if (outSchema == null) {
            SchemaBuilder b = SchemaBuilder.struct()
                    .name(inSchema.name())
                    .version(inSchema.version())
                    .doc(inSchema.doc());
            for (int i = 0, n = inFields.size(); i < n; i++) {
                Field f = inFields.get(i);
                if (!f.name().equals(fieldName)) {
                    b.field(f.name(), f.schema());
                }
            }
            b.field(targetField, Schema.OPTIONAL_STRING_SCHEMA);
            outSchema = b.build();
            schemaCache.put(inSchema, outSchema);
        }
        Struct updated = new Struct(outSchema);
        for (int i = 0, n = inFields.size(); i < n; i++) {
            Field f = inFields.get(i);
            if (!f.name().equals(fieldName)) {
                updated.put(f.name(), value.get(f));
            }
        }
        updated.put(targetField, latLon);
        return record.newRecord(record.topic(), record.kafkaPartition(),
                record.keySchema(), record.key(),
                outSchema, updated, record.timestamp());
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
            return wkb == null ? null : wkb.toString();
        }
        return null;
    }

    private String decodeLatLon(String wkbString) {
        byte[] wkb = decode(wkbString);
        if (wkb == null) {
            return null;
        }
        try {
            Geometry geom = WKB_READER.get().read(wkb);
            Coordinate c = geom.getCoordinate();
            StringBuilder sb = LATLON_BUILDER.get();
            sb.setLength(0);
            return sb.append(c.y).append(',').append(c.x).toString();
        } catch (Exception e) {
            return null;
        }
    }

    private byte[] decode(String wkbString) {
        if (isHex(wkbString)) {
            int len = wkbString.length();
            byte[] data = new byte[len / 2];
            for (int i = 0; i < len; i += 2) {
                int hi = Character.digit(wkbString.charAt(i), 16);
                int lo = Character.digit(wkbString.charAt(i + 1), 16);
                if (hi < 0 || lo < 0) return null;
                data[i / 2] = (byte) ((hi << 4) | lo);
            }
            return data;
        }
        try {
            return Base64.getDecoder().decode(wkbString);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static boolean isHex(String s) {
        int n = s.length();
        if (n == 0 || (n & 1) != 0) return false;
        for (int i = 0; i < n; i++) {
            char c = s.charAt(i);
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))) {
                return false;
            }
        }
        return true;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }
}
