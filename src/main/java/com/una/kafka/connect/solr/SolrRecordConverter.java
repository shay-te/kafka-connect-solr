package com.una.kafka.connect.solr;

import com.una.kafka.connect.solr.SolrSinkConfig.IdStrategy;
import com.una.kafka.connect.solr.SolrSinkConfig.WriteMethod;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.common.SolrInputDocument;

import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public final class SolrRecordConverter {

    private static final Base64.Encoder BASE64 = Base64.getEncoder();
    private static final FieldEncoder[] EMPTY_PLAN = new FieldEncoder[0];
    private static final ThreadLocal<StringBuilder> TPO_BUILDER =
            ThreadLocal.withInitial(() -> new StringBuilder(64));
    private static final ThreadLocal<java.util.ArrayList<org.apache.solr.common.SolrInputField>> AU_SNAPSHOT =
            ThreadLocal.withInitial(java.util.ArrayList::new);

    private final IdStrategy idStrategy;
    private final WriteMethod writeMethod;
    private final boolean keyIgnoreGlobal;
    private final boolean compactMapEntries;
    private final String idField;
    private final String[] idFieldPath;
    private final String mappingVersion;

    // Identity-keyed: CDC pipelines reuse Schema instances, so reference equality is enough
    // and skips Schema.equals/hashCode per record.
    private final Map<Schema, FieldEncoder[]> planCache = new IdentityHashMap<>();

    public SolrRecordConverter(SolrSinkConfig config) {
        this.idStrategy = config.idStrategy();
        this.writeMethod = config.writeMethod();
        this.keyIgnoreGlobal = config.keyIgnore();
        this.compactMapEntries = config.compactMapEntries();
        this.idField = config.idField();
        this.idFieldPath = this.idField == null ? new String[0] : this.idField.split("\\.");
        this.mappingVersion = config.mappingVersion() == null ? "" : config.mappingVersion();
    }

    public SolrInputDocument convert(SinkRecord record) {
        return convert(record, keyIgnoreGlobal);
    }

    public SolrInputDocument convert(SinkRecord record, boolean keyIgnored) {
        Object value = record.value();
        if (value == null) {
            return null;
        }
        Schema schema = record.valueSchema();
        int hint = estimateFieldHint(value, schema);
        SolrInputDocument doc = new SolrInputDocument(new LinkedHashMap<>(hint));
        doc.addField("id", deriveId(record, keyIgnored));
        if (!mappingVersion.isEmpty()) {
            doc.addField("_mapping_version", mappingVersion);
        }

        if (value instanceof Struct) {
            populateFromStruct(doc, (Struct) value, schema, "");
        } else if (value instanceof Map) {
            populateFromMap(doc, (Map<?, ?>) value, "");
        } else {
            throw new DataException("Unsupported value type: " + value.getClass().getName());
        }

        if (writeMethod == WriteMethod.ATOMIC_UPDATE) {
            applyAtomicUpdate(doc);
        }
        return doc;
    }

    private int estimateFieldHint(Object value, Schema schema) {
        int base = mappingVersion.isEmpty() ? 1 : 2;
        if (schema != null && schema.type() == Schema.Type.STRUCT) {
            return base + schema.fields().size();
        }
        if (value instanceof Map) {
            return base + ((Map<?, ?>) value).size();
        }
        return base + 8;
    }

    public String deriveId(SinkRecord record) {
        return deriveId(record, keyIgnoreGlobal);
    }

    public String deriveId(SinkRecord record, boolean keyIgnored) {
        IdStrategy strategy = idStrategy;
        if (keyIgnored && strategy == IdStrategy.KAFKA_KEY) {
            strategy = IdStrategy.TOPIC_PARTITION_OFFSET;
        }
        switch (strategy) {
            case KAFKA_KEY:
                Object key = record.key();
                if (key == null) {
                    throw new DataException("id.strategy=KAFKA_KEY but record key is null");
                }
                return key instanceof String ? (String) key : String.valueOf(key);
            case RECORD_FIELD:
                Object id = extractField(record.value(), idFieldPath);
                if (id == null) {
                    throw new DataException("id.strategy=RECORD_FIELD but field '" + idField + "' is null");
                }
                return id instanceof String ? (String) id : String.valueOf(id);
            case TOPIC_PARTITION_OFFSET: {
                StringBuilder sb = TPO_BUILDER.get();
                sb.setLength(0);
                return sb.append(record.topic()).append('-')
                        .append(record.kafkaPartition()).append('-')
                        .append(record.kafkaOffset())
                        .toString();
            }
            case UUID:
                return UUID.randomUUID().toString();
            default:
                throw new DataException("Unknown id strategy " + strategy);
        }
    }

    private void populateFromStruct(SolrInputDocument doc, Struct struct, Schema schema, String prefix) {
        if (struct == null || schema == null) {
            return;
        }
        FieldEncoder[] plan = planFor(schema, prefix);
        for (int i = 0; i < plan.length; i++) {
            plan[i].encode(this, doc, struct);
        }
    }

    private FieldEncoder[] planFor(Schema schema, String prefix) {
        // Nested plans are prefix-sensitive; only cache the top-level prefix="" case.
        if (!prefix.isEmpty()) {
            return buildPlan(schema, prefix);
        }
        FieldEncoder[] cached = planCache.get(schema);
        if (cached == null) {
            cached = buildPlan(schema, prefix);
            planCache.put(schema, cached);
        }
        return cached;
    }

    private FieldEncoder[] buildPlan(Schema schema, String prefix) {
        List<Field> fields = schema.fields();
        if (fields.isEmpty()) {
            return EMPTY_PLAN;
        }
        FieldEncoder[] plan = new FieldEncoder[fields.size()];
        for (int i = 0, n = fields.size(); i < n; i++) {
            Field field = fields.get(i);
            String name = prefix.isEmpty() ? field.name() : prefix + "." + field.name();
            plan[i] = encoderFor(field, name);
        }
        return plan;
    }

    private FieldEncoder encoderFor(Field field, String name) {
        Schema sub = field.schema();
        if (sub.name() != null) {
            switch (sub.name()) {
                case Timestamp.LOGICAL_NAME:
                case Date.LOGICAL_NAME:
                case Time.LOGICAL_NAME:
                    return (conv, doc, src) -> {
                        Object v = src.get(field);
                        if (v != null) doc.addField(name, ((java.util.Date) v).toInstant().toString());
                    };
                case Decimal.LOGICAL_NAME:
                    return (conv, doc, src) -> {
                        Object v = src.get(field);
                        if (v != null) doc.addField(name, v.toString());
                    };
                default:
                    break;
            }
        }
        switch (sub.type()) {
            case STRUCT:
                return (conv, doc, src) -> {
                    Struct nested = (Struct) src.get(field);
                    if (nested != null) conv.populateFromStruct(doc, nested, nested.schema(), name);
                };
            case ARRAY:
                Schema elementSchema = sub.valueSchema();
                ScalarEncoder elementEncoder = scalarEncoderFor(elementSchema);
                return (conv, doc, src) -> {
                    Object v = src.get(field);
                    if (v instanceof List) {
                        List<?> list = (List<?>) v;
                        for (int i = 0, n = list.size(); i < n; i++) {
                            Object item = list.get(i);
                            if (item != null) elementEncoder.write(conv, doc, name, item);
                        }
                    } else if (v instanceof Collection) {
                        for (Object item : (Collection<?>) v) {
                            if (item != null) elementEncoder.write(conv, doc, name, item);
                        }
                    }
                };
            case MAP:
                return (conv, doc, src) -> {
                    Object v = src.get(field);
                    if (v instanceof Map) conv.populateFromMap(doc, (Map<?, ?>) v, name);
                };
            case BYTES:
                return (conv, doc, src) -> {
                    Object v = src.get(field);
                    if (v instanceof byte[]) doc.addField(name, BASE64.encodeToString((byte[]) v));
                    else if (v instanceof ByteBuffer) doc.addField(name, BASE64.encodeToString(((ByteBuffer) v).array()));
                };
            case STRING:
            case INT8: case INT16: case INT32: case INT64:
            case FLOAT32: case FLOAT64:
            case BOOLEAN:
            default:
                return (conv, doc, src) -> {
                    Object v = src.get(field);
                    if (v != null) doc.addField(name, v);
                };
        }
    }

    void addScalar(SolrInputDocument doc, String name, Object value, Schema schema) {
        if (value == null) {
            return;
        }
        if (schema != null && schema.name() != null) {
            switch (schema.name()) {
                case Timestamp.LOGICAL_NAME:
                case Date.LOGICAL_NAME:
                case Time.LOGICAL_NAME:
                    doc.addField(name, ((java.util.Date) value).toInstant().toString());
                    return;
                case Decimal.LOGICAL_NAME:
                    doc.addField(name, value.toString());
                    return;
                default:
                    break;
            }
        }
        if (value instanceof Struct) {
            Struct s = (Struct) value;
            populateFromStruct(doc, s, s.schema(), name);
        } else if (value instanceof Map) {
            populateFromMap(doc, (Map<?, ?>) value, name);
        } else if (value instanceof byte[]) {
            doc.addField(name, BASE64.encodeToString((byte[]) value));
        } else if (value instanceof ByteBuffer) {
            doc.addField(name, BASE64.encodeToString(((ByteBuffer) value).array()));
        } else if (value instanceof java.util.Date) {
            doc.addField(name, ((java.util.Date) value).toInstant().toString());
        } else {
            doc.addField(name, value);
        }
    }

    private void populateFromMap(SolrInputDocument doc, Map<?, ?> map, String prefix) {
        if (compactMapEntries) {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                Object key = entry.getKey();
                String keyStr = key instanceof String ? (String) key : String.valueOf(key);
                String name = prefix.isEmpty() ? keyStr : prefix + "." + keyStr;
                addScalar(doc, name, entry.getValue(), null);
            }
            return;
        }
        if (prefix.isEmpty()) {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                Object key = entry.getKey();
                addScalar(doc, key instanceof String ? (String) key : String.valueOf(key),
                        entry.getValue(), null);
            }
            return;
        }
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            Map<String, Object> pair = new LinkedHashMap<>(2);
            pair.put("key", entry.getKey());
            pair.put("value", entry.getValue());
            doc.addField(prefix, pair);
        }
    }

    private void applyAtomicUpdate(SolrInputDocument doc) {
        // Snapshot before mutating: setField on the fields we're iterating triggers ConcurrentModification.
        java.util.ArrayList<org.apache.solr.common.SolrInputField> snap = AU_SNAPSHOT.get();
        snap.clear();
        for (org.apache.solr.common.SolrInputField f : doc) snap.add(f);
        for (int i = 0, n = snap.size(); i < n; i++) {
            org.apache.solr.common.SolrInputField f = snap.get(i);
            String name = f.getName();
            if ("id".equals(name)) continue;
            Object original = f.getValue();
            if (original == null) continue;
            doc.setField(name, Map.of("set", original));
        }
        snap.clear();
    }

    private static Object extractField(Object value, String[] path) {
        if (value == null || path.length == 0) {
            return null;
        }
        Object cur = value;
        for (String p : path) {
            if (cur == null) {
                return null;
            } else if (cur instanceof Struct) {
                cur = ((Struct) cur).get(p);
            } else if (cur instanceof Map) {
                cur = ((Map<?, ?>) cur).get(p);
            } else {
                return null;
            }
        }
        return cur;
    }

    private static ScalarEncoder scalarEncoderFor(Schema schema) {
        if (schema != null && schema.name() != null) {
            switch (schema.name()) {
                case Timestamp.LOGICAL_NAME:
                case Date.LOGICAL_NAME:
                case Time.LOGICAL_NAME:
                    return (conv, doc, name, value) -> doc.addField(name, ((java.util.Date) value).toInstant().toString());
                case Decimal.LOGICAL_NAME:
                    return (conv, doc, name, value) -> doc.addField(name, value.toString());
                default:
                    break;
            }
        }
        if (schema == null) {
            return SolrRecordConverter::addScalarUntyped;
        }
        switch (schema.type()) {
            case STRUCT:
                return (conv, doc, name, value) -> conv.populateFromStruct(doc, (Struct) value, ((Struct) value).schema(), name);
            case MAP:
                return (conv, doc, name, value) -> conv.populateFromMap(doc, (Map<?, ?>) value, name);
            case BYTES:
                return (conv, doc, name, value) -> {
                    if (value instanceof byte[]) doc.addField(name, BASE64.encodeToString((byte[]) value));
                    else if (value instanceof ByteBuffer) doc.addField(name, BASE64.encodeToString(((ByteBuffer) value).array()));
                };
            default:
                return (conv, doc, name, value) -> doc.addField(name, value);
        }
    }

    private static void addScalarUntyped(SolrRecordConverter conv, SolrInputDocument doc, String name, Object value) {
        if (value instanceof Struct) {
            Struct s = (Struct) value;
            conv.populateFromStruct(doc, s, s.schema(), name);
        } else if (value instanceof Map) {
            conv.populateFromMap(doc, (Map<?, ?>) value, name);
        } else if (value instanceof byte[]) {
            doc.addField(name, BASE64.encodeToString((byte[]) value));
        } else if (value instanceof ByteBuffer) {
            doc.addField(name, BASE64.encodeToString(((ByteBuffer) value).array()));
        } else if (value instanceof java.util.Date) {
            doc.addField(name, ((java.util.Date) value).toInstant().toString());
        } else {
            doc.addField(name, value);
        }
    }

    @FunctionalInterface
    interface FieldEncoder {
        void encode(SolrRecordConverter conv, SolrInputDocument doc, Struct src);
    }

    @FunctionalInterface
    interface ScalarEncoder {
        void write(SolrRecordConverter conv, SolrInputDocument doc, String name, Object value);
    }
}
