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

/**
 * Converts a Kafka Connect record into a SolrInputDocument.
 *
 * <p>Hot-path strategy:</p>
 * <ul>
 *     <li>All config knobs cached in {@code final} fields.</li>
 *     <li>Id-field dot-path pre-split once at construction.</li>
 *     <li>{@code Base64.Encoder} cached statically (thread-safe).</li>
 *     <li><b>Schema-aware encoder plan</b>: for every Struct schema we see,
 *         we build a {@code FieldEncoder[]} once and reuse it for every
 *         record that shares the same schema identity. This skips the
 *         per-field {@code switch (schemaType)} + {@code instanceof}
 *         dispatch on the hot path - the encoder array is just an
 *         indexed virtual call.</li>
 *     <li>{@link IdentityHashMap} keys on schema reference so we don't pay
 *         {@code Schema.equals/hashCode} cost per record. CDC pipelines
 *         reuse Schema instances - this is the common case.</li>
 * </ul>
 */
public final class SolrRecordConverter {

    private static final Base64.Encoder BASE64 = Base64.getEncoder();
    /** Sentinel encoder array for schemas with no fields (shouldn't really happen). */
    private static final FieldEncoder[] EMPTY_PLAN = new FieldEncoder[0];

    private final IdStrategy idStrategy;
    private final WriteMethod writeMethod;
    private final boolean keyIgnoreGlobal;
    private final boolean compactMapEntries;
    private final String idField;
    private final String[] idFieldPath;
    private final String mappingVersion;

    /** Identity-keyed cache: schema reference → precomputed per-field encoders. */
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
        if (record.value() == null) {
            return null;
        }

        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", deriveId(record, keyIgnored));
        if (!mappingVersion.isEmpty()) {
            doc.addField("_mapping_version", mappingVersion);
        }

        Object value = record.value();
        Schema schema = record.valueSchema();
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
            case TOPIC_PARTITION_OFFSET:
                return new StringBuilder(record.topic().length() + 24)
                        .append(record.topic()).append('-')
                        .append(record.kafkaPartition()).append('-')
                        .append(record.kafkaOffset())
                        .toString();
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
        // Plans are prefix-sensitive. The common case is prefix="" (top-level
        // struct) which is what we cache. Nested struct plans are built on
        // demand and not cached because their addresses depend on the parent's
        // prefix - cheaper to rebuild than to maintain a 2-level cache key.
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
        // Logical types take precedence over the primitive type.
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
                return (conv, doc, src) -> {
                    Object v = src.get(field);
                    if (v instanceof Collection) {
                        for (Object item : (Collection<?>) v) {
                            conv.addScalar(doc, name, item, elementSchema);
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

    /** Used by ARRAY encoders for each element; covers the rare element-is-itself-a-Struct case. */
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
        // Snapshot the field names BEFORE mutating - iterating doc.getFieldNames()
        // while calling doc.setField() on those same names worked only by relying
        // on LinkedHashMap not bumping modCount for replacement. Snapshotting is
        // a few bytes more per record and immune to that fragility.
        String[] names = doc.getFieldNames().toArray(new String[0]);
        for (String name : names) {
            if ("id".equals(name)) {
                continue;
            }
            Object original = doc.getFieldValue(name);
            if (original == null) {
                continue;
            }
            // Map.of returns a flyweight Map1 with the entry inlined - one
            // allocation vs HashMap's three (object + Entry[] + Entry node).
            // For ATOMIC_UPDATE workloads with N-field docs at K records/sec
            // this is N*K HashMap allocations eliminated per second.
            doc.setField(name, Map.of("set", original));
        }
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

    /**
     * One pre-bound encoder per Struct field. {@code Field}, target field
     * name and any element schema are captured in the lambda closure at
     * construction so the hot-path call is just an indexed virtual dispatch.
     */
    @FunctionalInterface
    interface FieldEncoder {
        void encode(SolrRecordConverter conv, SolrInputDocument doc, Struct src);
    }
}
