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
import java.util.HashMap;
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

    private static final String ID_FIELD = "id";
    private static final String MAPPING_VERSION_FIELD = "_mapping_version";
    // Map-entry wrap keys (non-compact mode): each unrooted Map value is rendered as
    // a {"key":..., "value":...} object so the dotted-prefix layout stays unambiguous.
    private static final String MAP_ENTRY_KEY = "key";
    private static final String MAP_ENTRY_VALUE = "value";
    // Solr atomic-update modifier — see Solr docs "Atomic Updates".
    private static final String ATOMIC_SET = "set";

    private final IdStrategy idStrategy;
    private final WriteMethod writeMethod;
    private final boolean keyIgnoreGlobal;
    private final boolean compactMapEntries;
    private final String idField;
    private final String[] idFieldPath;
    private final String mappingVersion;
    // When false, numeric Kafka keys / extracted ids are passed to Solr as primitives instead
    // of being String-ified — requires the Solr id field to be declared as a numeric type.
    private final boolean idCoerceToString;
    // Enabled when bulk.size.bytes > 0; accumulates an approximate byte estimate during
    // conversion so SolrBulkProcessor can skip its second-pass estimateBytes(doc) walk.
    private final boolean bulkSizeTracking;

    // Identity-keyed: CDC pipelines reuse Schema instances, so reference equality is enough
    // and skips Schema.equals/hashCode per record. Inner map keys are the field-name prefix —
    // top-level uses "", nested STRUCTs use the dotted parent path. Eager recursive build
    // captures nested plans inside their parent's encoder closure so the common case never
    // hits this map at runtime; the cache only serves schemaless nested calls and the
    // schema-substitution fallback.
    private final Map<Schema, Map<String, FieldEncoder[]>> planCache = new IdentityHashMap<>();

    // Caches the recursive leaf-field count per top-level STRUCT schema so the initial
    // SolrInputDocument LinkedHashMap is sized once correctly for nested schemas — without
    // this, a STRUCT containing nested STRUCTs would only see the top-level field count
    // and the backing map would resize partway through population.
    private final Map<Schema, Integer> leafCountCache = new IdentityHashMap<>();

    // Per-record byte accumulator; reset on every convert() entry. Safe under the single-thread
    // task contract Kafka Connect enforces (same as the other instance state in this class).
    private long convertedBytes;

    public SolrRecordConverter(SolrSinkConfig config) {
        this.idStrategy = config.idStrategy();
        this.writeMethod = config.writeMethod();
        this.keyIgnoreGlobal = config.keyIgnore();
        this.compactMapEntries = config.compactMapEntries();
        this.idField = config.idField();          // ConfigDef default "id" — never null
        this.idFieldPath = this.idField.split("\\.");
        this.mappingVersion = config.mappingVersion(); // ConfigDef default "" — never null
        this.idCoerceToString = config.idCoerceToString();
        this.bulkSizeTracking = config.bulkSizeBytes() > 0;
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
        // Capacity must account for the 0.75 load factor or the (hint)-th put will trigger
        // a rehash. `hint * 4 / 3 + 1` keeps the resulting tableSizeFor() threshold > hint.
        SolrInputDocument doc = new SolrInputDocument(new LinkedHashMap<>(hint * 4 / 3 + 1));
        if (bulkSizeTracking) convertedBytes = 16;
        addField(doc, ID_FIELD, deriveId(record, keyIgnored));
        if (!mappingVersion.isEmpty()) {
            addField(doc, MAPPING_VERSION_FIELD, mappingVersion);
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

    /**
     * Byte estimate of the most recent {@link #convert} call, or {@code -1} when
     * {@code bulk.size.bytes} is unset / non-positive (no tracking performed).
     * Approximate — slightly under-counts atomic-update wrapping overhead; close enough
     * for bulk-size threshold purposes.
     */
    public long lastConversionByteEstimate() {
        return bulkSizeTracking ? convertedBytes : -1L;
    }

    /**
     * Single point of entry for adding a field to the document so that the byte accumulator
     * sees every value. When tracking is off this is a thin {@code doc.addField} wrapper that
     * JIT inlines away.
     */
    void addField(SolrInputDocument doc, String name, Object value) {
        doc.addField(name, value);
        if (!bulkSizeTracking) return;
        convertedBytes += (long) name.length() * 2L;
        if (value == null) return;
        if (value instanceof CharSequence) {
            convertedBytes += (long) ((CharSequence) value).length() * 2L;
        } else if (value instanceof Number || value instanceof Boolean) {
            convertedBytes += 16L;
        } else {
            convertedBytes += 32L;
        }
    }

    private int estimateFieldHint(Object value, Schema schema) {
        int base = mappingVersion.isEmpty() ? 1 : 2;
        if (schema != null && schema.type() == Schema.Type.STRUCT) {
            return base + leafFieldCount(schema);
        }
        if (value instanceof Map) {
            return base + ((Map<?, ?>) value).size();
        }
        return base + 8;
    }

    /**
     * Recursive count of leaf fields produced by a STRUCT schema when flattened to dotted
     * Solr field names. A nested {@code customer{name, address{city,state,zip}}} schema
     * has 2 top-level fields but produces 4 leaves — sizing the doc's backing map by
     * top-level count alone would trigger a rehash on the 4th put.
     *
     * <p>Cached per Schema identity since CDC pipelines reuse the same Schema instance
     * across millions of records.</p>
     */
    private int leafFieldCount(Schema schema) {
        Integer cached = leafCountCache.get(schema);
        if (cached != null) return cached;
        int n = computeLeafCount(schema);
        leafCountCache.put(schema, n);
        return n;
    }

    private static int computeLeafCount(Schema schema) {
        if (schema.type() != Schema.Type.STRUCT) return 1;
        int n = 0;
        for (Field field : schema.fields()) {
            n += computeLeafCount(field.schema());
        }
        return n;
    }

    public Object deriveId(SinkRecord record) {
        return deriveId(record, keyIgnoreGlobal);
    }

    public Object deriveId(SinkRecord record, boolean keyIgnored) {
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
                if (key instanceof String) return key;
                return idCoerceToString ? String.valueOf(key) : key;
            case RECORD_FIELD:
                Object id = extractField(record.value(), idFieldPath);
                if (id == null) {
                    throw new DataException("id.strategy=RECORD_FIELD but field '" + idField + "' is null");
                }
                if (id instanceof String) return id;
                return idCoerceToString ? String.valueOf(id) : id;
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

    void populateFromStruct(SolrInputDocument doc, Struct struct, Schema schema, String prefix) {
        if (struct == null || schema == null) {
            return;
        }
        FieldEncoder[] plan = getOrBuildPlan(schema, prefix);
        for (int i = 0; i < plan.length; i++) {
            plan[i].encode(this, doc, struct);
        }
    }

    /**
     * Returns the cached plan for {@code (schema, prefix)} or builds + caches one.
     * Used by:
     *   - the top-level convert() path (prefix=""), which hits once per schema;
     *   - the schemaless STRUCT-in-Map path in addScalar (prefix=parent name);
     *   - the rare runtime-schema-substitution fallback in the STRUCT encoder.
     *
     * The common nested case is served by eagerly built plans captured in the STRUCT
     * encoder closure — that path bypasses this map entirely.
     */
    FieldEncoder[] getOrBuildPlan(Schema schema, String prefix) {
        Map<String, FieldEncoder[]> byPrefix = planCache.get(schema);
        if (byPrefix == null) {
            byPrefix = new HashMap<>(2);
            planCache.put(schema, byPrefix);
        }
        FieldEncoder[] plan = byPrefix.get(prefix);
        if (plan == null) {
            plan = buildPlan(schema, prefix);
            byPrefix.put(prefix, plan);
        }
        return plan;
    }

    // Top-level fields the converter sets itself; a value's own copy must not be re-emitted.
    private boolean isReservedTopLevelField(String name) {
        return ID_FIELD.equals(name)
                || (!mappingVersion.isEmpty() && MAPPING_VERSION_FIELD.equals(name));
    }

    private FieldEncoder[] buildPlan(Schema schema, String prefix) {
        List<Field> fields = schema.fields();
        if (fields.isEmpty()) {
            return EMPTY_PLAN;
        }
        boolean top = prefix.isEmpty();
        List<FieldEncoder> plan = new java.util.ArrayList<>(fields.size());
        for (int i = 0, n = fields.size(); i < n; i++) {
            Field field = fields.get(i);
            // Skip connector-managed top-level fields (id, _mapping_version): they are set by the
            // converter (deriveId / mapping.version), so re-emitting the value's copy would produce
            // two values and Solr rejects the whole doc ("multiple values for uniqueKey").
            if (top && isReservedTopLevelField(field.name())) {
                continue;
            }
            String name = top ? field.name() : prefix + "." + field.name();
            plan.add(encoderFor(field, name));
        }
        return plan.toArray(EMPTY_PLAN);
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
                        if (v != null) conv.addField(doc, name, ((java.util.Date) v).toInstant().toString());
                    };
                case Decimal.LOGICAL_NAME:
                    return (conv, doc, src) -> {
                        Object v = src.get(field);
                        if (v != null) conv.addField(doc, name, v.toString());
                    };
                default:
                    break;
            }
        }
        switch (sub.type()) {
            case STRUCT:
                // Eagerly recurse: build (and cache) the nested plan now so the runtime path
                // can invoke it directly without a per-record getOrBuildPlan lookup. Falls back
                // to a runtime lookup only when the inbound Struct carries a different Schema
                // instance than the declared one (rare; covered by getOrBuildPlan's cache).
                final Schema declaredNested = sub;
                final FieldEncoder[] nestedPlan = getOrBuildPlan(declaredNested, name);
                return (conv, doc, src) -> {
                    Struct nested = (Struct) src.get(field);
                    if (nested == null) return;
                    FieldEncoder[] plan = nested.schema() == declaredNested
                            ? nestedPlan
                            : conv.getOrBuildPlan(nested.schema(), name);
                    for (int i = 0; i < plan.length; i++) plan[i].encode(conv, doc, nested);
                };
            case ARRAY:
                Schema elementSchema = sub.valueSchema();
                ScalarEncoder elementEncoder = scalarEncoderFor(elementSchema);
                return (conv, doc, src) -> {
                    Object v = src.get(field);
                    // Connect ARRAY values are always List (Struct.validate enforces it).
                    if (v instanceof List) {
                        List<?> list = (List<?>) v;
                        for (int i = 0, n = list.size(); i < n; i++) {
                            Object item = list.get(i);
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
                    if (v instanceof byte[]) conv.addField(doc, name, BASE64.encodeToString((byte[]) v));
                    else if (v instanceof ByteBuffer) conv.addField(doc, name, BASE64.encodeToString(((ByteBuffer) v).array()));
                };
            case STRING:
            case INT8: case INT16: case INT32: case INT64:
            case FLOAT32: case FLOAT64:
            case BOOLEAN:
            default:
                return (conv, doc, src) -> {
                    Object v = src.get(field);
                    if (v != null) conv.addField(doc, name, v);
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
                    addField(doc, name, ((java.util.Date) value).toInstant().toString());
                    return;
                case Decimal.LOGICAL_NAME:
                    addField(doc, name, value.toString());
                    return;
                default:
                    break;
            }
        }
        if (value instanceof java.util.Collection) {
            // Schemaless array: flatten per element. Arrays of objects (e.g. promises,
            // custom_fields from the kstreams doc) become multi-valued dotted fields
            // (name.subfield); Solr can't index a raw Map value — it would misread it as an
            // atomic-update op. Scalars just become a multi-valued field. Mirrors the schema'd
            // struct-array path.
            for (Object item : (java.util.Collection<?>) value) {
                if (item != null) addScalar(doc, name, item, null);
            }
            return;
        }
        if (value instanceof Struct) {
            Struct s = (Struct) value;
            populateFromStruct(doc, s, s.schema(), name);
        } else if (value instanceof Map) {
            populateFromMap(doc, (Map<?, ?>) value, name);
        } else if (value instanceof byte[]) {
            addField(doc, name, BASE64.encodeToString((byte[]) value));
        } else if (value instanceof ByteBuffer) {
            addField(doc, name, BASE64.encodeToString(((ByteBuffer) value).array()));
        } else if (value instanceof java.util.Date) {
            addField(doc, name, ((java.util.Date) value).toInstant().toString());
        } else {
            addField(doc, name, value);
        }
    }

    void populateFromMap(SolrInputDocument doc, Map<?, ?> map, String prefix) {
        boolean top = prefix.isEmpty();
        if (compactMapEntries) {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                Object key = entry.getKey();
                String keyStr = key instanceof String ? (String) key : String.valueOf(key);
                // Skip connector-managed top-level fields (id / _mapping_version) — set by the converter.
                if (top && isReservedTopLevelField(keyStr)) {
                    continue;
                }
                String name = top ? keyStr : prefix + "." + keyStr;
                addScalar(doc, name, entry.getValue(), null);
            }
            return;
        }
        if (top) {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                Object key = entry.getKey();
                String keyStr = key instanceof String ? (String) key : String.valueOf(key);
                if (isReservedTopLevelField(keyStr)) {
                    continue;
                }
                addScalar(doc, keyStr, entry.getValue(), null);
            }
            return;
        }
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            // initialCapacity=4 avoids the resize that LinkedHashMap(2) would trigger
            // on the second put (loadFactor 0.75 × 2 = threshold 1.5 < 2).
            LinkedHashMap<String, Object> pair = new LinkedHashMap<>(4);
            pair.put(MAP_ENTRY_KEY, entry.getKey());
            pair.put(MAP_ENTRY_VALUE, entry.getValue());
            addField(doc, prefix, pair);
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
            if (ID_FIELD.equals(name)) continue;
            Object original = f.getValue();
            if (original == null) continue;
            doc.setField(name, Map.of(ATOMIC_SET, original));
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

    // The caller only ever passes an ARRAY's element schema, which is never null.
    private static ScalarEncoder scalarEncoderFor(Schema schema) {
        if (schema.name() != null) {
            switch (schema.name()) {
                case Timestamp.LOGICAL_NAME:
                case Date.LOGICAL_NAME:
                case Time.LOGICAL_NAME:
                    return (conv, doc, name, value) -> conv.addField(doc, name, ((java.util.Date) value).toInstant().toString());
                case Decimal.LOGICAL_NAME:
                    return (conv, doc, name, value) -> conv.addField(doc, name, value.toString());
                default:
                    break;
            }
        }
        switch (schema.type()) {
            case STRUCT:
                return (conv, doc, name, value) -> conv.populateFromStruct(doc, (Struct) value, ((Struct) value).schema(), name);
            case MAP:
                return (conv, doc, name, value) -> conv.populateFromMap(doc, (Map<?, ?>) value, name);
            case BYTES:
                return (conv, doc, name, value) -> {
                    if (value instanceof byte[]) conv.addField(doc, name, BASE64.encodeToString((byte[]) value));
                    else if (value instanceof ByteBuffer) conv.addField(doc, name, BASE64.encodeToString(((ByteBuffer) value).array()));
                };
            default:
                return (conv, doc, name, value) -> conv.addField(doc, name, value);
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
