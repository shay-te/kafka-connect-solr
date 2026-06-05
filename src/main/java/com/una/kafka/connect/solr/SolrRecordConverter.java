package com.una.kafka.connect.solr;

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
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Converts a Kafka Connect record into a SolrInputDocument. Field-type
 * mapping mirrors the Elasticsearch DataConverter:
 *   STRING        -> string
 *   INT8/16/32    -> pint
 *   INT64         -> plong
 *   FLOAT32       -> pfloat
 *   FLOAT64       -> pdouble
 *   BOOLEAN       -> boolean
 *   Timestamp     -> pdate (ISO-8601)
 *   Decimal       -> string (preserves precision)
 *   BYTES         -> base64 string
 *   STRUCT/MAP    -> dotted field paths
 *   ARRAY         -> multi-valued field
 */
public class SolrRecordConverter {

    private final SolrSinkConfig config;

    public SolrRecordConverter(SolrSinkConfig config) {
        this.config = config;
    }

    public SolrInputDocument convert(SinkRecord record) {
        if (record.value() == null) {
            return null;
        }

        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", deriveId(record));

        Object value = record.value();
        Schema schema = record.valueSchema();
        if (value instanceof Struct) {
            populateFromStruct(doc, (Struct) value, schema, "");
        } else if (value instanceof Map) {
            populateFromMap(doc, (Map<?, ?>) value, "");
        } else {
            throw new DataException("Unsupported value type: " + value.getClass().getName());
        }

        if (config.writeMethod() == WriteMethod.ATOMIC_UPDATE) {
            applyAtomicUpdate(doc);
        }
        return doc;
    }

    public String deriveId(SinkRecord record) {
        switch (config.idStrategy()) {
            case KAFKA_KEY:
                if (record.key() == null) {
                    throw new DataException("id.strategy=KAFKA_KEY but record key is null");
                }
                return String.valueOf(record.key());
            case RECORD_FIELD:
                Object id = extractField(record.value(), config.idField());
                if (id == null) {
                    throw new DataException("id.strategy=RECORD_FIELD but field '"
                            + config.idField() + "' is null");
                }
                return String.valueOf(id);
            case TOPIC_PARTITION_OFFSET:
                return record.topic() + "-" + record.kafkaPartition() + "-" + record.kafkaOffset();
            case UUID:
                return UUID.randomUUID().toString();
            default:
                throw new DataException("Unknown id strategy " + config.idStrategy());
        }
    }

    private void populateFromStruct(SolrInputDocument doc, Struct struct, Schema schema, String prefix) {
        if (struct == null || schema == null) {
            return;
        }
        for (Field field : schema.fields()) {
            Object v = struct.get(field);
            String name = prefix.isEmpty() ? field.name() : prefix + "." + field.name();
            addField(doc, name, v, field.schema());
        }
    }

    private void populateFromMap(SolrInputDocument doc, Map<?, ?> map, String prefix) {
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            String name = prefix.isEmpty() ? String.valueOf(entry.getKey())
                    : prefix + "." + entry.getKey();
            addField(doc, name, entry.getValue(), null);
        }
    }

    private void addField(SolrInputDocument doc, String name, Object value, Schema schema) {
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
        } else if (value instanceof Collection) {
            for (Object item : (Collection<?>) value) {
                addField(doc, name, item, schema != null ? schema.valueSchema() : null);
            }
        } else if (value instanceof byte[]) {
            doc.addField(name, Base64.getEncoder().encodeToString((byte[]) value));
        } else if (value instanceof ByteBuffer) {
            doc.addField(name, Base64.getEncoder().encodeToString(((ByteBuffer) value).array()));
        } else if (value instanceof java.util.Date) {
            doc.addField(name, ((java.util.Date) value).toInstant().toString());
        } else {
            doc.addField(name, value);
        }
    }

    private void applyAtomicUpdate(SolrInputDocument doc) {
        for (String name : doc.getFieldNames()) {
            if ("id".equals(name)) {
                continue;
            }
            Object original = doc.getFieldValue(name);
            Map<String, Object> setOp = new HashMap<>();
            setOp.put("set", original);
            doc.setField(name, setOp);
        }
    }

    private Object extractField(Object value, String fieldPath) {
        if (value == null || fieldPath == null) {
            return null;
        }
        String[] parts = fieldPath.split("\\.");
        Object cur = value;
        for (String p : parts) {
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
}
