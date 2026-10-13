package com.una.kafka.connect.solr;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public final class SolrSchemaManager {

    private static final Logger log = LoggerFactory.getLogger(SolrSchemaManager.class);

    private final SolrClient client;
    private final boolean autoEvolve;
    private final boolean compactMapEntries;
    private final Map<String, Set<String>> knownFieldsPerCollection = new HashMap<>();
    private final Map<String, Set<Schema>> evolvedSchemasPerCollection = new HashMap<>();

    public SolrSchemaManager(SolrClient client, SolrSinkConfig config) {
        this.client = client;
        this.autoEvolve = config.schemaAutoEvolve();
        this.compactMapEntries = config.compactMapEntries();
    }

    public void evolveIfNeeded(String collection, Schema schema) {
        if (!autoEvolve || schema == null) {
            return;
        }
        Set<Schema> seen = evolvedSchemasPerCollection.get(collection);
        if (seen != null && seen.contains(schema)) {
            return;
        }
        try {
            Set<String> known = knownFieldsPerCollection.computeIfAbsent(collection,
                    k -> loadKnown(collection));
            collectAndAdd(schema, "", false, collection, known);
            if (seen == null) {
                seen = java.util.Collections.newSetFromMap(new IdentityHashMap<>());
                evolvedSchemasPerCollection.put(collection, seen);
            }
            seen.add(schema);
        } catch (Exception e) {
            log.warn("Schema auto-evolve failed for collection {}: {}", collection, e.getMessage());
        }
    }

    private Set<String> loadKnown(String collection) {
        Set<String> set = new HashSet<>();
        try {
            SchemaResponse.FieldsResponse resp = new SchemaRequest.Fields().process(client, collection);
            resp.getFields().forEach(f -> set.add(String.valueOf(f.get("name"))));
            new SchemaRequest.DynamicFields().process(client, collection).getDynamicFields()
                    .forEach(f -> set.add(String.valueOf(f.get("name"))));
        } catch (Exception e) {
            log.warn("Could not read existing schema for {}: {}", collection, e.getMessage());
        }
        return set;
    }

    // Mirrors SolrRecordConverter's flattening: nested structs become dotted names, every array element writes the
    // same names (so they are multi-valued), and a map writes `<map>.<key>` (compact) or `<map>.key`/`<map>.value`.
    private void collectAndAdd(Schema schema, String name, boolean multiValued, String collection, Set<String> known) {
        switch (schema.type()) {
            case STRUCT:
                for (Field field : schema.fields()) {
                    String child = name.isEmpty() ? field.name() : name + "." + field.name();
                    collectAndAdd(field.schema(), child, multiValued, collection, known);
                }
                return;
            case ARRAY:
                collectAndAdd(schema.valueSchema(), name, true, collection, known);
                return;
            case MAP:
                if (name.isEmpty()) {
                    return;
                }
                if (compactMapEntries) {
                    // The keys are data, so every `<map>.<key>` (and anything under it) is one dynamic field.
                    ensureField(collection, name + ".*", mapValueType(schema.valueSchema()), true, true, known);
                } else {
                    collectAndAdd(schema.keySchema(), name + ".key", true, collection, known);
                    collectAndAdd(schema.valueSchema(), name + ".value", true, collection, known);
                }
                return;
            default:
                ensureField(collection, name, solrTypeFor(schema), multiValued, false, known);
        }
    }

    // A map of structs, maps or arrays of them writes several value types under one pattern: string takes them all.
    private static String mapValueType(Schema valueSchema) {
        Schema element = valueSchema.type() == Schema.Type.ARRAY ? valueSchema.valueSchema() : valueSchema;
        Schema.Type type = element.type();
        return type == Schema.Type.STRUCT || type == Schema.Type.MAP || type == Schema.Type.ARRAY
                ? "string" : solrTypeFor(element);
    }

    private void ensureField(String collection, String name, String type, boolean multiValued, boolean dynamic,
                             Set<String> known) {
        if (name.isEmpty() || known.contains(name)) {
            return;
        }
        Map<String, Object> attrs = new LinkedHashMap<>();
        attrs.put("name", name);
        attrs.put("type", type);
        attrs.put("stored", true);
        attrs.put("indexed", true);
        if (multiValued) {
            attrs.put("multiValued", true);
        }
        try {
            if (dynamic) {
                new SchemaRequest.AddDynamicField(attrs).process(client, collection);
            } else {
                new SchemaRequest.AddField(attrs).process(client, collection);
            }
            log.info("Added Solr {}field {} ({}) to collection {}", dynamic ? "dynamic " : "", name, type, collection);
        } catch (Exception e) {
            // Mark as known either way so we don't retry every record. Log at WARN
            // (not DEBUG) so genuine failures - permission denied, schema invalid -
            // are visible; the "field already exists" race noise is one WARN per
            // field per task lifetime, not per record.
            log.warn("Could not add Solr field {} to collection {}: {}", name, collection, e.getMessage());
        }
        known.add(name);
    }

    public static String solrTypeFor(Schema schema) {
        if (schema == null) {
            return "string";
        }
        if (schema.name() != null) {
            switch (schema.name()) {
                case Timestamp.LOGICAL_NAME:
                case Date.LOGICAL_NAME:
                case Time.LOGICAL_NAME:
                    return "pdate";
                case Decimal.LOGICAL_NAME:
                    return "string";
                default:
                    break;
            }
        }
        switch (schema.type()) {
            case INT8:
            case INT16:
            case INT32:
                return "pint";
            case INT64:
                return "plong";
            case FLOAT32:
                return "pfloat";
            case FLOAT64:
                return "pdouble";
            case BOOLEAN:
                return "boolean";
            case ARRAY:
                return solrTypeFor(schema.valueSchema());
            case BYTES:
            case STRING:
            case MAP:
            case STRUCT:
            default:
                return "string";
        }
    }
}
