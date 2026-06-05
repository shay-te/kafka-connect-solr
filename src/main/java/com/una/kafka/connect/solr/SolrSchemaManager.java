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
    private final Map<String, Set<String>> knownFieldsPerCollection = new HashMap<>();
    private final Map<String, Set<Schema>> evolvedSchemasPerCollection = new HashMap<>();

    public SolrSchemaManager(SolrClient client, SolrSinkConfig config) {
        this.client = client;
        this.autoEvolve = config.schemaAutoEvolve();
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
            collectAndAdd(schema, "", collection, known);
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
        } catch (Exception e) {
            log.warn("Could not read existing schema for {}: {}", collection, e.getMessage());
        }
        return set;
    }

    private void collectAndAdd(Schema schema, String prefix, String collection, Set<String> known) {
        if (schema.type() == Schema.Type.STRUCT) {
            for (Field field : schema.fields()) {
                String name = prefix.isEmpty() ? field.name() : prefix + "." + field.name();
                Schema sub = field.schema();
                if (sub.type() == Schema.Type.STRUCT) {
                    collectAndAdd(sub, name, collection, known);
                } else {
                    ensureField(collection, name, sub, known);
                }
            }
        } else {
            ensureField(collection, prefix, schema, known);
        }
    }

    private void ensureField(String collection, String name, Schema schema, Set<String> known) {
        if (name == null || name.isEmpty() || known.contains(name)) {
            return;
        }
        Map<String, Object> attrs = new LinkedHashMap<>();
        attrs.put("name", name);
        attrs.put("type", solrTypeFor(schema));
        attrs.put("stored", true);
        attrs.put("indexed", true);
        if (schema.type() == Schema.Type.ARRAY) {
            attrs.put("multiValued", true);
        }
        try {
            new SchemaRequest.AddField(attrs).process(client, collection);
            known.add(name);
            log.info("Added Solr field {} ({}) to collection {}", name, attrs.get("type"), collection);
        } catch (Exception e) {
            log.debug("AddField {} ignored: {}", name, e.getMessage());
            known.add(name);
        }
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
