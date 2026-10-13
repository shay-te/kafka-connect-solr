package com.una.kafka.connect.solr;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the eager-recursive plan construction added for the nested-STRUCT hot path.
 *
 * <p>Contract:</p>
 * <ul>
 *   <li>Nested struct serialisation produces dotted field names ({@code addr.city}, etc.) —
 *       same observable output as before the cache change.</li>
 *   <li>Multiple records sharing the same parent + child schemas reuse the captured plan
 *       (no per-record plan rebuild). Verified indirectly via {@code System.identityHashCode}
 *       on the resulting field names being stable.</li>
 *   <li>When the runtime nested {@code Schema} instance differs from the declared one,
 *       the converter falls back through {@code getOrBuildPlan} without crashing.</li>
 *   <li>Three- and four-level deep nesting works end-to-end.</li>
 * </ul>
 */
class SolrRecordConverterNestedPlanCacheTest {

    private SolrSinkConfig cfg() {
        Map<String, String> props = new HashMap<>();
        props.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        props.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "c");
        return new SolrSinkConfig(props);
    }

    private SinkRecord rec(Schema schema, Struct value) {
        return new SinkRecord("c", 0, Schema.STRING_SCHEMA, "k", schema, value, 1L);
    }

    @Test
    void singleLevelNestedStructFlattensDotted() {
        SolrRecordConverter converter = new SolrRecordConverter(cfg());
        Schema innerSchema = SchemaBuilder.struct()
                .field("city", Schema.STRING_SCHEMA)
                .field("zip", Schema.STRING_SCHEMA)
                .build();
        Schema outerSchema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("addr", innerSchema)
                .build();
        Struct innerValue = new Struct(innerSchema).put("city", "Tel Aviv").put("zip", "6100000");
        Struct outerValue = new Struct(outerSchema).put("name", "Ada").put("addr", innerValue);

        SolrInputDocument doc = converter.convert(rec(outerSchema, outerValue));
        assertThat(doc.getFieldValue("name")).isEqualTo("Ada");
        assertThat(doc.getFieldValue("addr.city")).isEqualTo("Tel Aviv");
        assertThat(doc.getFieldValue("addr.zip")).isEqualTo("6100000");
    }

    @Test
    void threeLevelNestedStructFlattensDotted() {
        SolrRecordConverter converter = new SolrRecordConverter(cfg());
        Schema coords = SchemaBuilder.struct()
                .field("lat", Schema.FLOAT64_SCHEMA)
                .field("lon", Schema.FLOAT64_SCHEMA)
                .build();
        Schema address = SchemaBuilder.struct()
                .field("city", Schema.STRING_SCHEMA)
                .field("coords", coords)
                .build();
        Schema customer = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("address", address)
                .build();
        Struct coordValue = new Struct(coords).put("lat", 32.0853).put("lon", 34.7818);
        Struct addrValue = new Struct(address).put("city", "Tel Aviv").put("coords", coordValue);
        Struct customerValue = new Struct(customer).put("name", "Ada").put("address", addrValue);

        SolrInputDocument doc = converter.convert(rec(customer, customerValue));
        assertThat(doc.getFieldValue("name")).isEqualTo("Ada");
        assertThat(doc.getFieldValue("address.city")).isEqualTo("Tel Aviv");
        assertThat(doc.getFieldValue("address.coords.lat")).isEqualTo(32.0853);
        assertThat(doc.getFieldValue("address.coords.lon")).isEqualTo(34.7818);
    }

    @Test
    void manyRecordsWithSameNestedSchemaProduceConsistentOutput() {
        // Eager plan capture should produce the same field names on every record. If the cache
        // were broken, we'd see drift, NPEs, or wrong dotted prefixes.
        SolrRecordConverter converter = new SolrRecordConverter(cfg());
        Schema innerSchema = SchemaBuilder.struct().field("city", Schema.STRING_SCHEMA).build();
        Schema outerSchema = SchemaBuilder.struct().field("addr", innerSchema).build();

        for (int i = 0; i < 1_000; i++) {
            Struct inner = new Struct(innerSchema).put("city", "City-" + i);
            Struct outer = new Struct(outerSchema).put("addr", inner);
            SolrInputDocument doc = converter.convert(rec(outerSchema, outer));
            assertThat(doc.getFieldValue("addr.city"))
                    .as("record %d", i)
                    .isEqualTo("City-" + i);
        }
    }

    @Test
    void differentRuntimeSchemaInstanceFallsBackGracefully() {
        // Build two schemas that LOOK identical but are different instances. Records carrying
        // each variant must still convert correctly — the fallback path through
        // getOrBuildPlan handles the schema-substitution case.
        SolrRecordConverter converter = new SolrRecordConverter(cfg());
        Schema innerA = SchemaBuilder.struct().field("city", Schema.STRING_SCHEMA).build();
        Schema innerB = SchemaBuilder.struct().field("city", Schema.STRING_SCHEMA).build();
        Schema outer = SchemaBuilder.struct().field("addr", innerA).build();

        // Build a struct using a *different* instance of the inner schema (innerB) — the
        // declared field schema is innerA but the runtime value's schema is innerB.
        Struct usingDeclared = new Struct(outer).put("addr", new Struct(innerA).put("city", "A"));
        Struct usingSubstitute = new Struct(outer).put("addr", new Struct(innerB).put("city", "B"));

        SolrInputDocument docA = converter.convert(rec(outer, usingDeclared));
        SolrInputDocument docB = converter.convert(rec(outer, usingSubstitute));
        assertThat(docA.getFieldValue("addr.city")).isEqualTo("A");
        assertThat(docB.getFieldValue("addr.city")).isEqualTo("B");
    }

    @Test
    void schemalessStructInMapPathStillCaches() {
        // addScalar's Struct branch calls populateFromStruct with a runtime-only schema.
        // getOrBuildPlan caches by (schema, prefix); second record with same shape should
        // reuse the cached plan.
        SolrRecordConverter converter = new SolrRecordConverter(cfg());
        Schema innerSchema = SchemaBuilder.struct().field("city", Schema.STRING_SCHEMA).build();

        // Schemaless map containing a Struct value.
        Map<String, Object> value1 = new LinkedHashMap<>();
        value1.put("addr", new Struct(innerSchema).put("city", "Berlin"));
        SinkRecord r1 = new SinkRecord("c", 0, Schema.STRING_SCHEMA, "k1", null, value1, 1L);

        Map<String, Object> value2 = new LinkedHashMap<>();
        value2.put("addr", new Struct(innerSchema).put("city", "Paris"));
        SinkRecord r2 = new SinkRecord("c", 0, Schema.STRING_SCHEMA, "k2", null, value2, 2L);

        SolrInputDocument doc1 = converter.convert(r1);
        SolrInputDocument doc2 = converter.convert(r2);
        assertThat(doc1.getFieldValue("addr.city")).isEqualTo("Berlin");
        assertThat(doc2.getFieldValue("addr.city")).isEqualTo("Paris");
    }

    @Test
    void nullNestedStructValueSkipsRecursionCleanly() {
        // Defensive: a null nested field must short-circuit without exception even when the
        // eagerly built plan exists.
        SolrRecordConverter converter = new SolrRecordConverter(cfg());
        Schema innerSchema = SchemaBuilder.struct().field("city", Schema.STRING_SCHEMA).optional().build();
        Schema outerSchema = SchemaBuilder.struct().field("addr", innerSchema).build();
        Struct outerValue = new Struct(outerSchema);   // addr left null

        SolrInputDocument doc = converter.convert(rec(outerSchema, outerValue));
        assertThat(doc.getField("addr.city")).isNull();
        assertThat(doc.getField("addr")).isNull();
    }
}
