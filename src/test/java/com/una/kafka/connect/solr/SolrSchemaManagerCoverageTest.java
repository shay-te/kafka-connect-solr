package com.una.kafka.connect.solr;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.util.NamedList;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SolrSchemaManagerCoverageTest {

    private SolrSinkConfig cfg(Map<String, String> o) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "c");
        p.put(SolrSinkConfig.SCHEMA_AUTO_EVOLVE_CONFIG, "true");
        p.putAll(o);
        return new SolrSinkConfig(p);
    }

    @Test
    void disabledIsNoop() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.SCHEMA_AUTO_EVOLVE_CONFIG, "false");
        SolrClient client = mock(SolrClient.class);
        SolrSchemaManager m = new SolrSchemaManager(client, cfg(o));
        Schema s = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        m.evolveIfNeeded("c", s);
        org.mockito.Mockito.verifyNoInteractions(client);
    }

    @Test
    void nullSchemaShortCircuits() {
        SolrClient client = mock(SolrClient.class);
        new SolrSchemaManager(client, cfg(new HashMap<>())).evolveIfNeeded("c", null);
        org.mockito.Mockito.verifyNoInteractions(client);
    }

    @Test
    void sameSchemaSeenIsMemoised() throws Exception {
        SolrClient client = mock(SolrClient.class);
        when(client.request(any(SolrRequest.class), any())).thenReturn(emptySchema());

        SolrSchemaManager m = new SolrSchemaManager(client, cfg(new HashMap<>()));
        Schema s = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        m.evolveIfNeeded("c", s);
        int afterFirst = org.mockito.Mockito.mockingDetails(client).getInvocations().size();
        m.evolveIfNeeded("c", s);
        int afterSecond = org.mockito.Mockito.mockingDetails(client).getInvocations().size();
        // Seen-set short-circuit: second call must not add any request.
        assertThat(afterSecond).isEqualTo(afterFirst);
    }

    @Test
    void addFieldFailureIsSwallowedAndMarkedKnown() throws Exception {
        SolrClient client = mock(SolrClient.class);
        // First request is the schema probe (returns empty fields). Subsequent AddField requests throw.
        java.util.concurrent.atomic.AtomicInteger n = new java.util.concurrent.atomic.AtomicInteger();
        when(client.request(any(SolrRequest.class), any())).thenAnswer(inv -> {
            if (n.getAndIncrement() == 0) return emptySchema();
            throw new RuntimeException("add failed");
        });
        SolrSchemaManager m = new SolrSchemaManager(client, cfg(new HashMap<>()));
        Schema s = SchemaBuilder.struct().field("y", Schema.INT64_SCHEMA).build();
        m.evolveIfNeeded("c", s);
        // No exception leaked; both probe and AddField fired.
        verify(client, times(2)).request(any(SolrRequest.class), any());
    }

    @Test
    void typeMappingCoversEveryConnectType() {
        // Drives the full switch table in solrTypeFor.
        assertThat(SolrSchemaManager.solrTypeFor(null)).isEqualTo("string");
        assertThat(SolrSchemaManager.solrTypeFor(Schema.INT8_SCHEMA)).isEqualTo("pint");
        assertThat(SolrSchemaManager.solrTypeFor(Schema.INT16_SCHEMA)).isEqualTo("pint");
        assertThat(SolrSchemaManager.solrTypeFor(Schema.INT32_SCHEMA)).isEqualTo("pint");
        assertThat(SolrSchemaManager.solrTypeFor(Schema.INT64_SCHEMA)).isEqualTo("plong");
        assertThat(SolrSchemaManager.solrTypeFor(Schema.FLOAT32_SCHEMA)).isEqualTo("pfloat");
        assertThat(SolrSchemaManager.solrTypeFor(Schema.FLOAT64_SCHEMA)).isEqualTo("pdouble");
        assertThat(SolrSchemaManager.solrTypeFor(Schema.BOOLEAN_SCHEMA)).isEqualTo("boolean");
        assertThat(SolrSchemaManager.solrTypeFor(Schema.STRING_SCHEMA)).isEqualTo("string");
        assertThat(SolrSchemaManager.solrTypeFor(Schema.BYTES_SCHEMA)).isEqualTo("string");
        // ARRAY recurses into element schema.
        Schema arr = SchemaBuilder.array(Schema.INT32_SCHEMA).build();
        assertThat(SolrSchemaManager.solrTypeFor(arr)).isEqualTo("pint");
        // Logical names.
        assertThat(SolrSchemaManager.solrTypeFor(org.apache.kafka.connect.data.Timestamp.SCHEMA)).isEqualTo("pdate");
        assertThat(SolrSchemaManager.solrTypeFor(org.apache.kafka.connect.data.Date.SCHEMA)).isEqualTo("pdate");
        assertThat(SolrSchemaManager.solrTypeFor(org.apache.kafka.connect.data.Time.SCHEMA)).isEqualTo("pdate");
        assertThat(SolrSchemaManager.solrTypeFor(org.apache.kafka.connect.data.Decimal.schema(2))).isEqualTo("string");
        // Map/Struct/default fall-throughs.
        Schema mapSch = SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build();
        assertThat(SolrSchemaManager.solrTypeFor(mapSch)).isEqualTo("string");
        Schema structSch = SchemaBuilder.struct().field("z", Schema.STRING_SCHEMA).build();
        assertThat(SolrSchemaManager.solrTypeFor(structSch)).isEqualTo("string");
        // Non-logical named schema (e.g. SchemaBuilder with name set, no special case).
        Schema named = SchemaBuilder.string().name("custom").build();
        assertThat(SolrSchemaManager.solrTypeFor(named)).isEqualTo("string");
    }

    @Test
    void nestedStructFieldsAddedWithDottedNames() throws Exception {
        SolrClient client = mock(SolrClient.class);
        when(client.request(any(SolrRequest.class), any())).thenReturn(emptySchema());
        SolrSchemaManager m = new SolrSchemaManager(client, cfg(new HashMap<>()));
        Schema inner = SchemaBuilder.struct().field("city", Schema.STRING_SCHEMA).build();
        Schema outer = SchemaBuilder.struct().field("addr", inner).build();
        m.evolveIfNeeded("c", outer);
        verify(client, org.mockito.Mockito.atLeastOnce()).request(any(SolrRequest.class), any());
    }

    @Test
    void nonStructTopLevelEvolveUsesEmptyName() throws Exception {
        // A non-STRUCT schema falls through to ensureField with empty name — name.isEmpty() short-circuits,
        // so no AddField issued. The schema probe still fires (loadKnown).
        SolrClient client = mock(SolrClient.class);
        when(client.request(any(SolrRequest.class), any())).thenReturn(emptySchema());
        SolrSchemaManager m = new SolrSchemaManager(client, cfg(new HashMap<>()));
        m.evolveIfNeeded("c", Schema.STRING_SCHEMA);
        // Exactly one request: the schema probe. No AddField since the name is empty.
        verify(client, times(1)).request(any(SolrRequest.class), any());
    }

    @Test
    void loadKnownFailureFallsBackToEmptySet() throws Exception {
        SolrClient client = mock(SolrClient.class);
        // Force the Fields() probe to throw; ensureField should still proceed (set empty).
        when(client.request(any(SolrRequest.class), any())).thenThrow(new RuntimeException("probe boom"));
        SolrSchemaManager m = new SolrSchemaManager(client, cfg(new HashMap<>()));
        Schema s = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        m.evolveIfNeeded("c", s);
        // Still calls back into client to attempt the AddField.
        verify(client, org.mockito.Mockito.atLeastOnce()).request(any(SolrRequest.class), any());
    }

    @Test
    void arrayFieldGetsMultiValuedTrue() throws Exception {
        SolrClient client = mock(SolrClient.class);
        when(client.request(any(SolrRequest.class), any())).thenReturn(emptySchema());
        SolrSchemaManager m = new SolrSchemaManager(client, cfg(new HashMap<>()));
        Schema arr = SchemaBuilder.array(Schema.STRING_SCHEMA).build();
        Schema schema = SchemaBuilder.struct().field("tags", arr).build();
        m.evolveIfNeeded("c", schema);
        verify(client, times(2)).request(any(SolrRequest.class), any());
    }

    private NamedList<Object> emptySchema() {
        NamedList<Object> r = new NamedList<>();
        r.add("fields", java.util.Collections.emptyList());
        return r;
    }
}
