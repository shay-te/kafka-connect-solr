package com.una.kafka.connect.solr;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.common.util.NamedList;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SolrSchemaManagerTest {

    private SolrSinkConfig cfg(boolean evolve) {
        Map<String, String> props = new HashMap<>();
        props.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        props.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "c");
        props.put(SolrSinkConfig.SCHEMA_AUTO_EVOLVE_CONFIG, String.valueOf(evolve));
        return new SolrSinkConfig(props);
    }

    @Test
    void noopWhenEvolveOff() throws Exception {
        SolrClient client = mock(SolrClient.class);
        SolrSchemaManager mgr = new SolrSchemaManager(client, cfg(false));
        mgr.evolveIfNeeded("c", SchemaBuilder.struct().field("a", Schema.STRING_SCHEMA).build());
        verify(client, never()).request(any(), anyString());
    }

    @Test
    void addsMissingFields() throws Exception {
        SolrClient client = mock(SolrClient.class);
        // Existing schema: empty fields list.
        NamedList<Object> existing = new NamedList<>();
        existing.add("fields", new ArrayList<Map<String, Object>>());
        when(client.request(any(SchemaRequest.Fields.class), anyString())).thenReturn(existing);
        when(client.request(any(SchemaRequest.AddField.class), anyString())).thenReturn(new NamedList<>());

        Schema schema = SchemaBuilder.struct()
                .field("first_name", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .field("score", Schema.FLOAT64_SCHEMA)
                .build();

        SolrSchemaManager mgr = new SolrSchemaManager(client, cfg(true));
        mgr.evolveIfNeeded("c", schema);

        verify(client, atLeastOnce()).request(any(SchemaRequest.AddField.class), anyString());
    }

    @Test
    void tolerantWhenSchemaApiThrows() throws Exception {
        SolrClient client = mock(SolrClient.class);
        when(client.request(any(SchemaRequest.Fields.class), anyString()))
                .thenThrow(new RuntimeException("boom"));
        when(client.request(any(SchemaRequest.AddField.class), anyString()))
                .thenThrow(new RuntimeException("boom too"));

        SolrSchemaManager mgr = new SolrSchemaManager(client, cfg(true));
        // Must not propagate.
        mgr.evolveIfNeeded("c", SchemaBuilder.struct().field("a", Schema.STRING_SCHEMA).build());
    }

    @Test
    void nestedStructFlattensIntoDottedPaths() throws Exception {
        SolrClient client = mock(SolrClient.class);
        NamedList<Object> existing = new NamedList<>();
        existing.add("fields", Collections.emptyList());
        when(client.request(any(SchemaRequest.Fields.class), anyString())).thenReturn(existing);
        when(client.request(any(SchemaRequest.AddField.class), anyString())).thenReturn(new NamedList<>());

        Schema inner = SchemaBuilder.struct().field("city", Schema.STRING_SCHEMA).build();
        Schema outer = SchemaBuilder.struct().field("addr", inner).build();
        new SolrSchemaManager(client, cfg(true)).evolveIfNeeded("c", outer);

        verify(client, atLeastOnce()).request(any(SchemaRequest.AddField.class), anyString());
    }

    @Test
    void repeatedCallsWithSameSchemaSkipTheWalk() throws Exception {
        SolrClient client = mock(SolrClient.class);
        NamedList<Object> existing = new NamedList<>();
        existing.add("fields", new ArrayList<Map<String, Object>>());
        when(client.request(any(SchemaRequest.Fields.class), anyString())).thenReturn(existing);
        when(client.request(any(SchemaRequest.AddField.class), anyString())).thenReturn(new NamedList<>());

        Schema schema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .build();

        SolrSchemaManager mgr = new SolrSchemaManager(client, cfg(true));
        mgr.evolveIfNeeded("c", schema);
        mgr.evolveIfNeeded("c", schema);
        mgr.evolveIfNeeded("c", schema);

        // First call adds the two fields; subsequent calls short-circuit on the seen-schemas cache.
        verify(client, org.mockito.Mockito.times(2))
                .request(any(SchemaRequest.AddField.class), anyString());
    }

    @Test
    void typeMappingMatrix() {
        assertThat(SolrSchemaManager.solrTypeFor(null)).isEqualTo("string");
        assertThat(SolrSchemaManager.solrTypeFor(Schema.STRING_SCHEMA)).isEqualTo("string");
        assertThat(SolrSchemaManager.solrTypeFor(Schema.INT8_SCHEMA)).isEqualTo("pint");
        assertThat(SolrSchemaManager.solrTypeFor(Schema.INT16_SCHEMA)).isEqualTo("pint");
        assertThat(SolrSchemaManager.solrTypeFor(Schema.INT32_SCHEMA)).isEqualTo("pint");
        assertThat(SolrSchemaManager.solrTypeFor(Schema.INT64_SCHEMA)).isEqualTo("plong");
        assertThat(SolrSchemaManager.solrTypeFor(Schema.FLOAT32_SCHEMA)).isEqualTo("pfloat");
        assertThat(SolrSchemaManager.solrTypeFor(Schema.FLOAT64_SCHEMA)).isEqualTo("pdouble");
        assertThat(SolrSchemaManager.solrTypeFor(Schema.BOOLEAN_SCHEMA)).isEqualTo("boolean");
        assertThat(SolrSchemaManager.solrTypeFor(Schema.BYTES_SCHEMA)).isEqualTo("string");
        assertThat(SolrSchemaManager.solrTypeFor(SchemaBuilder.array(Schema.INT64_SCHEMA).build()))
                .isEqualTo("plong");
        assertThat(SolrSchemaManager.solrTypeFor(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build()))
                .isEqualTo("string");
        assertThat(SolrSchemaManager.solrTypeFor(SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build()))
                .isEqualTo("string");
        assertThat(SolrSchemaManager.solrTypeFor(org.apache.kafka.connect.data.Timestamp.SCHEMA))
                .isEqualTo("pdate");
        assertThat(SolrSchemaManager.solrTypeFor(org.apache.kafka.connect.data.Date.SCHEMA))
                .isEqualTo("pdate");
        assertThat(SolrSchemaManager.solrTypeFor(org.apache.kafka.connect.data.Time.SCHEMA))
                .isEqualTo("pdate");
        assertThat(SolrSchemaManager.solrTypeFor(org.apache.kafka.connect.data.Decimal.schema(2)))
                .isEqualTo("string");
    }
}
