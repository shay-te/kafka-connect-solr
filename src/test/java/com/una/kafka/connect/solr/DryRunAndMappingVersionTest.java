package com.una.kafka.connect.solr;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class DryRunAndMappingVersionTest {

    private SolrSinkConfig cfg(Map<String, String> overrides) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "users");
        p.put(SolrSinkConfig.BATCH_SIZE_CONFIG, "1");
        p.put(SolrSinkConfig.LINGER_MS_CONFIG, "1");
        p.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "UNUSED");
        p.putAll(overrides);
        return new SolrSinkConfig(p);
    }

    @Test
    void dryRunSkipsSolrButAdvancesCounters() throws Exception {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.DRY_RUN_CONFIG, "true");
        SolrClient client = mock(SolrClient.class);
        SolrWriter w = new SolrWriter(client, cfg(o), new SyncOffsetTracker());

        Schema s = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        Struct v = new Struct(s).put("x", "y");
        w.write(new SinkRecord("users", 0, Schema.STRING_SCHEMA, "k", s, v, 7L));
        w.flush();

        // Never called Solr.
        verify(client, never()).request(any(UpdateRequest.class), anyString());
        // But counters moved.
        assertThat(w.recordsWritten()).isGreaterThanOrEqualTo(1);
        assertThat(w.lastSuccessEpochMs()).isPositive();
        w.close();
    }

    @Test
    void mappingVersionAddedToEveryDoc() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.MAPPING_VERSION_CONFIG, "2026-06-05");
        SolrRecordConverter conv = new SolrRecordConverter(cfg(o));

        Schema s = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        Struct v = new Struct(s).put("x", "y");
        SolrInputDocument doc = conv.convert(
                new SinkRecord("users", 0, Schema.STRING_SCHEMA, "k", s, v, 1L));
        assertThat(doc.getFieldValue("_mapping_version")).isEqualTo("2026-06-05");
    }

    @Test
    void mappingVersionAbsentByDefault() {
        SolrRecordConverter conv = new SolrRecordConverter(cfg(new HashMap<>()));
        Schema s = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        Struct v = new Struct(s).put("x", "y");
        SolrInputDocument doc = conv.convert(
                new SinkRecord("users", 0, Schema.STRING_SCHEMA, "k", s, v, 1L));
        assertThat(doc.getFieldNames()).doesNotContain("_mapping_version");
    }

    @Test
    void lastSuccessEpochMsStartsAtZero() {
        // Before any write, lastSuccessEpochMs should be 0.
        Map<String, String> o = new HashMap<>();
        SolrClient client = mock(SolrClient.class);
        SolrWriter w = new SolrWriter(client, cfg(o), new SyncOffsetTracker());
        assertThat(w.lastSuccessEpochMs()).isZero();
        w.close();
    }
}
