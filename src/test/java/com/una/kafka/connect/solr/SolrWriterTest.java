package com.una.kafka.connect.solr;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class SolrWriterTest {

    private SolrSinkConfig cfg(Map<String, String> overrides) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "users");
        p.put(SolrSinkConfig.BATCH_SIZE_CONFIG, "1");
        p.put(SolrSinkConfig.LINGER_MS_CONFIG, "1");
        p.put(SolrSinkConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG, "1");
        p.put(SolrSinkConfig.MAX_RETRIES_CONFIG, "0");
        // Skip the existence probe — tests use a mocked SolrClient with no collections.
        p.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "UNUSED");
        p.putAll(overrides);
        return new SolrSinkConfig(p);
    }

    private SinkRecord rec(String key, Struct value, Schema schema) {
        return new SinkRecord("users", 0, Schema.STRING_SCHEMA, key, schema, value, 1L);
    }

    @Test
    void writesStructDocument() throws Exception {
        SolrClient client = mock(SolrClient.class);
        SolrWriter w = new SolrWriter(client, cfg(new HashMap<>()));
        Schema s = SchemaBuilder.struct().field("first_name", Schema.STRING_SCHEMA).build();
        Struct v = new Struct(s).put("first_name", "Ada");

        w.write(rec("u1", v, s));
        w.flush();

        verify(client, atLeastOnce()).request(any(UpdateRequest.class), anyString());
        w.close();
    }

    @Test
    void tombstoneIgnoredByDefault() throws Exception {
        SolrClient client = mock(SolrClient.class);
        SolrWriter w = new SolrWriter(client, cfg(new HashMap<>()));
        SinkRecord r = new SinkRecord("users", 0, Schema.STRING_SCHEMA, "k", null, null, 1L);
        assertThatCode(() -> { w.write(r); w.flush(); }).doesNotThrowAnyException();
        w.close();
    }

    @Test
    void tombstoneDeletesWhenBehaviorIsDelete() throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put(SolrSinkConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG, "delete");
        SolrClient client = mock(SolrClient.class);
        SolrWriter w = new SolrWriter(client, cfg(overrides));

        SinkRecord r = new SinkRecord("users", 0, Schema.STRING_SCHEMA, "k", null, null, 1L);
        w.write(r);
        w.flush();
        verify(client, atLeastOnce()).request(any(UpdateRequest.class), anyString());
        w.close();
    }

    @Test
    void tombstoneWithNullKeyIsSkipped() throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put(SolrSinkConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG, "delete");
        SolrClient client = mock(SolrClient.class);
        SolrWriter w = new SolrWriter(client, cfg(overrides));
        SinkRecord r = new SinkRecord("users", 0, null, null, null, null, 1L);
        assertThatCode(() -> { w.write(r); w.flush(); }).doesNotThrowAnyException();
        w.close();
    }

    @Test
    void tombstoneFailsWhenBehaviorIsFail() {
        Map<String, String> overrides = new HashMap<>();
        overrides.put(SolrSinkConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG, "fail");
        SolrClient client = mock(SolrClient.class);
        SolrWriter w = new SolrWriter(client, cfg(overrides));
        SinkRecord r = new SinkRecord("users", 0, Schema.STRING_SCHEMA, "k", null, null, 1L);
        assertThatThrownBy(() -> w.write(r)).isInstanceOf(DataException.class);
        w.close();
    }

    @Test
    void malformedRecordIgnoredByBehavior() throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put(SolrSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, "ignore");
        overrides.put(SolrSinkConfig.ID_STRATEGY_CONFIG, "KAFKA_KEY");
        SolrClient client = mock(SolrClient.class);
        SolrWriter w = new SolrWriter(client, cfg(overrides));

        Schema s = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        Struct v = new Struct(s).put("x", "y");
        // null key with KAFKA_KEY id strategy is malformed.
        SinkRecord r = new SinkRecord("users", 0, null, null, s, v, 1L);
        assertThatCode(() -> w.write(r)).doesNotThrowAnyException();
        w.close();
    }

    @Test
    void malformedRecordWarnDoesNotThrow() throws Exception {
        Map<String, String> overrides = new HashMap<>();
        overrides.put(SolrSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, "warn");
        overrides.put(SolrSinkConfig.ID_STRATEGY_CONFIG, "KAFKA_KEY");
        SolrClient client = mock(SolrClient.class);
        SolrWriter w = new SolrWriter(client, cfg(overrides));

        Schema s = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        Struct v = new Struct(s).put("x", "y");
        SinkRecord r = new SinkRecord("users", 0, null, null, s, v, 1L);
        assertThatCode(() -> w.write(r)).doesNotThrowAnyException();
        w.close();
    }

    @Test
    void sourceFieldCapturesTheNestedShapeOfASchemadRecord() throws Exception {
        // Jackson discovers no bean properties on a Connect Struct, so serializing one directly
        // throws — which would make EVERY schema'd record malformed the moment source.field is on.
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.SOURCE_FIELD_CONFIG, "_src");
        o.put(SolrSinkConfig.SOURCE_EXCLUDE_FIELDS_CONFIG, "custom_field_");
        SolrClient client = mock(SolrClient.class);
        SolrWriter w = new SolrWriter(client, cfg(o));

        Schema address = SchemaBuilder.struct().field("city", Schema.STRING_SCHEMA).build();
        Schema user = SchemaBuilder.struct()
                .field("first_name", Schema.STRING_SCHEMA)
                .field("address", address)
                .field("custom_field_5_value_2", Schema.STRING_SCHEMA)
                .build();
        Struct value = new Struct(user)
                .put("first_name", "Ada")
                .put("address", new Struct(address).put("city", "London"))
                .put("custom_field_5_value_2", "index-only");

        org.mockito.ArgumentCaptor<UpdateRequest> req =
                org.mockito.ArgumentCaptor.forClass(UpdateRequest.class);
        w.write(rec("u1", value, user));
        w.flush();
        verify(client, atLeastOnce()).request(req.capture(), anyString());

        Object src = req.getValue().getDocuments().get(0).getFieldValue("_src");
        // Nested struct survives as nested JSON; the index-only prefix is stripped.
        assertThat(src).isEqualTo("{\"first_name\":\"Ada\",\"address\":{\"city\":\"London\"}}");
        w.close();
    }

    @Test
    void metricsExposed() {
        SolrClient client = mock(SolrClient.class);
        SolrWriter w = new SolrWriter(client, cfg(new HashMap<>()));
        // Just exercise the accessors.
        w.recordsWritten();
        w.recordsFailed();
        w.retries();
        w.queueDepth();
        w.close();
    }
}
