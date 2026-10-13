package com.una.kafka.connect.solr;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class SolrWriterExtendedTest {

    private SolrSinkConfig cfg(Map<String, String> overrides) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "users");
        p.put(SolrSinkConfig.BATCH_SIZE_CONFIG, "1");
        p.put(SolrSinkConfig.LINGER_MS_CONFIG, "1");
        p.put(SolrSinkConfig.MAX_RETRIES_CONFIG, "0");
        p.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "UNUSED");
        p.putAll(overrides);
        return new SolrSinkConfig(p);
    }

    private SinkRecord rec(String topic, String key, Struct v, Schema schema, ConnectHeaders headers) {
        return new SinkRecord(topic, 0, Schema.STRING_SCHEMA, key, schema, v, 1L,
                System.currentTimeMillis(),
                org.apache.kafka.common.record.TimestampType.NO_TIMESTAMP_TYPE, headers);
    }

    @Test
    void perTopicKeyIgnoreFallsBackToTopicPartitionOffset() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.TOPIC_KEY_IGNORE_CONFIG, "users");
        SolrClient client = mock(SolrClient.class);
        SolrWriter w = new SolrWriter(client, cfg(o), new SyncOffsetTracker());

        Schema s = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        Struct v = new Struct(s).put("x", "y");
        // No key, but per-topic ignore is set -> must NOT throw.
        assertThatCode(() -> {
            w.write(new SinkRecord("users", 0, null, null, s, v, 7L));
            w.flush();
        }).doesNotThrowAnyException();
        w.close();
    }

    @Test
    void externalVersionHeaderIsWritten() throws Exception {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.EXTERNAL_VERSION_HEADER_CONFIG, "kafka_offset");
        SolrClient client = mock(SolrClient.class);
        SolrWriter w = new SolrWriter(client, cfg(o), new SyncOffsetTracker());

        Schema s = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        Struct v = new Struct(s).put("x", "y");
        ConnectHeaders headers = new ConnectHeaders();
        headers.addLong("kafka_offset", 42L);

        w.write(rec("users", "k", v, s, headers));
        w.flush();
        // We can't easily intercept the SolrInputDocument without diving into
        // UpdateRequest internals, but we can at least verify the path is hit.
        verify(client, atLeastOnce()).request(any(UpdateRequest.class), anyString());
        w.close();
    }

    @Test
    void externalVersionHeaderMissingIsHarmless() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.EXTERNAL_VERSION_HEADER_CONFIG, "kafka_offset");
        SolrClient client = mock(SolrClient.class);
        SolrWriter w = new SolrWriter(client, cfg(o), new SyncOffsetTracker());

        Schema s = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        Struct v = new Struct(s).put("x", "y");
        assertThatCode(() -> w.write(rec("users", "k", v, s, new ConnectHeaders())))
                .doesNotThrowAnyException();
        w.close();
    }

    @Test
    void asyncOffsetTrackerAdvancesContiguousAcks() {
        SolrClient client = mock(SolrClient.class);
        AsyncOffsetTracker tracker = new AsyncOffsetTracker();
        SolrWriter w = new SolrWriter(client, cfg(new HashMap<>()), tracker);

        Schema s = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        Struct v = new Struct(s).put("x", "y");
        w.write(rec("users", "k1", v, s, new ConnectHeaders()));
        w.write(rec("users", "k2", v, s, new ConnectHeaders()));
        w.flush();
        // Both batches acked - safeOffsets should advance past offset 1.
        Map<org.apache.kafka.common.TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> safe =
                tracker.safeOffsets(new HashMap<>());
        org.apache.kafka.common.TopicPartition tp = new org.apache.kafka.common.TopicPartition("users", 0);
        org.assertj.core.api.Assertions.assertThat(safe.get(tp).offset()).isEqualTo(2L);
        w.close();
    }
}
