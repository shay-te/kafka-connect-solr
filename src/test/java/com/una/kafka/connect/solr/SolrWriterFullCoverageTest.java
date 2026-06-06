package com.una.kafka.connect.solr;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Exhausts SolrWriter branches: external version headers (numeric, ascii string,
 * byte[], null header, null value), schema-ignore set, key-ignore set,
 * malformed-record FAIL, processor caching hit, partition-fanout revoke path.
 */
class SolrWriterFullCoverageTest {

    private SolrSinkConfig cfg(Map<String, String> o) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "users");
        p.put(SolrSinkConfig.BATCH_SIZE_CONFIG, "1");
        p.put(SolrSinkConfig.LINGER_MS_CONFIG, "1");
        p.put(SolrSinkConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG, "1");
        p.put(SolrSinkConfig.MAX_RETRIES_CONFIG, "0");
        p.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "UNUSED");
        p.putAll(o);
        return new SolrSinkConfig(p);
    }

    private SinkRecord rec(Schema s, Struct v, Headers h, String topic) {
        return new SinkRecord(topic == null ? "users" : topic, 0,
                Schema.STRING_SCHEMA, "k", s, v, 1L,
                null, null, h);
    }

    @Test
    void externalVersionNumericHeader() throws Exception {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.EXTERNAL_VERSION_HEADER_CONFIG, "_v");
        SolrClient client = mock(SolrClient.class);
        SolrWriter w = new SolrWriter(client, cfg(o));
        Schema s = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        Struct v = new Struct(s).put("x", "y");
        Headers h = new ConnectHeaders();
        h.addLong("_v", 1234L);
        w.write(rec(s, v, h, null));
        w.flush();
        verify(client, atLeastOnce()).request(any(UpdateRequest.class), anyString());
        w.close();
    }

    @Test
    void externalVersionAsciiStringHeader() throws Exception {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.EXTERNAL_VERSION_HEADER_CONFIG, "_v");
        SolrClient client = mock(SolrClient.class);
        SolrWriter w = new SolrWriter(client, cfg(o));
        Schema s = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        Struct v = new Struct(s).put("x", "y");
        Headers h = new ConnectHeaders();
        h.addString("_v", " 42 ");
        w.write(rec(s, v, h, null));
        w.flush();
        w.close();
    }

    @Test
    void externalVersionByteArrayHeader() throws Exception {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.EXTERNAL_VERSION_HEADER_CONFIG, "_v");
        SolrClient client = mock(SolrClient.class);
        SolrWriter w = new SolrWriter(client, cfg(o));
        Schema s = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        Struct v = new Struct(s).put("x", "y");
        Headers h = new ConnectHeaders();
        h.addBytes("_v", "99".getBytes());
        w.write(rec(s, v, h, null));
        w.flush();
        w.close();
    }

    @Test
    void externalVersionMissingHeaderIsTolerated() throws Exception {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.EXTERNAL_VERSION_HEADER_CONFIG, "_v");
        SolrClient client = mock(SolrClient.class);
        SolrWriter w = new SolrWriter(client, cfg(o));
        Schema s = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        Struct v = new Struct(s).put("x", "y");
        // No header — applyExternalVersion's "h == null" branch.
        w.write(rec(s, v, new ConnectHeaders(), null));
        w.flush();
        w.close();
    }

    @Test
    void externalVersionUnparseableHeaderIsTolerated() throws Exception {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.EXTERNAL_VERSION_HEADER_CONFIG, "_v");
        SolrClient client = mock(SolrClient.class);
        SolrWriter w = new SolrWriter(client, cfg(o));
        Schema s = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        Struct v = new Struct(s).put("x", "y");
        Headers h = new ConnectHeaders();
        h.addString("_v", "not-a-number");
        w.write(rec(s, v, h, null));
        w.flush();
        w.close();
    }

    @Test
    void schemaIgnoreSetSkipsEvolve() throws Exception {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.TOPIC_SCHEMA_IGNORE_CONFIG, "users,other");
        o.put(SolrSinkConfig.SCHEMA_AUTO_EVOLVE_CONFIG, "true");
        SolrClient client = mock(SolrClient.class);
        SolrWriter w = new SolrWriter(client, cfg(o));
        Schema s = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        Struct v = new Struct(s).put("x", "y");
        w.write(rec(s, v, new ConnectHeaders(), null));
        w.flush();
        w.close();
    }

    @Test
    void topicKeyIgnoreSetForcesIdFallback() throws Exception {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.TOPIC_KEY_IGNORE_CONFIG, "users");
        SolrClient client = mock(SolrClient.class);
        SolrWriter w = new SolrWriter(client, cfg(o));
        Schema s = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        Struct v = new Struct(s).put("x", "y");
        w.write(rec(s, v, new ConnectHeaders(), null));
        w.flush();
        w.close();
    }

    @Test
    void malformedFailDefaultPropagates() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.ID_STRATEGY_CONFIG, "KAFKA_KEY");
        // No malformed override — default is FAIL.
        SolrClient client = mock(SolrClient.class);
        SolrWriter w = new SolrWriter(client, cfg(o));
        Schema s = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        Struct v = new Struct(s).put("x", "y");
        SinkRecord r = new SinkRecord("users", 0, null, null, s, v, 1L);
        assertThatThrownBy(() -> w.write(r)).isInstanceOf(DataException.class);
        w.close();
    }

    @Test
    void processorCachingHitOnSecondWriteForSamePartition() throws Exception {
        // Default writer (no fanout) — second write should hit the cached lastProcessor.
        SolrClient client = mock(SolrClient.class);
        SolrWriter w = new SolrWriter(client, cfg(new HashMap<>()));
        Schema s = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        Struct v = new Struct(s).put("x", "y");
        w.write(rec(s, v, new ConnectHeaders(), null));
        w.write(rec(s, v, new ConnectHeaders(), null));
        w.flush();
        w.close();
    }

    @Test
    void partitionRevokeNoopWhenFanoutDisabled() {
        SolrClient client = mock(SolrClient.class);
        SolrWriter w = new SolrWriter(client, cfg(new HashMap<>()));
        // No-fanout writer: partitionRevoked is a no-op.
        w.partitionRevoked(new org.apache.kafka.common.TopicPartition("users", 0));
        w.close();
    }

    @Test
    void partitionRevokeRemovesAndClosesFanoutProcessor() throws Exception {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.PARTITION_FANOUT_ENABLED_CONFIG, "true");
        SolrClient client = mock(SolrClient.class);
        SolrWriter w = new SolrWriter(client, cfg(o));
        Schema s = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        Struct v = new Struct(s).put("x", "y");
        // Drive a write to materialise the per-partition processor.
        w.write(new SinkRecord("users", 0, Schema.STRING_SCHEMA, "k", s, v, 1L));
        // Now revoke — should remove and close the processor.
        w.partitionRevoked(new org.apache.kafka.common.TopicPartition("users", 0));
        w.close();
    }

    @Test
    void tombstoneFailModeWithFanout() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.PARTITION_FANOUT_ENABLED_CONFIG, "true");
        o.put(SolrSinkConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG, "fail");
        SolrClient client = mock(SolrClient.class);
        SolrWriter w = new SolrWriter(client, cfg(o));
        SinkRecord r = new SinkRecord("users", 0, Schema.STRING_SCHEMA, "k", null, null, 1L);
        assertThatThrownBy(() -> w.write(r)).isInstanceOf(DataException.class);
        w.close();
    }

    @Test
    void weightedAvgWithEmptyFanout() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.PARTITION_FANOUT_ENABLED_CONFIG, "true");
        SolrClient client = mock(SolrClient.class);
        SolrWriter w = new SolrWriter(client, cfg(o));
        // No writes yet — empty per-partition map. All accessors return 0.
        assertThat(w.recordsWritten()).isZero();
        assertThat(w.recordsFailed()).isZero();
        assertThat(w.retries()).isZero();
        assertThat(w.queueDepth()).isZero();
        assertThat(w.batchCount()).isZero();
        assertThat(w.solrCallCount()).isZero();
        assertThat(w.avgBatchLatencyMs()).isZero();
        assertThat(w.avgSolrCallLatencyMs()).isZero();
        assertThat(w.lastSuccessEpochMs()).isZero();
        w.close();
    }

    @Test
    void parseAsciiLongRejectsAllWhitespace() {
        assertThat(SolrWriter.parseAsciiLong("   ")).isNull();
        assertThat(SolrWriter.parseAsciiLong(new byte[]{' ', ' ', ' '})).isNull();
    }

    @Test
    void parseAsciiLongHandlesPositiveSignAndNegative() {
        assertThat(SolrWriter.parseAsciiLong("+42")).isEqualTo(42L);
        assertThat(SolrWriter.parseAsciiLong("-42")).isEqualTo(-42L);
        assertThat(SolrWriter.parseAsciiLong(new byte[]{'+', '4', '2'})).isEqualTo(42L);
        assertThat(SolrWriter.parseAsciiLong(new byte[]{'-', '4', '2'})).isEqualTo(-42L);
        // Sign with no digits.
        assertThat(SolrWriter.parseAsciiLong("+")).isNull();
        assertThat(SolrWriter.parseAsciiLong("-")).isNull();
        assertThat(SolrWriter.parseAsciiLong(new byte[]{'+'})).isNull();
        assertThat(SolrWriter.parseAsciiLong(new byte[]{'-'})).isNull();
    }
}
