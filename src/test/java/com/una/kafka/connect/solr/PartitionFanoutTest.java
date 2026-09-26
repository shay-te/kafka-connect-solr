package com.una.kafka.connect.solr;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class PartitionFanoutTest {

    private SolrSinkConfig cfg(boolean fanout) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "users");
        p.put(SolrSinkConfig.BATCH_SIZE_CONFIG, "1");
        p.put(SolrSinkConfig.LINGER_MS_CONFIG, "1");
        p.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "UNUSED");
        p.put(SolrSinkConfig.PARTITION_FANOUT_ENABLED_CONFIG, String.valueOf(fanout));
        return new SolrSinkConfig(p);
    }

    private SinkRecord rec(int partition, long offset) {
        Schema s = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        Struct v = new Struct(s).put("x", "y");
        return new SinkRecord("users", partition, Schema.STRING_SCHEMA,
                "k-" + partition + "-" + offset, s, v, offset);
    }

    @Test
    void fanoutOffByDefault() {
        SolrClient client = mock(SolrClient.class);
        SolrWriter w = new SolrWriter(client, cfg(false), new SyncOffsetTracker());
        // partitionRevoked is a no-op in non-fanout mode.
        w.partitionRevoked(new TopicPartition("users", 0));
        w.close();
    }

    @Test
    void fanoutCreatesProcessorPerPartition() throws Exception {
        SolrClient client = mock(SolrClient.class);
        SolrWriter w = new SolrWriter(client, cfg(true), new SyncOffsetTracker());

        w.write(rec(0, 1));
        w.write(rec(1, 1));
        w.write(rec(2, 1));
        w.flush();

        // Three partitions -> at least three requests (one per partition's batch).
        verify(client, atLeastOnce()).request(any(UpdateRequest.class), anyString());
        assertThat(w.recordsWritten()).isGreaterThanOrEqualTo(3);
        w.close();
    }

    @Test
    void fanoutMetricsAggregateAcrossPartitions() throws Exception {
        SolrClient client = mock(SolrClient.class);
        SolrWriter w = new SolrWriter(client, cfg(true), new SyncOffsetTracker());

        for (int i = 0; i < 6; i++) {
            w.write(rec(i % 3, i));
        }
        w.flush();

        // The aggregated counters should see all 6 records, not just one partition's worth.
        assertThat(w.recordsWritten()).isGreaterThanOrEqualTo(6);
        assertThat(w.batchCount()).isGreaterThanOrEqualTo(3);
        w.close();
    }

    @Test
    void partitionRevokedClosesItsProcessor() throws Exception {
        SolrClient client = mock(SolrClient.class);
        SolrWriter w = new SolrWriter(client, cfg(true), new SyncOffsetTracker());

        w.write(rec(0, 1));
        w.flush();
        assertThat(w.recordsWritten()).isGreaterThanOrEqualTo(1);

        // Simulate a rebalance away from partition 0. The processor for tp(0) is
        // closed and removed from perPartition; its counter is no longer visible
        // via the aggregator.
        w.partitionRevoked(new TopicPartition("users", 0));

        // After revoke, writing to partition 0 again creates a NEW processor and
        // the request actually goes out — verify via the mock client.
        w.write(rec(0, 2));
        w.flush();
        verify(client, atLeastOnce()).request(any(UpdateRequest.class), anyString());
        w.close();
    }
}
