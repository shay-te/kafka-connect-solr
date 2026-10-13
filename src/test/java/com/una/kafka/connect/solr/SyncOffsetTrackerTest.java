package com.una.kafka.connect.solr;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SyncOffsetTrackerTest {

    private SinkRecord r(int partition, long offset) {
        return new SinkRecord("topic", partition, Schema.STRING_SCHEMA, "k", null, "v", offset);
    }

    @Test
    void trackReturnsAReusableSentinel() {
        // Sync mode skips per-record OffsetState allocation. The bulk
        // worker calls markAcked() on the returned state but no one reads
        // it - flushSync waits for the actual Future to complete.
        SyncOffsetTracker t = new SyncOffsetTracker();
        OffsetState a = t.track(r(0, 1));
        OffsetState b = t.track(r(1, 99));
        assertThat(a).isNotNull();
        assertThat(a).isSameAs(b);
    }

    @Test
    void safeOffsetsReturnsCurrent() {
        SyncOffsetTracker t = new SyncOffsetTracker();
        Map<TopicPartition, OffsetAndMetadata> current = new HashMap<>();
        current.put(new TopicPartition("topic", 0), new OffsetAndMetadata(42L));
        assertThat(t.safeOffsets(current)).isSameAs(current);
    }

    @Test
    void closePartitionIsNoop() {
        new SyncOffsetTracker().closePartition(new TopicPartition("t", 0));
    }
}
