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

    @Test
    void trackReturnsAState() {
        SyncOffsetTracker t = new SyncOffsetTracker();
        OffsetState s = t.track(new SinkRecord("topic", 1, Schema.STRING_SCHEMA, "k", null, "v", 9L));
        assertThat(s.partition()).isEqualTo(new TopicPartition("topic", 1));
        assertThat(s.offset()).isEqualTo(9L);
        assertThat(s.isAcked()).isFalse();
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
