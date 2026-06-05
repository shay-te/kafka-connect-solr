package com.una.kafka.connect.solr;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class AsyncOffsetTrackerTest {

    private SinkRecord r(int partition, long offset) {
        return new SinkRecord("t", partition, Schema.STRING_SCHEMA, "k", null, "v", offset);
    }

    @Test
    void happyPath() {
        AsyncOffsetTracker tracker = new AsyncOffsetTracker();
        OffsetState s0 = tracker.track(r(0, 0));
        OffsetState s1 = tracker.track(r(0, 1));
        OffsetState s2 = tracker.track(r(0, 2));
        s0.markAcked();
        s1.markAcked();
        s2.markAcked();
        Map<TopicPartition, OffsetAndMetadata> safe = tracker.safeOffsets(new HashMap<>());
        assertThat(safe.get(new TopicPartition("t", 0)).offset()).isEqualTo(3L);
    }

    @Test
    void belowWatermark() {
        // Only the contiguous prefix is committed.
        AsyncOffsetTracker tracker = new AsyncOffsetTracker();
        OffsetState s0 = tracker.track(r(0, 0));
        OffsetState s1 = tracker.track(r(0, 1));
        OffsetState s2 = tracker.track(r(0, 2));
        s0.markAcked();
        // s1 not acked - s2 is acked but should NOT advance past s1.
        s2.markAcked();
        Map<TopicPartition, OffsetAndMetadata> safe = tracker.safeOffsets(new HashMap<>());
        assertThat(safe.get(new TopicPartition("t", 0)).offset()).isEqualTo(1L);
        // Now ack s1 -> everything contiguous up to s2 commits.
        s1.markAcked();
        safe = tracker.safeOffsets(new HashMap<>());
        assertThat(safe.get(new TopicPartition("t", 0)).offset()).isEqualTo(3L);
    }

    @Test
    void multiplePartitionsIndependent() {
        AsyncOffsetTracker tracker = new AsyncOffsetTracker();
        OffsetState a0 = tracker.track(r(0, 5));
        OffsetState b0 = tracker.track(r(1, 9));
        a0.markAcked();
        Map<TopicPartition, OffsetAndMetadata> safe = tracker.safeOffsets(new HashMap<>());
        assertThat(safe.get(new TopicPartition("t", 0)).offset()).isEqualTo(6L);
        assertThat(safe.containsKey(new TopicPartition("t", 1))).isFalse();
        b0.markAcked();
        safe = tracker.safeOffsets(new HashMap<>());
        assertThat(safe.get(new TopicPartition("t", 1)).offset()).isEqualTo(10L);
    }

    @Test
    void rebalanceDropsPartition() {
        AsyncOffsetTracker tracker = new AsyncOffsetTracker();
        tracker.track(r(0, 0));
        tracker.track(r(0, 1));
        assertThat(tracker.pendingCount()).isEqualTo(2);
        tracker.closePartition(new TopicPartition("t", 0));
        assertThat(tracker.pendingCount()).isZero();
    }

    @Test
    void noOpReturnsCurrentOffsets() {
        AsyncOffsetTracker tracker = new AsyncOffsetTracker();
        Map<TopicPartition, OffsetAndMetadata> current = new HashMap<>();
        current.put(new TopicPartition("t", 0), new OffsetAndMetadata(50L));
        Map<TopicPartition, OffsetAndMetadata> safe = tracker.safeOffsets(current);
        assertThat(safe).isEqualTo(current);
    }
}
