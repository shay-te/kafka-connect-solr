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
        // Partition 1 has unacked work — must be pinned at the earliest unacked offset
        // so a restart re-delivers from there. Without this, Connect would commit
        // currentOffsets[tp] (the next-to-read) and silently lose b0.
        assertThat(safe.get(new TopicPartition("t", 1)).offset()).isEqualTo(9L);
        b0.markAcked();
        safe = tracker.safeOffsets(new HashMap<>());
        assertThat(safe.get(new TopicPartition("t", 1)).offset()).isEqualTo(10L);
    }

    @Test
    void noAcksYetPinsToEarliestTrackedOffset() {
        // Regression: previously when nothing was acked, the tracker left
        // currentOffsets[tp] unchanged, causing Kafka Connect to commit past
        // unacked records and lose them on restart.
        AsyncOffsetTracker tracker = new AsyncOffsetTracker();
        tracker.track(r(0, 5));
        tracker.track(r(0, 6));
        tracker.track(r(0, 7));

        Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();
        currentOffsets.put(new TopicPartition("t", 0), new OffsetAndMetadata(8L));

        Map<TopicPartition, OffsetAndMetadata> safe = tracker.safeOffsets(currentOffsets);
        assertThat(safe.get(new TopicPartition("t", 0)).offset())
                .as("With unacked work, commit must pin to earliest unacked (5), not the consumer's next-to-read (8)")
                .isEqualTo(5L);
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

    @Test
    void emptyDequeAfterFullDrainLeavesCurrentOffsetsAlone() {
        // After all tracked records are acked and polled, the partition's deque
        // is empty but the partition key remains in `pending`. A subsequent
        // safeOffsets call with NEW currentOffsets should leave them untouched
        // since lastAcked stays -1 (loop doesn't iterate) and queue is empty.
        AsyncOffsetTracker tracker = new AsyncOffsetTracker();
        OffsetState s = tracker.track(r(0, 5));
        s.markAcked();
        // First call drains the deque.
        tracker.safeOffsets(new HashMap<>());

        // Second call with different currentOffsets - partition's deque is now
        // empty and lastAcked=-1; out[tp] stays as currentOffsets[tp].
        Map<TopicPartition, OffsetAndMetadata> current = new HashMap<>();
        current.put(new TopicPartition("t", 0), new OffsetAndMetadata(99L));
        Map<TopicPartition, OffsetAndMetadata> safe = tracker.safeOffsets(current);
        assertThat(safe.get(new TopicPartition("t", 0)).offset()).isEqualTo(99L);
    }

    @Test
    void pendingCountCountsAllTracked() {
        AsyncOffsetTracker tracker = new AsyncOffsetTracker();
        tracker.track(r(0, 0));
        tracker.track(r(0, 1));
        tracker.track(r(1, 0));
        assertThat(tracker.pendingCount()).isEqualTo(3);
    }
}
