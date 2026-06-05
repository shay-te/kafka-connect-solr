package com.una.kafka.connect.solr;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

/**
 * Tracks per-partition pending writes as an in-order deque of
 * {@link OffsetState}s. When preCommit is called we walk the head of each
 * deque popping acked offsets, and commit the highest contiguous offset.
 *
 * <p>Better than the ES connector's {@code AsyncOffsetTracker}:
 * <ul>
 *     <li>Tracks per record (not per batch) so a single slow doc in a
 *         batch doesn't stall the whole batch's offsets.</li>
 *     <li>Drops bookkeeping on closePartition() so rebalances don't leak
 *         memory.</li>
 * </ul></p>
 */
public final class AsyncOffsetTracker implements OffsetTracker {

    private final Map<TopicPartition, Deque<OffsetState>> pending = new HashMap<>();

    @Override
    public synchronized OffsetState track(SinkRecord record) {
        TopicPartition tp = new TopicPartition(record.topic(), record.kafkaPartition());
        OffsetState state = new OffsetState(tp, record.kafkaOffset());
        pending.computeIfAbsent(tp, k -> new ArrayDeque<>()).add(state);
        return state;
    }

    @Override
    public synchronized Map<TopicPartition, OffsetAndMetadata> safeOffsets(
            Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        Map<TopicPartition, OffsetAndMetadata> out = new HashMap<>(currentOffsets);
        for (Map.Entry<TopicPartition, Deque<OffsetState>> e : pending.entrySet()) {
            Deque<OffsetState> queue = e.getValue();
            long lastAcked = -1L;
            while (!queue.isEmpty() && queue.peekFirst().isAcked()) {
                lastAcked = queue.pollFirst().offset();
            }
            if (lastAcked >= 0) {
                // Kafka Connect commits "next-to-read" semantics, so +1.
                out.put(e.getKey(), new OffsetAndMetadata(lastAcked + 1));
            }
        }
        return out;
    }

    @Override
    public synchronized void closePartition(TopicPartition partition) {
        pending.remove(partition);
    }

    public synchronized int pendingCount() {
        int n = 0;
        for (Deque<OffsetState> q : pending.values()) n += q.size();
        return n;
    }
}
