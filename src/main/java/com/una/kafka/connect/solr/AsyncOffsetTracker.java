package com.una.kafka.connect.solr;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

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
            while (!queue.isEmpty() && queue.peekFirst().isAcked()) {
                queue.pollFirst();
            }
            if (!queue.isEmpty()) {
                // Earliest unacked record — pin commit there so on restart we
                // re-deliver from this offset. Without this, currentOffsets[tp]
                // (the consumer's next-to-read) would be committed and any
                // unacked records before it would be silently lost.
                out.put(e.getKey(), new OffsetAndMetadata(queue.peekFirst().offset()));
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
