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
            long lastAcked = -1L;
            while (!queue.isEmpty() && queue.peekFirst().isAcked()) {
                lastAcked = queue.pollFirst().offset();
            }
            if (!queue.isEmpty()) {
                // Has unacked work — pin commit to the earliest unacked offset
                // so on restart we re-deliver from there. Without this,
                // currentOffsets[tp] (the consumer's next-to-read) would be
                // committed and any unacked records before it would be lost.
                out.put(e.getKey(), new OffsetAndMetadata(queue.peekFirst().offset()));
            } else if (lastAcked >= 0) {
                // All tracked records acked — commit just past the last one.
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
