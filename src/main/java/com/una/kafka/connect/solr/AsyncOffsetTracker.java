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

    // Hot-path cache: same (topic, partition) typically repeats for many consecutive records,
    // so we avoid allocating a fresh TopicPartition just to look up the deque each time.
    private String lastTopic;
    private int lastPartition = -1;
    private TopicPartition lastTp;
    private Deque<OffsetState> lastDeque;

    @Override
    public synchronized OffsetState track(SinkRecord record) {
        String topic = record.topic();
        int partition = record.kafkaPartition();
        TopicPartition tp;
        Deque<OffsetState> queue;
        if (partition == lastPartition && topic.equals(lastTopic) && lastDeque != null) {
            tp = lastTp;
            queue = lastDeque;
        } else {
            tp = new TopicPartition(topic, partition);
            queue = pending.computeIfAbsent(tp, k -> new ArrayDeque<>());
            lastTopic = topic;
            lastPartition = partition;
            lastTp = tp;
            lastDeque = queue;
        }
        OffsetState state = new OffsetState(tp, record.kafkaOffset());
        queue.add(state);
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
        if (partition.equals(lastTp)) {
            lastTp = null;
            lastDeque = null;
            lastTopic = null;
            lastPartition = -1;
        }
    }

    public synchronized int pendingCount() {
        int n = 0;
        for (Deque<OffsetState> q : pending.values()) n += q.size();
        return n;
    }
}
