package com.una.kafka.connect.solr;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Map;

// Sync path never reads isAcked() (flushSync waits for all in-flight writes),
// so return a shared sentinel and skip the per-record OffsetState allocation.
public final class SyncOffsetTracker implements OffsetTracker {

    private static final OffsetState SENTINEL = new OffsetState(new TopicPartition("", -1), -1L);

    @Override
    public OffsetState track(SinkRecord record) {
        return SENTINEL;
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> safeOffsets(
            Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        return currentOffsets;
    }

    @Override
    public void closePartition(TopicPartition partition) {
    }
}
