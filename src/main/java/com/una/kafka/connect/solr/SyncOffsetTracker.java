package com.una.kafka.connect.solr;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Map;

/**
 * The conservative tracker. The sync path never reads {@code isAcked()} on
 * the returned {@link OffsetState} (preCommit waits for all in-flight
 * writes to complete via {@code flushSync}), so we hand back a shared
 * sentinel and skip one allocation per record. {@code markAcked()} on the
 * sentinel is a no-op because the field is shared and rapidly toggled by
 * different workers - which is fine, nobody reads it.
 */
public final class SyncOffsetTracker implements OffsetTracker {

    /** Reusable across every record; safe because the sync path never reads .isAcked(). */
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
        // nothing to drop
    }
}
