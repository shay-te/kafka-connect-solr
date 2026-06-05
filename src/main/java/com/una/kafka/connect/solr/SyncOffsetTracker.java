package com.una.kafka.connect.solr;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Map;

/**
 * The conservative tracker. Hands out a transient OffsetState (just for
 * API symmetry) and trusts the caller to have flushed every write before
 * preCommit is called. Returns the offsets Kafka Connect handed us
 * verbatim.
 */
public class SyncOffsetTracker implements OffsetTracker {

    @Override
    public OffsetState track(SinkRecord record) {
        return new OffsetState(
                new TopicPartition(record.topic(), record.kafkaPartition()),
                record.kafkaOffset());
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
