package com.una.kafka.connect.solr;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;

import java.util.Map;

/**
 * Hands out {@link OffsetState}s per record and produces the offset map
 * Kafka Connect should commit. Two implementations:
 * <ul>
 *     <li>{@link SyncOffsetTracker} — committed = the highest seen offset
 *         once everything is flushed.</li>
 *     <li>{@link AsyncOffsetTracker} — committed = the longest contiguous
 *         prefix of acked offsets per partition, allowing offsets to advance
 *         without blocking on slow writes.</li>
 * </ul>
 */
public interface OffsetTracker extends AutoCloseable {

    /** Track a record about to be written; the returned state's markAcked() must be called on success. */
    OffsetState track(SinkRecord record);

    /** Returns the offsets safe to commit now (Kafka Connect calls this on preCommit). */
    Map<TopicPartition, OffsetAndMetadata> safeOffsets(Map<TopicPartition, OffsetAndMetadata> currentOffsets);

    /** Drop all bookkeeping for a partition (rebalance / partition revoked). */
    void closePartition(TopicPartition partition);

    @Override
    default void close() { /* no-op by default */ }
}
