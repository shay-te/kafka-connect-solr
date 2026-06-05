package com.una.kafka.connect.solr;

import org.apache.kafka.common.TopicPartition;

/**
 * One pending record's write state. {@code volatile} on the ack flag is
 * sufficient because there is exactly one writer (the bulk worker that
 * processes the record's batch) and only the preCommit thread reads it.
 * Saves the AtomicBoolean wrapper object per record - measurable at
 * high record rates because this allocation happens on every successful
 * write in async mode.
 */
public final class OffsetState {

    private final TopicPartition partition;
    private final long offset;
    private volatile boolean acked;

    public OffsetState(TopicPartition partition, long offset) {
        this.partition = partition;
        this.offset = offset;
    }

    public TopicPartition partition() {
        return partition;
    }

    public long offset() {
        return offset;
    }

    public boolean isAcked() {
        return acked;
    }

    public void markAcked() {
        acked = true;
    }

    @Override
    public String toString() {
        return partition + "@" + offset + (acked ? "*" : "");
    }
}
