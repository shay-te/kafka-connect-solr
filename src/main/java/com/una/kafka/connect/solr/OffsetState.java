package com.una.kafka.connect.solr;

import org.apache.kafka.common.TopicPartition;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * One pending record's write state. Lets the async offset tracker advance
 * a partition's committed offset only as far as the contiguous prefix of
 * records that have actually been acked by Solr.
 */
public class OffsetState {

    private final TopicPartition partition;
    private final long offset;
    private final AtomicBoolean acked = new AtomicBoolean(false);

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
        return acked.get();
    }

    public void markAcked() {
        acked.set(true);
    }

    @Override
    public String toString() {
        return partition + "@" + offset + (acked.get() ? "*" : "");
    }
}
