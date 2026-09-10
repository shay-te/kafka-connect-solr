package com.una.kafka.connect.solr;

import org.apache.kafka.common.TopicPartition;

// volatile (not AtomicBoolean) is sufficient: one writer per record (the bulk worker),
// one reader (preCommit). Skipping the wrapper saves one alloc per record in async mode.
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
