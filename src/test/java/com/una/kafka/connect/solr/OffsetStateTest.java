package com.una.kafka.connect.solr;

import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class OffsetStateTest {

    @Test
    void exposesConstructorArgs() {
        TopicPartition tp = new TopicPartition("t", 3);
        OffsetState s = new OffsetState(tp, 42L);
        assertThat(s.partition()).isSameAs(tp);
        assertThat(s.offset()).isEqualTo(42L);
        assertThat(s.isAcked()).isFalse();
    }

    @Test
    void markAckedFlipsTheFlag() {
        OffsetState s = new OffsetState(new TopicPartition("t", 0), 1L);
        s.markAcked();
        assertThat(s.isAcked()).isTrue();
    }

    @Test
    void toStringPrintsUnackedForm() {
        OffsetState s = new OffsetState(new TopicPartition("t", 0), 7L);
        assertThat(s.toString()).isEqualTo("t-0@7");
    }

    @Test
    void toStringMarksAckedWithStar() {
        OffsetState s = new OffsetState(new TopicPartition("t", 1), 9L);
        s.markAcked();
        assertThat(s.toString()).isEqualTo("t-1@9*");
    }
}
