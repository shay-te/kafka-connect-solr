package com.una.kafka.connect.solr;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SolrSinkConfigTest {

    @Test
    void defaultsAreSensible() {
        Map<String, String> props = new HashMap<>();
        props.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://solr:8983/solr");
        props.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "users");
        SolrSinkConfig cfg = new SolrSinkConfig(props);

        assertThat(cfg.batchSize()).isEqualTo(2000);
        assertThat(cfg.maxInFlight()).isEqualTo(8);
        assertThat(cfg.behaviorOnNullValues()).isEqualTo(SolrSinkConfig.BehaviorOnNullValues.IGNORE);
        assertThat(cfg.writeMethod()).isEqualTo(SolrSinkConfig.WriteMethod.INDEX);
        assertThat(cfg.idStrategy()).isEqualTo(SolrSinkConfig.IdStrategy.KAFKA_KEY);
        assertThat(cfg.isCloud()).isFalse();
    }

    @Test
    void cloudWhenZkHostSet() {
        Map<String, String> props = new HashMap<>();
        props.put(SolrSinkConfig.SOLR_ZK_HOST_CONFIG, "zk:2181/solr");
        props.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "users");
        assertThat(new SolrSinkConfig(props).isCloud()).isTrue();
    }
}
