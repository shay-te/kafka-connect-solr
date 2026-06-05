package com.una.kafka.connect.solr;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class CollectionResolverTest {

    @Test
    void staticAlwaysWins() {
        Map<String, String> props = new HashMap<>();
        props.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://solr:8983/solr");
        props.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "users");
        props.put(SolrSinkConfig.COLLECTION_NAMING_STRATEGY_CONFIG, "STATIC");
        CollectionResolver r = new CollectionResolver(new SolrSinkConfig(props));
        assertThat(r.resolve("anything")).isEqualTo("users");
    }

    @Test
    void topicFallsBackToTopicWhenCollectionEmpty() {
        Map<String, String> props = new HashMap<>();
        props.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://solr:8983/solr");
        CollectionResolver r = new CollectionResolver(new SolrSinkConfig(props));
        assertThat(r.resolve("user-events")).isEqualTo("user-events");
    }

    @Test
    void regexReplacement() {
        Map<String, String> props = new HashMap<>();
        props.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://solr:8983/solr");
        props.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "logs-.*=>logs");
        props.put(SolrSinkConfig.COLLECTION_NAMING_STRATEGY_CONFIG, "TOPIC_REGEX");
        CollectionResolver r = new CollectionResolver(new SolrSinkConfig(props));
        assertThat(r.resolve("logs-prod")).isEqualTo("logs");
        assertThat(r.resolve("logs-stage-7")).isEqualTo("logs");
    }
}
