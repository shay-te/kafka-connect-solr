package com.una.kafka.connect.solr;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CollectionResolverTest {

    private SolrSinkConfig config(String strategy, String collection) {
        Map<String, String> props = new HashMap<>();
        props.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://solr:8983/solr");
        if (collection != null) {
            props.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, collection);
        }
        if (strategy != null) {
            props.put(SolrSinkConfig.COLLECTION_NAMING_STRATEGY_CONFIG, strategy);
        }
        return new SolrSinkConfig(props);
    }

    @Test
    void staticAlwaysWins() {
        CollectionResolver r = new CollectionResolver(config("STATIC", "users"));
        assertThat(r.resolve("anything")).isEqualTo("users");
    }

    @Test
    void staticFallsBackToTopicWhenCollectionEmpty() {
        CollectionResolver r = new CollectionResolver(config("STATIC", null));
        assertThat(r.resolve("a-topic")).isEqualTo("a-topic");
    }

    @Test
    void topicFallsBackToTopicWhenCollectionEmpty() {
        CollectionResolver r = new CollectionResolver(config("TOPIC", null));
        assertThat(r.resolve("user-events")).isEqualTo("user-events");
    }

    @Test
    void topicReturnsCollectionWhenSet() {
        CollectionResolver r = new CollectionResolver(config("TOPIC", "users"));
        assertThat(r.resolve("user-events")).isEqualTo("users");
    }

    @Test
    void regexReplacement() {
        CollectionResolver r = new CollectionResolver(config("TOPIC_REGEX", "logs-.*=>logs"));
        assertThat(r.resolve("logs-prod")).isEqualTo("logs");
        assertThat(r.resolve("logs-stage-7")).isEqualTo("logs");
    }

    @Test
    void regexFallsBackToTopicOnNoMatch() {
        CollectionResolver r = new CollectionResolver(config("TOPIC_REGEX", "logs-.*=>logs"));
        assertThat(r.resolve("metrics-prod")).isEqualTo("metrics-prod");
    }

    @Test
    void regexRequiresArrow() {
        assertThatThrownBy(() ->
                new CollectionResolver(config("TOPIC_REGEX", "no-arrow")))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void unknownStrategyTreatedAsTopic() {
        CollectionResolver r = new CollectionResolver(config("BOGUS", "users"));
        assertThat(r.resolve("anything")).isEqualTo("users");
    }

    @Test
    void lowercaseStrategyIsAcceptedLocaleIndependent() {
        // Strategy parse uses Locale.ROOT — "static" must work the same in any JVM locale.
        CollectionResolver r = new CollectionResolver(config("static", "users"));
        assertThat(r.resolve("anything")).isEqualTo("users");
    }

    @Test
    void lastSeenCacheReturnsSameStringForRepeatedTopic() {
        // Two distinct topics interleaved — the last-seen cache should not mix them up.
        CollectionResolver r = new CollectionResolver(config("TOPIC", null));
        String a = r.resolve("topic-a");
        String b = r.resolve("topic-b");
        String aAgain = r.resolve("topic-a");
        assertThat(a).isEqualTo("topic-a");
        assertThat(b).isEqualTo("topic-b");
        assertThat(aAgain).isSameAs(a);
    }

    @Test
    void resolveIsMemoised() {
        // Same topic returned String must be cached (same identity) so the
        // hot path is a single ConcurrentHashMap lookup, no regex match.
        CollectionResolver r = new CollectionResolver(config("TOPIC_REGEX", "logs-.*=>logs"));
        String first = r.resolve("logs-prod");
        String second = r.resolve("logs-prod");
        assertThat(first).isSameAs(second);
    }
}
