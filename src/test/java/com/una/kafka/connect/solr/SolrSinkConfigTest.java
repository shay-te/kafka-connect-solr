package com.una.kafka.connect.solr;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SolrSinkConfigTest {

    private Map<String, String> base() {
        Map<String, String> props = new HashMap<>();
        props.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://solr:8983/solr");
        props.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "users");
        return props;
    }

    @Test
    void defaultsAreSensible() {
        SolrSinkConfig cfg = new SolrSinkConfig(base());
        assertThat(cfg.batchSize()).isEqualTo(2000);
        assertThat(cfg.maxInFlight()).isEqualTo(8);
        assertThat(cfg.behaviorOnNullValues()).isEqualTo(SolrSinkConfig.BehaviorOnNullValues.IGNORE);
        assertThat(cfg.behaviorOnMalformed()).isEqualTo(SolrSinkConfig.BehaviorOnMalformed.FAIL);
        assertThat(cfg.writeMethod()).isEqualTo(SolrSinkConfig.WriteMethod.INDEX);
        assertThat(cfg.idStrategy()).isEqualTo(SolrSinkConfig.IdStrategy.KAFKA_KEY);
        assertThat(cfg.isCloud()).isFalse();
        assertThat(cfg.idField()).isEqualTo("id");
        assertThat(cfg.lingerMs()).isEqualTo(50L);
        assertThat(cfg.flushTimeoutMs()).isEqualTo(30_000L);
        assertThat(cfg.maxBufferedRecords()).isEqualTo(20_000);
        assertThat(cfg.maxRetries()).isEqualTo(5);
        assertThat(cfg.retryBackoffMs()).isEqualTo(200L);
        assertThat(cfg.commitWithinMs()).isEqualTo(1_000L);
        assertThat(cfg.connectionTimeoutMs()).isEqualTo(5_000);
        assertThat(cfg.readTimeoutMs()).isEqualTo(60_000);
        assertThat(cfg.collectionNamingStrategy()).isEqualTo("TOPIC");
        assertThat(cfg.keyIgnore()).isFalse();
        assertThat(cfg.schemaIgnore()).isFalse();
        assertThat(cfg.schemaAutoCreate()).isFalse();
        assertThat(cfg.schemaAutoEvolve()).isFalse();
        assertThat(cfg.username()).isEmpty();
        assertThat(cfg.password()).isEmpty();
        assertThat(cfg.zkHost()).isEmpty();
        assertThat(cfg.defaultCollection()).isEqualTo("users");
        assertThat(cfg.solrUrls()).containsExactly("http://solr:8983/solr");
    }

    @Test
    void cloudWhenZkHostSet() {
        Map<String, String> props = new HashMap<>();
        props.put(SolrSinkConfig.SOLR_ZK_HOST_CONFIG, "zk:2181/solr");
        props.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "users");
        assertThat(new SolrSinkConfig(props).isCloud()).isTrue();
    }

    @Test
    void enumParsersCaseInsensitive() {
        Map<String, String> props = base();
        props.put(SolrSinkConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG, "DELETE");
        props.put(SolrSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, "warn");
        props.put(SolrSinkConfig.WRITE_METHOD_CONFIG, "upsert");
        props.put(SolrSinkConfig.ID_STRATEGY_CONFIG, "uuid");
        SolrSinkConfig cfg = new SolrSinkConfig(props);
        assertThat(cfg.behaviorOnNullValues()).isEqualTo(SolrSinkConfig.BehaviorOnNullValues.DELETE);
        assertThat(cfg.behaviorOnMalformed()).isEqualTo(SolrSinkConfig.BehaviorOnMalformed.WARN);
        assertThat(cfg.writeMethod()).isEqualTo(SolrSinkConfig.WriteMethod.UPSERT);
        assertThat(cfg.idStrategy()).isEqualTo(SolrSinkConfig.IdStrategy.UUID);
    }

    @Test
    void allEnumValuesParse() {
        for (SolrSinkConfig.BehaviorOnNullValues v : SolrSinkConfig.BehaviorOnNullValues.values()) {
            assertThat(SolrSinkConfig.BehaviorOnNullValues.parse(v.name())).isEqualTo(v);
        }
        for (SolrSinkConfig.BehaviorOnMalformed v : SolrSinkConfig.BehaviorOnMalformed.values()) {
            assertThat(SolrSinkConfig.BehaviorOnMalformed.parse(v.name())).isEqualTo(v);
        }
        for (SolrSinkConfig.WriteMethod v : SolrSinkConfig.WriteMethod.values()) {
            assertThat(SolrSinkConfig.WriteMethod.parse(v.name())).isEqualTo(v);
        }
        for (SolrSinkConfig.IdStrategy v : SolrSinkConfig.IdStrategy.values()) {
            assertThat(SolrSinkConfig.IdStrategy.parse(v.name())).isEqualTo(v);
        }
    }

    @Test
    void enumParserRejectsBadValue() {
        assertThatThrownBy(() -> SolrSinkConfig.BehaviorOnNullValues.parse("nope"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void passwordIsAccessible() {
        Map<String, String> props = base();
        props.put(SolrSinkConfig.CONNECTION_PASSWORD_CONFIG, "secret");
        SolrSinkConfig cfg = new SolrSinkConfig(props);
        assertThat(cfg.password()).isEqualTo("secret");
    }

    @Test
    void configDefIsConstructible() {
        // Sanity: ConfigDef build doesn't throw.
        assertThat(SolrSinkConfig.config()).isNotNull();
    }
}
