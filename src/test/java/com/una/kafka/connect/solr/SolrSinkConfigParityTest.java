package com.una.kafka.connect.solr;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests imported from kafka-connect-elasticsearch and adapted to the
 * config surface of the Solr sink. See docs/test-parity.md for the full
 * mapping.
 */
class SolrSinkConfigParityTest {

    private Map<String, String> base() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://solr:8983/solr");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "users");
        return p;
    }

    /** Parity for ES testSecured: basic-auth credentials round-trip. */
    @Test
    void basicAuthIsConfigurable() {
        Map<String, String> p = base();
        p.put(SolrSinkConfig.CONNECTION_USERNAME_CONFIG, "alice");
        p.put(SolrSinkConfig.CONNECTION_PASSWORD_CONFIG, "s3cret");
        SolrSinkConfig cfg = new SolrSinkConfig(p);
        assertThat(cfg.username()).isEqualTo("alice");
        assertThat(cfg.password()).isEqualTo("s3cret");
    }

    /** Parity for ES testSetHttpTimeoutsConfig. */
    @Test
    void customTimeoutsAreApplied() {
        Map<String, String> p = base();
        p.put(SolrSinkConfig.CONNECTION_TIMEOUT_MS_CONFIG, "12000");
        p.put(SolrSinkConfig.READ_TIMEOUT_MS_CONFIG, "90000");
        SolrSinkConfig cfg = new SolrSinkConfig(p);
        assertThat(cfg.connectionTimeoutMs()).isEqualTo(12_000);
        assertThat(cfg.readTimeoutMs()).isEqualTo(90_000);
    }

    /** Parity for ES testValidLingerMs / testInvalidLingerMs. */
    @Test
    void lingerMsBoundaries() {
        Map<String, String> p = base();
        p.put(SolrSinkConfig.LINGER_MS_CONFIG, "1");
        assertThat(new SolrSinkConfig(p).lingerMs()).isEqualTo(1L);
        p.put(SolrSinkConfig.LINGER_MS_CONFIG, "5000");
        assertThat(new SolrSinkConfig(p).lingerMs()).isEqualTo(5_000L);
        p.put(SolrSinkConfig.LINGER_MS_CONFIG, "not-a-number");
        assertThatThrownBy(() -> new SolrSinkConfig(p))
                .isInstanceOf(RuntimeException.class);
    }

    /** Parity for ES testValidMaxBufferedRecords / testInvalidMaxBufferedRecords. */
    @Test
    void maxBufferedRecordsBoundaries() {
        Map<String, String> p = base();
        p.put(SolrSinkConfig.MAX_BUFFERED_RECORDS_CONFIG, "1");
        assertThat(new SolrSinkConfig(p).maxBufferedRecords()).isEqualTo(1);
        p.put(SolrSinkConfig.MAX_BUFFERED_RECORDS_CONFIG, "1000000");
        assertThat(new SolrSinkConfig(p).maxBufferedRecords()).isEqualTo(1_000_000);
        p.put(SolrSinkConfig.MAX_BUFFERED_RECORDS_CONFIG, "garbage");
        assertThatThrownBy(() -> new SolrSinkConfig(p))
                .isInstanceOf(RuntimeException.class);
    }

    /** Parity for ES testValidIgnoreConfigs / testInvalidIgnoreConfigs. */
    @Test
    void ignoreFlagsParseBooleans() {
        Map<String, String> p = base();
        p.put(SolrSinkConfig.KEY_IGNORE_CONFIG, "true");
        p.put(SolrSinkConfig.SCHEMA_IGNORE_CONFIG, "true");
        SolrSinkConfig cfg = new SolrSinkConfig(p);
        assertThat(cfg.keyIgnore()).isTrue();
        assertThat(cfg.schemaIgnore()).isTrue();
    }
}
