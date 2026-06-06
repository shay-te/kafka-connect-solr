package com.una.kafka.connect.solr;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exhausts Validator's branch table - empty configs, exclusive connection,
 * partial Kerberos, SSL file checks, regex-collection arrow, throughput
 * positivity gates.
 */
class ValidatorCoverageTest {

    private Config validate(Map<String, String> props) {
        Config base = new Config(SolrSinkConfig.config().validate(props));
        return Validator.validate(base, props);
    }

    private List<String> errors(Config cfg, String name) {
        for (ConfigValue cv : cfg.configValues()) {
            if (cv.name().equals(name)) return cv.errorMessages();
        }
        return java.util.Collections.emptyList();
    }

    @Test
    void noUrlOrZkErrors() {
        Map<String, String> p = new HashMap<>();
        Config cfg = validate(p);
        assertThat(errors(cfg, SolrSinkConfig.SOLR_URL_CONFIG))
                .anyMatch(s -> s.contains("Set either"));
    }

    @Test
    void bothUrlAndZkErrors() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.SOLR_ZK_HOST_CONFIG, "zk:2181");
        Config cfg = validate(p);
        assertThat(errors(cfg, SolrSinkConfig.SOLR_URL_CONFIG))
                .anyMatch(s -> s.contains("not both"));
    }

    @Test
    void partialKerberosPrincipalOnlyErrors() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.KERBEROS_PRINCIPAL_CONFIG, "u@R");
        Config cfg = validate(p);
        assertThat(errors(cfg, SolrSinkConfig.KERBEROS_PRINCIPAL_CONFIG)).isNotEmpty();
    }

    @Test
    void partialKerberosKeytabOnlyErrors() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.KERBEROS_KEYTAB_PATH_CONFIG, "/nonexistent.keytab");
        Config cfg = validate(p);
        assertThat(errors(cfg, SolrSinkConfig.KERBEROS_KEYTAB_PATH_CONFIG)).isNotEmpty();
    }

    @Test
    void kerberosKeytabFileMissingErrors() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.KERBEROS_PRINCIPAL_CONFIG, "u@R");
        p.put(SolrSinkConfig.KERBEROS_KEYTAB_PATH_CONFIG, "/path/does/not/exist");
        Config cfg = validate(p);
        assertThat(errors(cfg, SolrSinkConfig.KERBEROS_KEYTAB_PATH_CONFIG))
                .anyMatch(s -> s.contains("not found"));
    }

    @Test
    void sslWithMissingKeystoreFileErrors() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.SECURITY_PROTOCOL_CONFIG, "SSL");
        p.put(SolrSinkConfig.SSL_KEYSTORE_LOCATION_CONFIG, "/no/such/keystore");
        Config cfg = validate(p);
        assertThat(errors(cfg, SolrSinkConfig.SSL_KEYSTORE_LOCATION_CONFIG))
                .anyMatch(s -> s.contains("not found"));
    }

    @Test
    void sslDisabledSkipsFileChecks() {
        // PLAINTEXT - no SSL files required.
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        Config cfg = validate(p);
        assertThat(errors(cfg, SolrSinkConfig.SSL_KEYSTORE_LOCATION_CONFIG)).isEmpty();
    }

    @Test
    void maxBufferedRecordsLessThanBatchSizeErrors() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.BATCH_SIZE_CONFIG, "100");
        p.put(SolrSinkConfig.MAX_BUFFERED_RECORDS_CONFIG, "50");
        Config cfg = validate(p);
        assertThat(errors(cfg, SolrSinkConfig.MAX_BUFFERED_RECORDS_CONFIG))
                .anyMatch(s -> s.contains(">= batch.size"));
    }

    @Test
    void zeroBatchSizeErrors() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.BATCH_SIZE_CONFIG, "0");
        Config cfg = validate(p);
        assertThat(errors(cfg, SolrSinkConfig.BATCH_SIZE_CONFIG))
                .anyMatch(s -> s.contains(">= 1"));
    }

    @Test
    void zeroInFlightErrors() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG, "0");
        Config cfg = validate(p);
        assertThat(errors(cfg, SolrSinkConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG))
                .anyMatch(s -> s.contains(">= 1"));
    }

    @Test
    void zeroFlushTimeoutErrors() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.FLUSH_TIMEOUT_MS_CONFIG, "0");
        Config cfg = validate(p);
        assertThat(errors(cfg, SolrSinkConfig.FLUSH_TIMEOUT_MS_CONFIG))
                .anyMatch(s -> s.contains("throw immediately"));
    }

    @Test
    void negativeLingerErrors() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.LINGER_MS_CONFIG, "-1");
        Config cfg = validate(p);
        assertThat(errors(cfg, SolrSinkConfig.LINGER_MS_CONFIG))
                .anyMatch(s -> s.contains(">= 0"));
    }

    @Test
    void negativeMaxRetriesErrors() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.MAX_RETRIES_CONFIG, "-1");
        Config cfg = validate(p);
        assertThat(errors(cfg, SolrSinkConfig.MAX_RETRIES_CONFIG))
                .anyMatch(s -> s.contains(">= 0"));
    }

    @Test
    void regexCollectionWithoutArrowErrors() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.COLLECTION_NAMING_STRATEGY_CONFIG, "TOPIC_REGEX");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "no-arrow");
        Config cfg = validate(p);
        assertThat(errors(cfg, SolrSinkConfig.SOLR_COLLECTION_CONFIG))
                .anyMatch(s -> s.contains("pattern=>replacement"));
    }

    @Test
    void regexCollectionWithArrowAccepted() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.COLLECTION_NAMING_STRATEGY_CONFIG, "TOPIC_REGEX");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "logs-.*=>logs");
        Config cfg = validate(p);
        // No error message about pattern=>replacement.
        assertThat(errors(cfg, SolrSinkConfig.SOLR_COLLECTION_CONFIG))
                .noneMatch(s -> s.contains("pattern=>replacement"));
    }

    @Test
    void nonRegexStrategyAcceptsCollection() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.COLLECTION_NAMING_STRATEGY_CONFIG, "TOPIC");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "logs");
        Config cfg = validate(p);
        assertThat(errors(cfg, SolrSinkConfig.SOLR_COLLECTION_CONFIG)).isEmpty();
    }
}
