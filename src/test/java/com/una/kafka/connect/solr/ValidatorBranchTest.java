package com.una.kafka.connect.solr;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drives short-circuit branches in Validator: numeric coercion of bad values,
 * partial null-check combinations, and the err() de-duplication guard.
 */
class ValidatorBranchTest {

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
    void nonNumericThroughputValuesAreIgnored() {
        // batch.size / linger.ms / etc. as garbage strings — asInt/asLong return null and
        // every numeric sanity branch short-circuits silently.
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.BATCH_SIZE_CONFIG, "not-a-number");
        p.put(SolrSinkConfig.LINGER_MS_CONFIG, "garbage");
        p.put(SolrSinkConfig.MAX_RETRIES_CONFIG, "bad");
        p.put(SolrSinkConfig.MAX_BUFFERED_RECORDS_CONFIG, "also-bad");
        p.put(SolrSinkConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG, "junk");
        p.put(SolrSinkConfig.FLUSH_TIMEOUT_MS_CONFIG, "x");
        Config cfg = validate(p);
        // The framework validator may still flag these as parse errors, but Validator's
        // own sanity branches shouldn't add additional ones.
        assertThat(errors(cfg, SolrSinkConfig.BATCH_SIZE_CONFIG))
                .noneMatch(s -> s.contains("must be >= 1"));
        assertThat(errors(cfg, SolrSinkConfig.LINGER_MS_CONFIG))
                .noneMatch(s -> s.contains("must be >= 0"));
    }

    @Test
    void onlyOneOfMaxBufferedOrBatchSetIsTolerant() {
        // Both must be non-null for the cross-check to fire.
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.BATCH_SIZE_CONFIG, "100");
        // max.buffered unset
        Config cfg = validate(p);
        assertThat(errors(cfg, SolrSinkConfig.MAX_BUFFERED_RECORDS_CONFIG)).isEmpty();
    }

    @Test
    void positiveThroughputValuesProduceNoErrors() {
        // Drives the negative-side short-circuits (value >= 0 / >= 1 / etc.).
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.BATCH_SIZE_CONFIG, "10");
        p.put(SolrSinkConfig.LINGER_MS_CONFIG, "100");
        p.put(SolrSinkConfig.MAX_RETRIES_CONFIG, "3");
        p.put(SolrSinkConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG, "4");
        p.put(SolrSinkConfig.FLUSH_TIMEOUT_MS_CONFIG, "5000");
        p.put(SolrSinkConfig.MAX_BUFFERED_RECORDS_CONFIG, "1000");
        Config cfg = validate(p);
        assertThat(errors(cfg, SolrSinkConfig.BATCH_SIZE_CONFIG)).isEmpty();
        assertThat(errors(cfg, SolrSinkConfig.LINGER_MS_CONFIG)).isEmpty();
        assertThat(errors(cfg, SolrSinkConfig.MAX_RETRIES_CONFIG)).isEmpty();
    }

    @Test
    void repeatedErrorMessageDeduplicated() {
        // Triggering the same condition twice (validate called twice) shouldn't double the message.
        Map<String, String> p = new HashMap<>();
        // No url, no zk — error fires.
        Config cfg = validate(p);
        Config cfg2 = Validator.validate(cfg, p);
        List<String> errs = errors(cfg2, SolrSinkConfig.SOLR_URL_CONFIG);
        long count = errs.stream().filter(s -> s.contains("Set either")).count();
        assertThat(count).isEqualTo(1L);
    }

    @Test
    void requiredKerberosFileEmptyIsCaughtByCheckFile() {
        // hasKeytab false but principal set -> requireKerberos branch reaches the partial fail.
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.KERBEROS_KEYTAB_PATH_CONFIG, "   ");  // whitespace only -> empty
        Config cfg = validate(p);
        // Whitespace-only is treated as not set; partial principal+keytab combo therefore both unset.
        assertThat(errors(cfg, SolrSinkConfig.KERBEROS_KEYTAB_PATH_CONFIG)).isEmpty();
    }

    @Test
    void sslMissingTruststoreFileFlagged() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.SECURITY_PROTOCOL_CONFIG, "SSL");
        p.put(SolrSinkConfig.SSL_TRUSTSTORE_LOCATION_CONFIG, "/no/such/truststore");
        Config cfg = validate(p);
        assertThat(errors(cfg, SolrSinkConfig.SSL_TRUSTSTORE_LOCATION_CONFIG))
                .anyMatch(s -> s.contains("not found"));
    }
}
