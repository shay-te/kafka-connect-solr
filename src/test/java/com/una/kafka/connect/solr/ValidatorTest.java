package com.una.kafka.connect.solr;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ValidatorTest {

    private Map<String, String> valid() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x:8983/solr");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "users");
        return p;
    }

    private static boolean hasErrorFor(Config c, String name) {
        for (ConfigValue v : c.configValues()) {
            if (name.equals(v.name()) && !v.errorMessages().isEmpty()) {
                return true;
            }
        }
        return false;
    }

    @Test
    void validDefaultConfig() {
        Map<String, String> p = valid();
        Config result = new SolrSinkConnector().validate(p);
        assertThat(hasErrorFor(result, SolrSinkConfig.SOLR_URL_CONFIG)).isFalse();
    }

    @Test
    void requiresConnection() {
        Map<String, String> p = new HashMap<>();
        Config result = new SolrSinkConnector().validate(p);
        assertThat(hasErrorFor(result, SolrSinkConfig.SOLR_URL_CONFIG)).isTrue();
    }

    @Test
    void cannotSetBothUrlAndZk() {
        Map<String, String> p = valid();
        p.put(SolrSinkConfig.SOLR_ZK_HOST_CONFIG, "zk:2181");
        Config result = new SolrSinkConnector().validate(p);
        assertThat(hasErrorFor(result, SolrSinkConfig.SOLR_URL_CONFIG)).isTrue();
    }

    @Test
    void sslMissingFilesFails() {
        Map<String, String> p = valid();
        p.put(SolrSinkConfig.SECURITY_PROTOCOL_CONFIG, "SSL");
        p.put(SolrSinkConfig.SSL_KEYSTORE_LOCATION_CONFIG, "/no/such/file.jks");
        Config result = new SolrSinkConnector().validate(p);
        assertThat(hasErrorFor(result, SolrSinkConfig.SSL_KEYSTORE_LOCATION_CONFIG)).isTrue();
    }

    @Test
    void kerberosRequiresBothKeys() {
        Map<String, String> p = valid();
        p.put(SolrSinkConfig.KERBEROS_PRINCIPAL_CONFIG, "user@REALM");
        Config result = new SolrSinkConnector().validate(p);
        assertThat(hasErrorFor(result, SolrSinkConfig.KERBEROS_KEYTAB_PATH_CONFIG)).isTrue();
    }

    @Test
    void kerberosKeytabMustExist() {
        Map<String, String> p = valid();
        p.put(SolrSinkConfig.KERBEROS_PRINCIPAL_CONFIG, "user@REALM");
        p.put(SolrSinkConfig.KERBEROS_KEYTAB_PATH_CONFIG, "/no/such/keytab");
        Config result = new SolrSinkConnector().validate(p);
        assertThat(hasErrorFor(result, SolrSinkConfig.KERBEROS_KEYTAB_PATH_CONFIG)).isTrue();
    }

    @Test
    void throughputSanityCheck() {
        Map<String, String> p = valid();
        p.put(SolrSinkConfig.BATCH_SIZE_CONFIG, "1000");
        p.put(SolrSinkConfig.MAX_BUFFERED_RECORDS_CONFIG, "10");
        Config result = new SolrSinkConnector().validate(p);
        assertThat(hasErrorFor(result, SolrSinkConfig.MAX_BUFFERED_RECORDS_CONFIG)).isTrue();
    }

    @Test
    void regexCollectionRequiresArrow() {
        Map<String, String> p = valid();
        p.put(SolrSinkConfig.COLLECTION_NAMING_STRATEGY_CONFIG, "TOPIC_REGEX");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "no-arrow");
        Config result = new SolrSinkConnector().validate(p);
        assertThat(hasErrorFor(result, SolrSinkConfig.SOLR_COLLECTION_CONFIG)).isTrue();
    }
}
