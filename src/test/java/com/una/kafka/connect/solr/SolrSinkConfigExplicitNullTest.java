package com.una.kafka.connect.solr;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A connector config submitted through the Connect REST API can carry explicit JSON nulls
 * ({@code "proxy.host": null}); ConfigDef then yields null instead of the default. Every derived
 * accessor must treat null like "not set" rather than throwing.
 */
class SolrSinkConfigExplicitNullTest {

    private static SolrSinkConfig withNulls(String... keys) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://solr:8983/solr");
        for (String k : keys) p.put(k, null);
        return new SolrSinkConfig(p);
    }

    @Test
    void nullPasswordsReadAsEmpty() {
        SolrSinkConfig c = withNulls(
                SolrSinkConfig.CONNECTION_PASSWORD_CONFIG,
                SolrSinkConfig.PROXY_PASSWORD_CONFIG,
                SolrSinkConfig.SSL_KEYSTORE_PASSWORD_CONFIG,
                SolrSinkConfig.SSL_KEY_PASSWORD_CONFIG,
                SolrSinkConfig.SSL_TRUSTSTORE_PASSWORD_CONFIG);
        assertThat(c.password()).isEmpty();
        assertThat(c.proxyPassword()).isEmpty();
        assertThat(c.sslKeystorePassword()).isEmpty();
        assertThat(c.sslKeyPassword()).isEmpty();
        assertThat(c.sslTruststorePassword()).isEmpty();
    }

    @Test
    void nullHostsAndNamesDisableTheirFeatures() {
        SolrSinkConfig c = withNulls(
                SolrSinkConfig.PROXY_HOST_CONFIG,
                SolrSinkConfig.KERBEROS_PRINCIPAL_CONFIG,
                SolrSinkConfig.EXTERNAL_VERSION_HEADER_CONFIG,
                SolrSinkConfig.SOLR_ZK_HOST_CONFIG);
        assertThat(c.proxyEnabled()).isFalse();
        assertThat(c.kerberosEnabled()).isFalse();
        assertThat(c.externalVersioningEnabled()).isFalse();
        assertThat(c.isCloud()).isFalse();
    }

    @Test
    void principalWithANullKeytabDoesNotEnableKerberos() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://solr:8983/solr");
        p.put(SolrSinkConfig.KERBEROS_PRINCIPAL_CONFIG, "solr-sink@EXAMPLE.COM");
        p.put(SolrSinkConfig.KERBEROS_KEYTAB_PATH_CONFIG, null);
        assertThat(new SolrSinkConfig(p).kerberosEnabled()).isFalse();
    }

    @Test
    void emptyProxyHostWithAPortIsStillDisabled() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://solr:8983/solr");
        p.put(SolrSinkConfig.PROXY_HOST_CONFIG, "");
        p.put(SolrSinkConfig.PROXY_PORT_CONFIG, "3128");
        assertThat(new SolrSinkConfig(p).proxyEnabled()).isFalse();
    }
}
