package com.una.kafka.connect.solr;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drives the positive sides of every accessor branch in SolrSinkConfig that
 * is only hit when a password / proxy / kerberos / external-version field is
 * actually configured.
 */
class SolrSinkConfigBranchTest {

    private SolrSinkConfig with(Map<String, String> o) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "c");
        p.putAll(o);
        return new SolrSinkConfig(p);
    }

    @Test
    void connectionPasswordReturnsValueWhenSet() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.CONNECTION_PASSWORD_CONFIG, "secret");
        assertThat(with(o).password()).isEqualTo("secret");
    }

    @Test
    void proxyEnabledRequiresHostAndPort() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.PROXY_HOST_CONFIG, "proxy.example");
        o.put(SolrSinkConfig.PROXY_PORT_CONFIG, "3128");
        o.put(SolrSinkConfig.PROXY_PASSWORD_CONFIG, "pw");
        SolrSinkConfig cfg = with(o);
        assertThat(cfg.proxyEnabled()).isTrue();
        assertThat(cfg.proxyPassword()).isEqualTo("pw");
    }

    @Test
    void sslPasswordsReturnValuesWhenSet() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.SSL_KEYSTORE_PASSWORD_CONFIG, "k");
        o.put(SolrSinkConfig.SSL_KEY_PASSWORD_CONFIG, "kk");
        o.put(SolrSinkConfig.SSL_TRUSTSTORE_PASSWORD_CONFIG, "t");
        SolrSinkConfig cfg = with(o);
        assertThat(cfg.sslKeystorePassword()).isEqualTo("k");
        assertThat(cfg.sslKeyPassword()).isEqualTo("kk");
        assertThat(cfg.sslTruststorePassword()).isEqualTo("t");
    }

    @Test
    void kerberosEnabledRequiresBothPrincipalAndKeytab() throws Exception {
        Path keytab = Files.createTempFile("kt", ".kt");
        try {
            Map<String, String> o = new HashMap<>();
            o.put(SolrSinkConfig.KERBEROS_PRINCIPAL_CONFIG, "u@R");
            o.put(SolrSinkConfig.KERBEROS_KEYTAB_PATH_CONFIG, keytab.toAbsolutePath().toString());
            assertThat(with(o).kerberosEnabled()).isTrue();
        } finally {
            Files.deleteIfExists(keytab);
        }
    }

    @Test
    void externalVersioningEnabledOnlyWhenHeaderNonEmpty() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.EXTERNAL_VERSION_HEADER_CONFIG, "_v");
        assertThat(with(o).externalVersioningEnabled()).isTrue();
        // Empty header (default) -> disabled.
        assertThat(with(new HashMap<>()).externalVersioningEnabled()).isFalse();
    }

    @Test
    void isCloudOnlyWhenZkSet() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.SOLR_URL_CONFIG, "");
        o.put(SolrSinkConfig.SOLR_ZK_HOST_CONFIG, "zk:2181");
        assertThat(with(o).isCloud()).isTrue();
        assertThat(with(new HashMap<>()).isCloud()).isFalse();
    }

    @Test
    void proxyEnabledFalseWhenHostMissing() {
        // Default props -> no proxy host -> proxyEnabled short-circuits to false at first condition.
        assertThat(with(new HashMap<>()).proxyEnabled()).isFalse();
    }

    @Test
    void proxyEnabledFalseWhenHostEmptyString() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.PROXY_HOST_CONFIG, "");
        o.put(SolrSinkConfig.PROXY_PORT_CONFIG, "8080");
        assertThat(with(o).proxyEnabled()).isFalse();
    }

    @Test
    void proxyEnabledFalseWhenPortZero() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.PROXY_HOST_CONFIG, "proxy.example");
        // No proxy.port override — default is 0 -> proxyEnabled returns false.
        assertThat(with(o).proxyEnabled()).isFalse();
    }

    @Test
    void kerberosEnabledFalseWhenOnlyPrincipalSet() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.KERBEROS_PRINCIPAL_CONFIG, "u@R");
        assertThat(with(o).kerberosEnabled()).isFalse();
    }

    @Test
    void kerberosEnabledFalseWhenPrincipalEmpty() throws Exception {
        java.nio.file.Path keytab = java.nio.file.Files.createTempFile("kt", ".kt");
        try {
            Map<String, String> o = new HashMap<>();
            o.put(SolrSinkConfig.KERBEROS_PRINCIPAL_CONFIG, "");
            o.put(SolrSinkConfig.KERBEROS_KEYTAB_PATH_CONFIG, keytab.toAbsolutePath().toString());
            assertThat(with(o).kerberosEnabled()).isFalse();
        } finally {
            java.nio.file.Files.deleteIfExists(keytab);
        }
    }

    @Test
    void kerberosEnabledFalseWhenKeytabEmpty() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.KERBEROS_PRINCIPAL_CONFIG, "u@R");
        o.put(SolrSinkConfig.KERBEROS_KEYTAB_PATH_CONFIG, "");
        assertThat(with(o).kerberosEnabled()).isFalse();
    }

    @Test
    void externalVersioningDisabledWhenHeaderEmpty() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.EXTERNAL_VERSION_HEADER_CONFIG, "");
        assertThat(with(o).externalVersioningEnabled()).isFalse();
    }

    @Test
    void isCloudFalseWhenZkEmpty() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.SOLR_ZK_HOST_CONFIG, "");
        assertThat(with(o).isCloud()).isFalse();
    }
}
