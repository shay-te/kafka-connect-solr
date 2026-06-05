package com.una.kafka.connect.solr;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class ProxyConfiguratorTest {

    @AfterEach
    void cleanup() {
        // Don't leak JVM-wide state from one test into the next.
        System.clearProperty("http.proxyHost");
        System.clearProperty("http.proxyPort");
        System.clearProperty("https.proxyHost");
        System.clearProperty("https.proxyPort");
        java.net.Authenticator.setDefault(null);
    }

    private SolrSinkConfig cfg(Map<String, String> overrides) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "c");
        p.putAll(overrides);
        return new SolrSinkConfig(p);
    }

    @Test
    void noProxyIsNoop() {
        ProxyConfigurator.apply(cfg(new HashMap<>()));
        assertThat(System.getProperty("http.proxyHost")).isNull();
    }

    @Test
    void zeroPortIsNoop() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.PROXY_HOST_CONFIG, "proxy.example");
        // port omitted -> default 0
        ProxyConfigurator.apply(cfg(o));
        assertThat(System.getProperty("http.proxyHost")).isNull();
    }

    @Test
    void sysPropsSetWhenProxyEnabled() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.PROXY_HOST_CONFIG, "proxy.example");
        o.put(SolrSinkConfig.PROXY_PORT_CONFIG, "3128");
        ProxyConfigurator.apply(cfg(o));
        assertThat(System.getProperty("http.proxyHost")).isEqualTo("proxy.example");
        assertThat(System.getProperty("http.proxyPort")).isEqualTo("3128");
        assertThat(System.getProperty("https.proxyHost")).isEqualTo("proxy.example");
        assertThat(System.getProperty("https.proxyPort")).isEqualTo("3128");
    }

    @Test
    void authenticatorInstalledWhenCredentialsSet() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.PROXY_HOST_CONFIG, "proxy.example");
        o.put(SolrSinkConfig.PROXY_PORT_CONFIG, "3128");
        o.put(SolrSinkConfig.PROXY_USERNAME_CONFIG, "alice");
        o.put(SolrSinkConfig.PROXY_PASSWORD_CONFIG, "s3cret");
        ProxyConfigurator.apply(cfg(o));
        // We can't directly retrieve the installed Authenticator, but
        // Authenticator.getDefault was removed in JDK 9. Confirm via the
        // request flow that asks for proxy auth.
        java.net.PasswordAuthentication auth = java.net.Authenticator.requestPasswordAuthentication(
                "proxy.example", null, 3128, "http", null, "Basic",
                null, java.net.Authenticator.RequestorType.PROXY);
        assertThat(auth).isNotNull();
        assertThat(auth.getUserName()).isEqualTo("alice");
        assertThat(new String(auth.getPassword())).isEqualTo("s3cret");
    }
}
