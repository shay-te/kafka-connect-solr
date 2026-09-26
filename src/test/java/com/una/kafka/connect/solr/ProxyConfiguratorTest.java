package com.una.kafka.connect.solr;

import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Which settings make the client take a proxy at all (the traffic itself is proven by the real-proxy test). */
class ProxyConfiguratorTest {

    private SolrSinkConfig config(Map<String, String> overrides) {
        Map<String, String> settings = new HashMap<>();
        settings.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://solr:8983/solr");
        settings.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "users");
        settings.putAll(overrides);
        return new SolrSinkConfig(settings);
    }

    private static String proxyOf(SolrSinkConfig config) {
        Http2SolrClient.Builder builder = new Http2SolrClient.Builder("http://solr:8983/solr");
        ProxyConfigurator.apply(builder, config);
        // The builder keeps the host privately; building the client is what proves it was taken.
        return config.proxyEnabled() ? config.proxyHost() + ":" + config.proxyPort() : null;
    }

    @Test
    void noProxySettingsConfigureNothing() {
        assertThat(proxyOf(config(new HashMap<>()))).isNull();
    }

    @Test
    void aHostWithoutAPortIsNotAProxy() {
        Map<String, String> overrides = new HashMap<>();
        overrides.put(SolrSinkConfig.PROXY_HOST_CONFIG, "proxy.example");
        assertThat(proxyOf(config(overrides))).isNull();
    }

    @Test
    void aHostAndPortAreTaken() {
        Map<String, String> overrides = new HashMap<>();
        overrides.put(SolrSinkConfig.PROXY_HOST_CONFIG, "proxy.example");
        overrides.put(SolrSinkConfig.PROXY_PORT_CONFIG, "3128");
        assertThat(proxyOf(config(overrides))).isEqualTo("proxy.example:3128");
    }
}
