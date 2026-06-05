package com.una.kafka.connect.solr;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class CompressionConfiguratorTest {

    @AfterEach
    void cleanup() {
        System.clearProperty("jetty.client.acceptedEncodings");
        System.clearProperty("jetty.client.gzipRequests");
    }

    private SolrSinkConfig cfg(Map<String, String> overrides) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "c");
        p.putAll(overrides);
        return new SolrSinkConfig(p);
    }

    @Test
    void disabledIsNoop() {
        CompressionConfigurator.apply(cfg(new HashMap<>()));
        assertThat(System.getProperty("jetty.client.acceptedEncodings")).isNull();
        assertThat(System.getProperty("jetty.client.gzipRequests")).isNull();
    }

    @Test
    void gzipResponse() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.CONNECTION_COMPRESSION_CONFIG, "true");
        o.put(SolrSinkConfig.CONNECTION_COMPRESSION_ALGORITHM_CONFIG, "GZIP");
        CompressionConfigurator.apply(cfg(o));
        assertThat(System.getProperty("jetty.client.acceptedEncodings")).isEqualTo("gzip");
    }

    @Test
    void zstdFallsBackToGzip() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.CONNECTION_COMPRESSION_CONFIG, "true");
        o.put(SolrSinkConfig.CONNECTION_COMPRESSION_ALGORITHM_CONFIG, "ZSTD");
        CompressionConfigurator.apply(cfg(o));
        assertThat(System.getProperty("jetty.client.acceptedEncodings")).isEqualTo("zstd, gzip");
    }

    @Test
    void noneAlgorithmSkipsResponseHeader() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.CONNECTION_COMPRESSION_CONFIG, "true");
        o.put(SolrSinkConfig.CONNECTION_COMPRESSION_ALGORITHM_CONFIG, "NONE");
        CompressionConfigurator.apply(cfg(o));
        assertThat(System.getProperty("jetty.client.acceptedEncodings")).isNull();
    }

    @Test
    void requestCompressionTogglesGzipFlag() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.CONNECTION_COMPRESSION_REQUESTS_CONFIG, "true");
        CompressionConfigurator.apply(cfg(o));
        assertThat(System.getProperty("jetty.client.gzipRequests")).isEqualTo("true");
    }

    @Test
    void responseAndRequestCompressionAreIndependent() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.CONNECTION_COMPRESSION_CONFIG, "true");
        o.put(SolrSinkConfig.CONNECTION_COMPRESSION_ALGORITHM_CONFIG, "GZIP");
        o.put(SolrSinkConfig.CONNECTION_COMPRESSION_REQUESTS_CONFIG, "true");
        CompressionConfigurator.apply(cfg(o));
        assertThat(System.getProperty("jetty.client.acceptedEncodings")).isEqualTo("gzip");
        assertThat(System.getProperty("jetty.client.gzipRequests")).isEqualTo("true");
    }
}
