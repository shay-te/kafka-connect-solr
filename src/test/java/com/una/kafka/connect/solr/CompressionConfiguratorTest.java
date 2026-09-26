package com.una.kafka.connect.solr;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/** Compression settings touch no global state: what this client can do is decided by SolrJ, not by us. */
class CompressionConfiguratorTest {

    private SolrSinkConfig config(Map<String, String> overrides) {
        Map<String, String> settings = new HashMap<>();
        settings.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://solr:8983/solr");
        settings.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "users");
        settings.putAll(overrides);
        return new SolrSinkConfig(settings);
    }

    private static Map<String, String> settings(String... keyValue) {
        Map<String, String> overrides = new HashMap<>();
        for (int i = 0; i < keyValue.length; i += 2) overrides.put(keyValue[i], keyValue[i + 1]);
        return overrides;
    }

    @Test
    void noSettingWritesTheJettySystemPropertiesThatNothingReads() {
        for (Map<String, String> overrides : java.util.Arrays.asList(
                settings(),
                settings(SolrSinkConfig.CONNECTION_COMPRESSION_CONFIG, "true"),
                settings(SolrSinkConfig.CONNECTION_COMPRESSION_CONFIG, "true",
                        SolrSinkConfig.CONNECTION_COMPRESSION_ALGORITHM_CONFIG, "ZSTD"),
                settings(SolrSinkConfig.CONNECTION_COMPRESSION_REQUESTS_CONFIG, "true"))) {
            CompressionConfigurator.apply(config(overrides));
            assertThat(System.getProperty("jetty.client.acceptedEncodings")).isNull();
            assertThat(System.getProperty("jetty.client.gzipRequests")).isNull();
        }
    }

    @Test
    void everyAlgorithmIsAccepted() {
        for (String algorithm : new String[]{"GZIP", "ZSTD", "NONE"}) {
            assertThatCode(() -> CompressionConfigurator.apply(config(settings(
                    SolrSinkConfig.CONNECTION_COMPRESSION_CONFIG, "true",
                    SolrSinkConfig.CONNECTION_COMPRESSION_ALGORITHM_CONFIG, algorithm))))
                    .doesNotThrowAnyException();
        }
    }
}
