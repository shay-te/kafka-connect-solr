package com.una.kafka.connect.solr;

import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import java.io.FileOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SslConfigBuilderTest {

    private SolrSinkConfig cfg(Map<String, String> overrides) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "c");
        p.put(SolrSinkConfig.SECURITY_PROTOCOL_CONFIG, "SSL");
        p.putAll(overrides);
        return new SolrSinkConfig(p);
    }

    @Test
    void buildsWithoutKeystoreOrTruststore() throws Exception {
        // No store paths configured -> SSLContext built with default trust manager.
        SSLContext ctx = SslConfigBuilder.build(cfg(new HashMap<>()));
        assertThat(ctx).isNotNull();
        assertThat(ctx.getProtocol()).isEqualTo("TLSv1.3");
    }

    @Test
    void missingKeystoreFileFailsCleanly() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.SSL_KEYSTORE_LOCATION_CONFIG, "/no/such/path.jks");
        assertThatThrownBy(() -> SslConfigBuilder.build(cfg(o)))
                .isInstanceOf(Exception.class);
    }

    @Test
    void buildsWithRealKeystore() throws Exception {
        Path ks = Files.createTempFile("test-keystore", ".jks");
        try {
            // Write a minimal but valid empty JKS keystore.
            KeyStore keyStore = KeyStore.getInstance("JKS");
            keyStore.load(null, "changeit".toCharArray());
            try (FileOutputStream out = new FileOutputStream(ks.toFile())) {
                keyStore.store(out, "changeit".toCharArray());
            }

            Map<String, String> o = new HashMap<>();
            o.put(SolrSinkConfig.SSL_KEYSTORE_LOCATION_CONFIG, ks.toAbsolutePath().toString());
            o.put(SolrSinkConfig.SSL_KEYSTORE_PASSWORD_CONFIG, "changeit");
            o.put(SolrSinkConfig.SSL_TRUSTSTORE_LOCATION_CONFIG, ks.toAbsolutePath().toString());
            o.put(SolrSinkConfig.SSL_TRUSTSTORE_PASSWORD_CONFIG, "changeit");
            SSLContext ctx = SslConfigBuilder.build(cfg(o));
            assertThat(ctx).isNotNull();
        } finally {
            Files.deleteIfExists(ks);
        }
    }

    @Test
    void usesConfiguredProtocol() throws Exception {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.SSL_PROTOCOL_CONFIG, "TLSv1.2");
        SSLContext ctx = SslConfigBuilder.build(cfg(o));
        assertThat(ctx.getProtocol()).isEqualTo("TLSv1.2");
    }
}
