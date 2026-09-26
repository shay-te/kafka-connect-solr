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

/**
 * Drives the remaining SslConfigBuilder branches: distinct key-password vs
 * keystore-password, null/empty truststore location, and the asChars(null) path.
 */
class SslConfigBuilderBranchTest {

    private SolrSinkConfig cfg(Map<String, String> overrides) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "c");
        p.put(SolrSinkConfig.SECURITY_PROTOCOL_CONFIG, "SSL");
        p.putAll(overrides);
        return new SolrSinkConfig(p);
    }

    @Test
    void keyPasswordDistinctFromKeystorePasswordTriggersBothFills() throws Exception {
        Path ks = Files.createTempFile("kp-ks", ".jks");
        try {
            KeyStore keyStore = KeyStore.getInstance("JKS");
            keyStore.load(null, "outer".toCharArray());
            try (FileOutputStream out = new FileOutputStream(ks.toFile())) {
                keyStore.store(out, "outer".toCharArray());
            }

            Map<String, String> o = new HashMap<>();
            o.put(SolrSinkConfig.SSL_KEYSTORE_LOCATION_CONFIG, ks.toAbsolutePath().toString());
            o.put(SolrSinkConfig.SSL_KEYSTORE_PASSWORD_CONFIG, "outer");
            // Distinct key password so the `keyPw != storePw` Arrays.fill branch fires.
            o.put(SolrSinkConfig.SSL_KEY_PASSWORD_CONFIG, "inner");
            SSLContext ctx = SslConfigBuilder.build(cfg(o));
            assertThat(ctx).isNotNull();
        } finally {
            Files.deleteIfExists(ks);
        }
    }

    @Test
    void emptyKeystorePathSkipsKeyManagerLoad() throws Exception {
        Map<String, String> o = new HashMap<>();
        // Explicitly empty location -> loadKeyManagers returns null early.
        o.put(SolrSinkConfig.SSL_KEYSTORE_LOCATION_CONFIG, "");
        SSLContext ctx = SslConfigBuilder.build(cfg(o));
        assertThat(ctx).isNotNull();
    }

    @Test
    void emptyTruststorePathSkipsTrustManagerLoad() throws Exception {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.SSL_TRUSTSTORE_LOCATION_CONFIG, "");
        SSLContext ctx = SslConfigBuilder.build(cfg(o));
        assertThat(ctx).isNotNull();
    }
}
