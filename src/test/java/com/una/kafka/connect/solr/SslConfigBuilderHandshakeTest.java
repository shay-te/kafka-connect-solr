package com.una.kafka.connect.solr;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLSocket;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link SslConfigBuilder} and the SSL branch of {@link SolrClientFactory} with real key material
 * from keytool: a server context built from only a keystore and a client context built from only a
 * truststore complete a genuine TLS handshake over loopback, and a keystore that cannot be opened
 * fails client creation up front.
 */
class SslConfigBuilderHandshakeTest {

    private static TestKeystores stores;

    @BeforeAll
    static void keys() throws Exception {
        stores = TestKeystores.generate();
    }

    private static SolrSinkConfig config(String keystore, String truststore, String keystorePassword) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "https://127.0.0.1:8983/solr");
        p.put(SolrSinkConfig.SECURITY_PROTOCOL_CONFIG, "SSL");
        // Explicit nulls: a Connect REST config can carry "ssl.keystore.location": null.
        p.put(SolrSinkConfig.SSL_KEYSTORE_LOCATION_CONFIG, keystore);
        p.put(SolrSinkConfig.SSL_KEYSTORE_PASSWORD_CONFIG, keystorePassword);
        p.put(SolrSinkConfig.SSL_KEYSTORE_TYPE_CONFIG, TestKeystores.TYPE);
        p.put(SolrSinkConfig.SSL_TRUSTSTORE_LOCATION_CONFIG, truststore);
        p.put(SolrSinkConfig.SSL_TRUSTSTORE_PASSWORD_CONFIG, TestKeystores.PASSWORD);
        p.put(SolrSinkConfig.SSL_TRUSTSTORE_TYPE_CONFIG, TestKeystores.TYPE);
        p.put(SolrSinkConfig.SSL_PROTOCOL_CONFIG, "TLSv1.3");
        return new SolrSinkConfig(p);
    }

    @Test
    void keystoreOnlyServerAndTruststoreOnlyClientCompleteAHandshake() throws Exception {
        SSLContext serverCtx = SslConfigBuilder.build(config(stores.keystore.toString(), null, TestKeystores.PASSWORD));
        SSLContext clientCtx = SslConfigBuilder.build(config(null, stores.truststore.toString(), TestKeystores.PASSWORD));

        try (SSLServerSocket server = (SSLServerSocket) serverCtx.getServerSocketFactory()
                .createServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            CompletableFuture<String> served = CompletableFuture.supplyAsync(() -> {
                try (SSLSocket s = (SSLSocket) server.accept();
                     InputStream in = s.getInputStream();
                     OutputStream out = s.getOutputStream()) {
                    byte[] buf = new byte[64];
                    int n = in.read(buf);
                    out.write(("ack:" + new String(buf, 0, n, StandardCharsets.UTF_8)).getBytes(StandardCharsets.UTF_8));
                    out.flush();
                    return s.getSession().getProtocol();
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            });
            try (SSLSocket client = (SSLSocket) clientCtx.getSocketFactory()
                    .createSocket(InetAddress.getLoopbackAddress(), server.getLocalPort())) {
                client.getOutputStream().write("ping".getBytes(StandardCharsets.UTF_8));
                client.getOutputStream().flush();
                byte[] buf = new byte[64];
                int n = client.getInputStream().read(buf);
                assertThat(new String(buf, 0, n, StandardCharsets.UTF_8)).isEqualTo("ack:ping");
                assertThat(client.getSession().getPeerCertificates()).hasSize(1);
            }
            assertThat(served.get(30, TimeUnit.SECONDS)).isEqualTo("TLSv1.3");
        }
    }

    @Test
    void clientCreationFailsFastWhenTheKeystoreCannotBeOpened() {
        SolrSinkConfig wrongPassword = config(stores.keystore.toString(), stores.truststore.toString(), "not-the-password");
        assertThatThrownBy(() -> SolrClientFactory.create(wrongPassword))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageStartingWith("Failed to build SSL context");
    }
}
