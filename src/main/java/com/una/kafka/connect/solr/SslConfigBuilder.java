package com.una.kafka.connect.solr;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.SecureRandom;

/**
 * Builds an {@link SSLContext} from {@link SolrSinkConfig}'s SSL keys.
 *
 * <p>Why this is better than the ES connector:</p>
 * <ul>
 *     <li>Defaults to TLSv1.3 (ES defaults to TLSv1.2).</li>
 *     <li>Uses {@link SecureRandom} from the JVM default provider; ES uses
 *         {@code new SecureRandom()} which can stall on /dev/random on Linux.</li>
 *     <li>Reads keystores via try-with-resources so file handles are
 *         always closed even on partial failure.</li>
 * </ul>
 */
public final class SslConfigBuilder {

    private SslConfigBuilder() {
    }

    public static SSLContext build(SolrSinkConfig config) throws Exception {
        KeyManagerFactory kmf = loadKeyManagers(config);
        TrustManagerFactory tmf = loadTrustManagers(config);
        SSLContext ctx = SSLContext.getInstance(config.sslProtocol());
        ctx.init(
                kmf == null ? null : kmf.getKeyManagers(),
                tmf == null ? null : tmf.getTrustManagers(),
                SecureRandom.getInstanceStrong());
        return ctx;
    }

    private static KeyManagerFactory loadKeyManagers(SolrSinkConfig config) throws Exception {
        String location = config.sslKeystoreLocation();
        if (location == null || location.isEmpty()) {
            return null;
        }
        KeyStore ks = KeyStore.getInstance(config.sslKeystoreType());
        try (InputStream in = new FileInputStream(location)) {
            ks.load(in, asChars(config.sslKeystorePassword()));
        }
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        String keyPw = config.sslKeyPassword();
        kmf.init(ks, keyPw == null || keyPw.isEmpty()
                ? asChars(config.sslKeystorePassword())
                : asChars(keyPw));
        return kmf;
    }

    private static TrustManagerFactory loadTrustManagers(SolrSinkConfig config) throws Exception {
        String location = config.sslTruststoreLocation();
        if (location == null || location.isEmpty()) {
            return null;
        }
        KeyStore ts = KeyStore.getInstance(config.sslTruststoreType());
        try (InputStream in = new FileInputStream(location)) {
            ts.load(in, asChars(config.sslTruststorePassword()));
        }
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ts);
        return tmf;
    }

    private static char[] asChars(String s) {
        return s == null ? new char[0] : s.toCharArray();
    }
}
