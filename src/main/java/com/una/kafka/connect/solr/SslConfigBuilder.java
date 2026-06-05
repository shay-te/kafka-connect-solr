package com.una.kafka.connect.solr;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.io.InputStream;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.Arrays;

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
        char[] storePw = asChars(config.sslKeystorePassword());
        String keyPwStr = config.sslKeyPassword();
        char[] keyPw = (keyPwStr == null || keyPwStr.isEmpty()) ? storePw : asChars(keyPwStr);
        try {
            KeyStore ks = KeyStore.getInstance(config.sslKeystoreType());
            try (InputStream in = new FileInputStream(location)) {
                ks.load(in, storePw);
            }
            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(ks, keyPw);
            return kmf;
        } finally {
            Arrays.fill(storePw, '\0');
            if (keyPw != storePw) Arrays.fill(keyPw, '\0');
        }
    }

    private static TrustManagerFactory loadTrustManagers(SolrSinkConfig config) throws Exception {
        String location = config.sslTruststoreLocation();
        if (location == null || location.isEmpty()) {
            return null;
        }
        KeyStore ts = KeyStore.getInstance(config.sslTruststoreType());
        char[] pw = asChars(config.sslTruststorePassword());
        try {
            try (InputStream in = new FileInputStream(location)) {
                ts.load(in, pw);
            }
        } finally {
            Arrays.fill(pw, '\0');
        }
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(ts);
        return tmf;
    }

    private static char[] asChars(String s) {
        return s == null ? new char[0] : s.toCharArray();
    }
}
