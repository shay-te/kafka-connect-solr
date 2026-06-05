package com.una.kafka.connect.solr;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ProxyConfigurator {

    private static final Logger log = LoggerFactory.getLogger(ProxyConfigurator.class);

    private ProxyConfigurator() {
    }

    public static void apply(SolrSinkConfig config) {
        String host = config.proxyHost();
        int port = config.proxyPort();
        if (host == null || host.isEmpty() || port <= 0) {
            return;
        }
        log.info("Configuring HTTP proxy {}:{}", host, port);
        System.setProperty("http.proxyHost", host);
        System.setProperty("http.proxyPort", Integer.toString(port));
        System.setProperty("https.proxyHost", host);
        System.setProperty("https.proxyPort", Integer.toString(port));
        final String user = config.proxyUsername();
        if (!user.isEmpty()) {
            final String pass = config.proxyPassword();
            java.net.Authenticator.setDefault(new java.net.Authenticator() {
                @Override
                protected java.net.PasswordAuthentication getPasswordAuthentication() {
                    if (getRequestorType() == RequestorType.PROXY) {
                        return new java.net.PasswordAuthentication(user, pass.toCharArray());
                    }
                    return null;
                }
            });
        }
    }
}
