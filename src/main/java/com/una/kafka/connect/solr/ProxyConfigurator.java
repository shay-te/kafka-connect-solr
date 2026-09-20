package com.una.kafka.connect.solr;

import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The proxy SolrJ actually uses. JVM proxy system properties never reached its Jetty client. */
public final class ProxyConfigurator {

    private static final Logger log = LoggerFactory.getLogger(ProxyConfigurator.class);

    private ProxyConfigurator() {
    }

    public static void apply(Http2SolrClient.Builder builder, SolrSinkConfig config) {
        if (!config.proxyEnabled()) {
            return;
        }
        log.info("Routing Solr traffic through HTTP proxy {}:{}", config.proxyHost(), config.proxyPort());
        builder.withProxyConfiguration(config.proxyHost(), config.proxyPort(), false, false);
    }
}
