package com.una.kafka.connect.solr;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * What compression this client can actually do. SolrJ's HTTP/2 client negotiates gzip RESPONSES on its
 * own (Jetty registers a gzip decoder), and it can neither gzip request bodies nor speak zstd — the
 * `jetty.client.*` system properties this used to set are read by nothing.
 */
public final class CompressionConfigurator {

    private static final Logger log = LoggerFactory.getLogger(CompressionConfigurator.class);

    private CompressionConfigurator() {
    }

    public static void apply(SolrSinkConfig config) {
        if (config.compressRequests()) {
            log.warn("{}=true has no effect: SolrJ's HTTP/2 client cannot compress request bodies",
                    SolrSinkConfig.CONNECTION_COMPRESSION_REQUESTS_CONFIG);
        }
        if (config.connectionCompression() && config.compressionAlgorithm() == SolrSinkConfig.CompressionAlgorithm.ZSTD) {
            log.warn("{}=ZSTD is not supported; responses are negotiated as gzip",
                    SolrSinkConfig.CONNECTION_COMPRESSION_ALGORITHM_CONFIG);
        }
    }
}
