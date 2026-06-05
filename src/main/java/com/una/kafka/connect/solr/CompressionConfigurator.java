package com.una.kafka.connect.solr;

import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class CompressionConfigurator {

    private static final Logger log = LoggerFactory.getLogger(CompressionConfigurator.class);

    private CompressionConfigurator() {
    }

    public static void apply(Http2SolrClient.Builder builder, SolrSinkConfig config) {
        if (config.connectionCompression()) {
            applyResponseCompression(config);
        }
        if (config.compressRequests()) {
            applyRequestCompression(config);
        }
    }

    private static void applyResponseCompression(SolrSinkConfig config) {
        String value;
        switch (config.compressionAlgorithm()) {
            case GZIP:
                value = "gzip";
                break;
            case ZSTD:
                value = "zstd, gzip";
                break;
            case NONE:
            default:
                return;
        }
        log.info("Enabling response compression: {}", value);
        System.setProperty("jetty.client.acceptedEncodings", value);
    }

    private static void applyRequestCompression(SolrSinkConfig config) {
        log.info("Enabling outbound request compression (gzip)");
        System.setProperty("jetty.client.gzipRequests", "true");
    }
}
