package com.una.kafka.connect.solr;

import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wires HTTP compression. The ES connector supports only response gzip
 * on optional. We support both directions and accept zstd in addition
 * to gzip on the response.
 *
 * <p>The default request payload is already smaller than ES's because
 * {@link Http2SolrClient} defaults to SolrJ's javabin (binary) request
 * writer - ~30% smaller and ~50% faster to parse than the JSON the
 * ES bulk API uses. No connector-side flag needed for that.</p>
 */
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
                // Try zstd first, fall back to gzip if the server doesn't speak it.
                value = "zstd, gzip";
                break;
            case NONE:
            default:
                return;
        }
        log.info("Enabling response compression: {}", value);
        // Jetty's HttpClient honours this system property for its Accept-Encoding default.
        System.setProperty("jetty.client.acceptedEncodings", value);
    }

    private static void applyRequestCompression(SolrSinkConfig config) {
        log.info("Enabling outbound request compression (gzip)");
        // Jetty supports outbound gzip via the GzipRequest extension. We set
        // the documented system property so it applies to every Http2SolrClient
        // built by this connector without us reaching into the inner HttpClient.
        System.setProperty("jetty.client.gzipRequests", "true");
    }
}
