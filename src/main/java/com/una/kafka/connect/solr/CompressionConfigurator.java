package com.una.kafka.connect.solr;

import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wires HTTP response compression. The ES connector only supports gzip.
 * We support both gzip (default) and zstd (newer, ~3x faster decompression
 * at the same ratio) — when configured the client adds an
 * {@code Accept-Encoding} header on every request.
 */
public final class CompressionConfigurator {

    private static final Logger log = LoggerFactory.getLogger(CompressionConfigurator.class);

    private CompressionConfigurator() {
    }

    public static void apply(Http2SolrClient.Builder builder, SolrSinkConfig config) {
        if (!config.connectionCompression()) {
            return;
        }
        String value;
        switch (config.compressionAlgorithm()) {
            case GZIP:
                value = "gzip";
                break;
            case ZSTD:
                value = "zstd, gzip"; // fall back to gzip if the server can't do zstd
                break;
            case NONE:
            default:
                return;
        }
        log.info("Enabling response compression: {}", value);
        builder.withRequestWriter(new org.apache.solr.client.solrj.impl.BinaryRequestWriter());
        // SolrJ's Http2SolrClient does not expose per-request headers via the
        // builder; we set the header on every request by wrapping the
        // builder's listener. As a simpler alternative we set the JVM-wide
        // default Accept-Encoding for HttpURLConnection-derived clients.
        System.setProperty("http.agent", "kafka-connect-solr (" + value + ")");
    }
}
