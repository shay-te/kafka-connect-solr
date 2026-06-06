package com.una.kafka.connect.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.SSLConfig;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateHttp2SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.LBHttp2SolrClient;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public final class SolrClientFactory {

    private static final Logger log = LoggerFactory.getLogger(SolrClientFactory.class);

    private SolrClientFactory() {
    }

    public static SolrClient create(SolrSinkConfig config) {
        KerberosConfigurator.install(config);

        if (config.isCloud()) {
            if (config.streamingEnabled()) {
                log.warn("streaming.enabled=true is ignored in SolrCloud mode; using CloudHttp2SolrClient");
            }
            return buildCloud(config);
        }
        List<String> urls = config.solrUrls();
        if (urls.size() > 1) {
            if (config.streamingEnabled()) {
                log.warn("streaming.enabled=true with multiple solr.url entries is ignored; using LBHttp2SolrClient");
            }
            return buildLoadBalanced(config, urls);
        }
        String baseUrl = urls.isEmpty() ? "" : urls.get(0);
        if (config.streamingEnabled()) {
            return buildStreaming(config, baseUrl);
        }
        return buildSingle(config, baseUrl);
    }

    private static SolrClient buildSingle(SolrSinkConfig config, String url) {
        log.info("Creating Http2SolrClient against {}", url);
        Http2SolrClient.Builder builder = new Http2SolrClient.Builder(url);
        applyCommon(builder, config);
        return builder.build();
    }

    private static SolrClient buildLoadBalanced(SolrSinkConfig config, List<String> urls) {
        log.info("Creating LBHttp2SolrClient against {} urls", urls.size());
        Http2SolrClient.Builder inner = new Http2SolrClient.Builder();
        applyCommon(inner, config);
        Http2SolrClient delegate = inner.build();
        LBHttp2SolrClient lb = new LBHttp2SolrClient.Builder(delegate, urls.toArray(new String[0])).build();
        // LBHttp2SolrClient.close() (via LBSolrClient.close()) only shuts down the
        // alive-check executor; it does NOT close the delegate Http2SolrClient.
        // Verified against SolrJ 9.4.1 source. Wrap so our close() closes both.
        return new OwnedLBClient(lb, delegate);
    }

    private static final class OwnedLBClient extends SolrClient {
        private final LBHttp2SolrClient lb;
        private final Http2SolrClient delegate;

        OwnedLBClient(LBHttp2SolrClient lb, Http2SolrClient delegate) {
            this.lb = lb;
            this.delegate = delegate;
        }

        @Override
        public NamedList<Object> request(SolrRequest<?> request, String collection)
                throws SolrServerException, IOException {
            return lb.request(request, collection);
        }

        @Override
        public void close() throws IOException {
            try (Http2SolrClient owned = delegate) {
                lb.close();
            }
        }
    }

    private static SolrClient buildCloud(SolrSinkConfig config) {
        log.info("Creating CloudHttp2SolrClient against zk={}", config.zkHost());
        Http2SolrClient.Builder inner = new Http2SolrClient.Builder();
        applyCommon(inner, config);
        CloudHttp2SolrClient.Builder b = new CloudHttp2SolrClient.Builder(
                Arrays.asList(config.zkHost().split(",")), Optional.empty())
                .withInternalClientBuilder(inner);
        CloudHttp2SolrClient client = b.build();
        if (config.defaultCollection() != null && !config.defaultCollection().isEmpty()) {
            client.setDefaultCollection(config.defaultCollection());
        }
        return client;
    }

    private static SolrClient buildStreaming(SolrSinkConfig config, String url) {
        log.info("Creating ConcurrentUpdateHttp2SolrClient against {} (queue={}, threads={})",
                url, config.streamingQueueSize(), config.streamingThreads());
        Http2SolrClient.Builder inner = new Http2SolrClient.Builder(url);
        applyCommon(inner, config);
        Http2SolrClient http2 = inner.build();
        // Pass closeHttp2Client=true so CUHTTP2.close() also closes the inner client.
        // Without this flag the delegate Http2SolrClient leaks on shutdown.
        return new ConcurrentUpdateHttp2SolrClient.Builder(url, http2, true)
                .withQueueSize(config.streamingQueueSize())
                .withThreadCount(config.streamingThreads())
                .build();
    }

    private static void applyCommon(Http2SolrClient.Builder builder, SolrSinkConfig config) {
        builder.useHttp1_1(false);
        builder.withConnectionTimeout(config.connectionTimeoutMs(), TimeUnit.MILLISECONDS);
        builder.withIdleTimeout(config.readTimeoutMs(), TimeUnit.MILLISECONDS);

        Optional<String> user = nonEmpty(config.username());
        Optional<String> pw = nonEmpty(config.password());
        if (user.isPresent() && pw.isPresent()) {
            builder.withBasicAuthCredentials(user.get(), pw.get());
        }

        if (config.sslEnabled()) {
            // SslConfigBuilder.build is invoked to surface keystore/truststore load errors
            // before we hand the raw paths to SolrJ's SSLConfig.
            try {
                SslConfigBuilder.build(config);
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to build SSL context: " + e.getMessage(), e);
            }
            SSLConfig sslConfig = new SSLConfig(
                    true,
                    false,
                    config.sslKeystoreLocation(),
                    config.sslKeystorePassword(),
                    config.sslTruststoreLocation(),
                    config.sslTruststorePassword());
            builder.withSSLConfig(sslConfig);
        }

        if (config.proxyEnabled()) {
            ProxyConfigurator.apply(config);
        }

        if (config.connectionCompression() || config.compressRequests()) {
            CompressionConfigurator.apply(config);
        }
    }

    private static Optional<String> nonEmpty(String s) {
        return (s == null || s.isEmpty()) ? Optional.empty() : Optional.of(s);
    }
}
