package com.una.kafka.connect.solr;

import org.apache.kafka.common.config.ConfigException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.SSLConfig;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateHttp2SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.LBHttp2SolrClient;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public final class SolrClientFactory {

    private static final Logger log = LoggerFactory.getLogger(SolrClientFactory.class);

    private SolrClientFactory() {
    }

    public static SolrClient create(SolrSinkConfig config) {
        if (config.kerberosEnabled()) {
            throw new ConfigException(SolrSinkConfig.KERBEROS_UNSUPPORTED);
        }

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
        // LBHttp2SolrClient.close() (via LBSolrClient.close()) only shuts down the
        // alive-check executor; it does NOT close the delegate Http2SolrClient.
        // Verified against SolrJ 9.4.1 source. Subclass to also close the delegate.
        return new OwnedLBClient(delegate, urls.toArray(new String[0]));
    }

    private static final class OwnedLBClient extends LBHttp2SolrClient {
        private final Http2SolrClient delegate;

        OwnedLBClient(Http2SolrClient delegate, String... urls) {
            super(delegate, urls);
            this.delegate = delegate;
        }

        @Override
        public void close() {
            try {
                super.close();
            } finally {
                delegate.close();
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
        return new StreamingSolrClient(new ConcurrentUpdateHttp2SolrClient.Builder(url, http2, true)
                .withQueueSize(config.streamingQueueSize())
                .withThreadCount(config.streamingThreads()));
    }

    private static void applyCommon(Http2SolrClient.Builder builder, SolrSinkConfig config) {
        // Cleartext HTTP/2 (prior knowledge) cannot pass an ordinary HTTP proxy, so a proxied client
        // speaks HTTP/1.1; without this the proxy setting was accepted and every request failed.
        builder.useHttp1_1(config.proxyEnabled());
        builder.withConnectionTimeout(config.connectionTimeoutMs(), TimeUnit.MILLISECONDS);
        builder.withIdleTimeout(config.readTimeoutMs(), TimeUnit.MILLISECONDS);

        Optional<String> user = nonEmpty(config.username());
        Optional<String> pw = nonEmpty(config.password());
        if (user.isPresent() && pw.isPresent()) {
            builder.withBasicAuthCredentials(user.get(), pw.get());
        }

        if (config.sslEnabled()) {
            SSLContext context;
            try {
                context = SslConfigBuilder.build(config);
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to build SSL context: " + e.getMessage(), e);
            }
            builder.withSSLConfig(new ContextSslConfig(context));
        }

        ProxyConfigurator.apply(builder, config);

        CompressionConfigurator.apply(config);
    }

    private static Optional<String> nonEmpty(String s) {
        return (s == null || s.isEmpty()) ? Optional.empty() : Optional.of(s);
    }

    /**
     * Gives Jetty the context built from every ssl.* setting: SolrJ's own SSLConfig applies a truststore only with
     * client auth on, and never the store types or the key password.
     */
    private static final class ContextSslConfig extends SSLConfig {
        private final SSLContext context;

        ContextSslConfig(SSLContext context) {
            super(true, false, null, null, null, null);
            this.context = context;
        }

        @Override
        public SslContextFactory.Client createClientContextFactory() {
            SslContextFactory.Client factory = new SslContextFactory.Client();
            factory.setSslContext(context);
            return factory;
        }
    }
}
