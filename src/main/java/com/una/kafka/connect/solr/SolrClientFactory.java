package com.una.kafka.connect.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.LBHttp2SolrClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Builds a single, reusable {@link SolrClient}. HTTP/2 multiplexing gives
 * us the concurrent in-flight requests advantage over the ES connector.
 *
 * <p>Configured features (all surfaced as connector config keys, no JVM
 * properties required):</p>
 * <ul>
 *     <li>Basic auth (username / password).</li>
 *     <li>SSL / TLS with keystore + truststore.</li>
 *     <li>HTTP proxy (host / port / basic-auth).</li>
 *     <li>Kerberos (JAAS keytab, auto-renew TGT).</li>
 *     <li>Response compression (Accept-Encoding gzip or zstd).</li>
 * </ul>
 */
public final class SolrClientFactory {

    private static final Logger log = LoggerFactory.getLogger(SolrClientFactory.class);

    private SolrClientFactory() {
    }

    public static SolrClient create(SolrSinkConfig config) {
        KerberosConfigurator.install(config);

        if (config.isCloud()) {
            return buildCloud(config);
        }
        List<String> urls = config.solrUrls();
        if (urls.size() > 1) {
            return buildLoadBalanced(config, urls);
        }
        return buildSingle(config, urls.isEmpty() ? "" : urls.get(0));
    }

    private static SolrClient buildSingle(SolrSinkConfig config, String url) {
        log.info("Creating Http2SolrClient against {} (ssl={}, proxy={}, kerberos={})",
                url, config.sslEnabled(), config.proxyEnabled(), config.kerberosEnabled());
        Http2SolrClient.Builder builder = new Http2SolrClient.Builder(url);
        applyCommon(builder, config);
        return builder.build();
    }

    private static SolrClient buildLoadBalanced(SolrSinkConfig config, List<String> urls) {
        log.info("Creating LBHttp2SolrClient against {} urls", urls.size());
        Http2SolrClient.Builder inner = new Http2SolrClient.Builder();
        applyCommon(inner, config);
        Http2SolrClient delegate = inner.build();
        return new LBHttp2SolrClient.Builder(delegate, urls.toArray(new String[0])).build();
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

    private static void applyCommon(Http2SolrClient.Builder builder, SolrSinkConfig config) {
        builder.useHttp1_1(false);
        builder.withConnectionTimeout(config.connectionTimeoutMs(), TimeUnit.MILLISECONDS);
        builder.withIdleTimeout(config.readTimeoutMs(), TimeUnit.MILLISECONDS);

        // Auth
        Optional<String> user = nonEmpty(config.username());
        Optional<String> pw = nonEmpty(config.password());
        if (user.isPresent() && pw.isPresent()) {
            builder.withBasicAuthCredentials(user.get(), pw.get());
        }

        // SSL
        if (config.sslEnabled()) {
            try {
                SSLContext sslContext = SslConfigBuilder.build(config);
                builder.withSSLContext(sslContext);
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to build SSL context: " + e.getMessage(), e);
            }
        }

        // Proxy - we have to wire this via the Jetty client because SolrJ's
        // builder does not expose proxy directly.
        if (config.proxyEnabled()) {
            ProxyConfigurator.apply(builder, config);
        }

        // Compression
        if (config.connectionCompression()) {
            CompressionConfigurator.apply(builder, config);
        }
    }

    private static Optional<String> nonEmpty(String s) {
        return (s == null || s.isEmpty()) ? Optional.empty() : Optional.of(s);
    }
}
