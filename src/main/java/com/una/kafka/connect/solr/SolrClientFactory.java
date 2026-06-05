package com.una.kafka.connect.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.LBHttp2SolrClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Builds a single, reusable {@link SolrClient}. SolrJ's HTTP/2 client lets
 * the connector multiplex multiple in-flight requests over one connection,
 * which is why we can drive higher throughput than the Elasticsearch sink.
 */
public final class SolrClientFactory {

    private static final Logger log = LoggerFactory.getLogger(SolrClientFactory.class);

    private SolrClientFactory() {
    }

    public static SolrClient create(SolrSinkConfig config) {
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
        Optional<String> user = nonEmpty(config.username());
        Optional<String> pw = nonEmpty(config.password());
        if (user.isPresent() && pw.isPresent()) {
            builder.withBasicAuthCredentials(user.get(), pw.get());
        }
    }

    private static Optional<String> nonEmpty(String s) {
        return (s == null || s.isEmpty()) ? Optional.empty() : Optional.of(s);
    }
}
