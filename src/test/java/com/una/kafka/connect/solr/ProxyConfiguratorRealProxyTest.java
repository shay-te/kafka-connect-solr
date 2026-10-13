package com.una.kafka.connect.solr;

import com.sun.net.httpserver.HttpServer;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The proxy setting with a REAL proxy and a REAL Solr client: SolrJ's Jetty client must route through
 * {@link TinyHttpProxy} to reach an origin its own DNS cannot resolve. The JVM proxy system properties
 * this used to set are read by nothing in SolrJ, so a request that arrives is the only proof.
 */
class ProxyConfiguratorRealProxyTest {

    // A loopback address nothing listens on: reachable ONLY because the proxy re-routes it, and it
    // needs no DNS (SolrJ's Jetty client resolves the origin itself even when a proxy is configured).
    private static final String ORIGIN_HOST = "127.0.0.1";
    private int deadPort;

    private HttpServer origin;
    private TinyHttpProxy proxy;

    @BeforeEach
    void setUp() throws Exception {
        origin = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        origin.createContext("/solr/users/select", exchange -> {
            byte[] body = javabinSearchResponse(7);            // what SolrJ's default parser expects
            exchange.getResponseHeaders().add("Content-Type", "application/vnd.apache.solr.javabin");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        origin.start();
        try (java.net.ServerSocket probe = new java.net.ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            deadPort = probe.getLocalPort();             // closed again: only the proxy can reach the origin
        }
        proxy = new TinyHttpProxy(null, null);           // SolrJ cannot answer a 407
        proxy.route(ORIGIN_HOST, origin.getAddress());
    }

    @AfterEach
    void tearDown() throws Exception {
        proxy.close();
        origin.stop(0);
    }

    private SolrSinkConfig config(String proxyHost, String proxyPort) {
        Map<String, String> settings = new HashMap<>();
        settings.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://" + ORIGIN_HOST + ":" + deadPort + "/solr");
        settings.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "users");
        if (proxyHost != null) {
            settings.put(SolrSinkConfig.PROXY_HOST_CONFIG, proxyHost);
            settings.put(SolrSinkConfig.PROXY_PORT_CONFIG, proxyPort);
        }
        return new SolrSinkConfig(settings);
    }

    private static byte[] javabinSearchResponse(int numFound) throws java.io.IOException {
        org.apache.solr.common.SolrDocumentList documents = new org.apache.solr.common.SolrDocumentList();
        documents.setNumFound(numFound);
        documents.setStart(0);
        org.apache.solr.common.util.NamedList<Object> response = new org.apache.solr.common.util.NamedList<>();
        org.apache.solr.common.util.NamedList<Object> header = new org.apache.solr.common.util.NamedList<>();
        header.add("status", 0);
        header.add("QTime", 1);
        response.add("responseHeader", header);
        response.add("response", documents);
        java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
        try (org.apache.solr.common.util.JavaBinCodec codec = new org.apache.solr.common.util.JavaBinCodec()) {
            codec.marshal(response, bytes);
        }
        return bytes.toByteArray();
    }

    @Test
    void solrTrafficReallyGoesThroughTheConfiguredProxy() throws Exception {
        try (SolrClient client = SolrClientFactory.create(
                config("127.0.0.1", Integer.toString(proxy.port())))) {
            assertThat(client.query("users", new SolrQuery("*:*")).getResults().getNumFound()).isEqualTo(7);
        }
        assertThat(proxy.forwardedRequests()).as("the query reached the origin through the proxy").isEqualTo(1);
    }

    @Test
    void withoutAProxyTheSameRequestCannotResolveTheOrigin() throws Exception {
        try (SolrClient client = SolrClientFactory.create(config(null, null))) {
            org.assertj.core.api.Assertions.assertThatThrownBy(
                    () -> client.query("users", new SolrQuery("*:*"))).isInstanceOf(Exception.class);
        }
        assertThat(proxy.forwardedRequests()).isZero();
    }
}
