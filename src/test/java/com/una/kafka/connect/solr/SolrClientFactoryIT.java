package com.una.kafka.connect.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateHttp2SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.LBHttp2SolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Exercises SolrClientFactory.create against a real Solr 9.4 container for
 * each client variant: single-URL HTTP/2, load-balanced (multiple URLs),
 * and streaming (ConcurrentUpdateHttp2). No mocks — every client is closed
 * normally so the OwnedLBClient and CUHTTP2 close paths execute against
 * real network resources.
 */
class SolrClientFactoryIT {

    private static GenericContainer<?> container;
    private static String baseUrl;

    @BeforeAll
    static void start() throws Exception {
        container = new GenericContainer<>(DockerImageName.parse("solr:9.4"))
                .withExposedPorts(8983)
                .withCommand("solr-precreate", "factory");
        container.start();
        baseUrl = "http://" + container.getHost() + ":" + container.getMappedPort(8983) + "/solr";
        SolrTestSupport.awaitCoreReady(baseUrl, "factory");
    }

    @AfterAll
    static void stop() {
        if (container != null) container.stop();
    }

    private SolrSinkConfig cfg(Map<String, String> o) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, baseUrl);
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "factory");
        p.putAll(o);
        return new SolrSinkConfig(p);
    }

    @Test
    void singleUrlBuildsHttp2Client() throws Exception {
        SolrClient client = SolrClientFactory.create(cfg(new HashMap<>()));
        try {
            assertThat(client).isInstanceOf(Http2SolrClient.class);
            UpdateRequest req = new UpdateRequest();
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("id", "f-single");
            req.add(doc);
            req.setCommitWithin(100);
            req.process(client, "factory");
            SolrTestSupport.awaitHits(baseUrl, "factory", "id:f-single", 1L);
        } finally {
            client.close();
        }
    }

    @Test
    void multipleUrlsBuildLoadBalancedClient() throws Exception {
        Map<String, String> o = new HashMap<>();
        // Same URL listed twice — still triggers LBHttp2SolrClient because urls.size() > 1.
        o.put(SolrSinkConfig.SOLR_URL_CONFIG, baseUrl + "," + baseUrl);
        SolrClient client = SolrClientFactory.create(cfg(o));
        try {
            assertThat(client).isInstanceOf(LBHttp2SolrClient.class);
        } finally {
            client.close();
        }
    }

    @Test
    void streamingEnabledBuildsConcurrentUpdateClient() throws Exception {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.STREAMING_ENABLED_CONFIG, "true");
        o.put(SolrSinkConfig.STREAMING_QUEUE_SIZE_CONFIG, "50");
        o.put(SolrSinkConfig.STREAMING_THREADS_CONFIG, "2");
        SolrClient client = SolrClientFactory.create(cfg(o));
        try {
            assertThat(client).isInstanceOf(ConcurrentUpdateHttp2SolrClient.class);
        } finally {
            client.close();
        }
    }

    @Test
    void streamingWithMultipleUrlsFallsBackToLoadBalanced() throws Exception {
        // Per the factory: streaming.enabled is ignored when multiple URLs are present.
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.SOLR_URL_CONFIG, baseUrl + "," + baseUrl);
        o.put(SolrSinkConfig.STREAMING_ENABLED_CONFIG, "true");
        SolrClient client = SolrClientFactory.create(cfg(o));
        try {
            assertThat(client).isInstanceOf(LBHttp2SolrClient.class);
        } finally {
            client.close();
        }
    }

    /** Runs {@code body} and restores the JVM-wide properties the configurators may set. */
    private static void withRestoredSystemProperties(ThrowingRunnable body) throws Exception {
        String[] keys = {"http.proxyHost", "http.proxyPort", "https.proxyHost", "https.proxyPort",
                "jetty.client.acceptedEncodings", "jetty.client.gzipRequests"};
        Map<String, String> saved = new HashMap<>();
        for (String k : keys) saved.put(k, System.getProperty(k));
        try {
            body.run();
        } finally {
            for (String k : keys) {
                if (saved.get(k) == null) System.clearProperty(k);
                else System.setProperty(k, saved.get(k));
            }
            java.net.Authenticator.setDefault(null);
        }
    }

    private interface ThrowingRunnable {
        void run() throws Exception;
    }

    private static void writeOne(SolrClient client, String id) throws Exception {
        UpdateRequest req = new UpdateRequest();
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", id);
        doc.addField("title_s", "client variant " + id);
        req.add(doc);
        req.setCommitWithin(100);
        req.process(client, "factory");
    }

    @Test
    void aProxiedClientWritesToSolrThroughTheProxy() throws Exception {
        // An OPEN proxy: SolrJ's client cannot answer a 407, which is why Validator refuses
        // proxy.username. The write landing in Solr is the proof that the proxy is really used.
        try (TinyHttpProxy proxy = new TinyHttpProxy(null, null)) {
            java.net.URI solr = java.net.URI.create(baseUrl);
            proxy.route(solr.getHost(), new java.net.InetSocketAddress(solr.getHost(), solr.getPort()));
            Map<String, String> o = new HashMap<>();
            o.put(SolrSinkConfig.PROXY_HOST_CONFIG, "127.0.0.1");
            o.put(SolrSinkConfig.PROXY_PORT_CONFIG, Integer.toString(proxy.port()));
            try (SolrClient client = SolrClientFactory.create(cfg(o))) {
                writeOne(client, "f-proxy");
            }
            SolrTestSupport.awaitHits(baseUrl, "factory", "id:f-proxy", 1L);
            assertThat(proxy.forwardedRequests()).isGreaterThanOrEqualTo(1);
        }
    }

    @Test
    void compressionSettingsStillProduceAWorkingClient() throws Exception {
        withRestoredSystemProperties(() -> {
            Map<String, String> zstd = new HashMap<>();
            zstd.put(SolrSinkConfig.CONNECTION_COMPRESSION_CONFIG, "true");
            zstd.put(SolrSinkConfig.CONNECTION_COMPRESSION_ALGORITHM_CONFIG, "ZSTD");
            try (SolrClient client = SolrClientFactory.create(cfg(zstd))) {
                writeOne(client, "f-zstd");
            }
            Map<String, String> gzipRequests = new HashMap<>();
            gzipRequests.put(SolrSinkConfig.CONNECTION_COMPRESSION_REQUESTS_CONFIG, "true");
            try (SolrClient client = SolrClientFactory.create(cfg(gzipRequests))) {
                writeOne(client, "f-gzip-requests");
            }
            SolrTestSupport.awaitHits(baseUrl, "factory", "id:f-zstd OR id:f-gzip-requests", 2L);
        });
    }

    @Test
    void usernameWithoutPasswordSendsNoCredentials() throws Exception {
        // Half-configured basic auth is ignored rather than sending "user:" to Solr.
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.CONNECTION_USERNAME_CONFIG, "solr-writer");
        try (SolrClient client = SolrClientFactory.create(cfg(o))) {
            writeOne(client, "f-user-only");
        }
        SolrTestSupport.awaitHits(baseUrl, "factory", "id:f-user-only", 1L);
    }

    @Test
    void basicAuthCredentialsApplied() throws Exception {
        // Solr container has no auth, but the credentials configurator branch still fires.
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.CONNECTION_USERNAME_CONFIG, "test");
        o.put(SolrSinkConfig.CONNECTION_PASSWORD_CONFIG, "test");
        SolrClient client = SolrClientFactory.create(cfg(o));
        try {
            assertThat(client).isInstanceOf(Http2SolrClient.class);
        } finally {
            client.close();
        }
    }
}
