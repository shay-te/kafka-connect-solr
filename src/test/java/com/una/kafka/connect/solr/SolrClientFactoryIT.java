package com.una.kafka.connect.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
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
            Thread.sleep(300);
            assertThat(client.query("factory", new SolrQuery("id:f-single")).getResults().getNumFound())
                    .isEqualTo(1L);
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
