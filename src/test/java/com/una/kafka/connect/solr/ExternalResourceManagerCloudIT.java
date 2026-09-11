package com.una.kafka.connect.solr;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A real SolrCloud node reached by a plain URL — the 2026-09-10 rehearsal shape (SOLR_ZK_HOST
 * empty). Its cores are {@code <collection>_shard1_replica_n1}, so a CoreAdmin probe reports every
 * collection missing; the Collections API is the only correct probe here.
 */
class ExternalResourceManagerCloudIT {

    private static final String COLLECTION = "users";

    private static GenericContainer<?> container;
    private static SolrClient client;
    private static String baseUrl;

    @BeforeAll
    static void start() throws Exception {
        container = new GenericContainer<>(DockerImageName.parse("solr:9.4"))
                .withExposedPorts(8983)
                .withCommand("solr-foreground", "-c");
        container.start();
        baseUrl = "http://" + container.getHost() + ":" + container.getMappedPort(8983) + "/solr";
        client = new Http2SolrClient.Builder(baseUrl).build();
        awaitCollections();
        container.execInContainer("solr", "create", "-c", COLLECTION, "-s", "1", "-rf", "1");
        for (int i = 0; i < 120 && !CollectionAdminRequest.listCollections(client).contains(COLLECTION); i++) {
            Thread.sleep(500);
        }
    }

    private static void awaitCollections() throws InterruptedException {
        for (int i = 0; i < 120; i++) {
            try {
                CollectionAdminRequest.listCollections(client);
                return;
            } catch (Exception notReadyYet) {
                Thread.sleep(500);
            }
        }
    }

    @AfterAll
    static void stop() throws Exception {
        if (client != null) client.close();
        if (container != null) container.stop();
    }

    private ExternalResourceManager manager(String usage) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, baseUrl);
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, COLLECTION);
        p.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, usage);
        return new ExternalResourceManager(client, new SolrSinkConfig(p));
    }

    @Test
    void anExistingCollectionIsFoundThroughAPlainNodeUrl() throws Exception {
        List<String> collections = CollectionAdminRequest.listCollections(client);
        assertThat(collections).contains(COLLECTION);
        ExternalResourceManager mgr = manager("REQUIRED");
        mgr.ensure(COLLECTION);
        assertThat(mgr.isKnown(COLLECTION)).isTrue();
    }

    @Test
    void aMissingCollectionIsReportedMissingNotRetried() {
        ExternalResourceManager mgr = manager("REQUIRED");
        assertThatThrownBy(() -> mgr.ensure("not-there"))
                .isInstanceOf(ConnectException.class)
                .isNotInstanceOf(RetriableException.class);
    }
}
