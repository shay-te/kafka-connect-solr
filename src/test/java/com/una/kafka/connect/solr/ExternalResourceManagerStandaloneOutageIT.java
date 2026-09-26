package com.una.kafka.connect.solr;

import org.apache.kafka.connect.errors.RetriableException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A real standalone Solr 9.4 over HTTP: a missing core is auto-created through CoreAdmin, and once
 * the node has gone away the next core probe fails as a retriable I/O error (not "missing").
 * The node is stopped by the last test, so the order matters.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ExternalResourceManagerStandaloneOutageIT {

    private static GenericContainer<?> container;
    private static SolrClient client;
    private static String baseUrl;
    // One manager for the whole class: it learns "standalone" once and keeps that answer.
    private static ExternalResourceManager manager;

    @BeforeAll
    static void start() throws Exception {
        container = new GenericContainer<>(DockerImageName.parse("solr:9.4"))
                .withExposedPorts(8983)
                .withCommand("solr-precreate", "preexisting");
        container.start();
        baseUrl = "http://" + container.getHost() + ":" + container.getMappedPort(8983) + "/solr";
        SolrTestSupport.awaitCoreReady(baseUrl, "preexisting");
        // CoreAdmin CREATE resolves configsets under the Solr home.
        container.execInContainer("cp", "-r", "/opt/solr/server/solr/configsets", "/var/solr/data/");
        client = new Http2SolrClient.Builder(baseUrl).build();
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, baseUrl);
        p.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "AUTO");
        p.put(SolrSinkConfig.SCHEMA_AUTO_CREATE_CONFIG, "true");
        manager = new ExternalResourceManager(client, new SolrSinkConfig(p));
    }

    @AfterAll
    static void stop() throws Exception {
        if (client != null) client.close();
        if (container != null) container.stop();
    }

    @Test
    @Order(1)
    void missingCoreIsCreatedThroughCoreAdmin() throws Exception {
        manager.ensure("tickets");
        assertThat(manager.isKnown("tickets")).isTrue();
        assertThat(CoreAdminRequest.getStatus("tickets", client).getCoreStatus("tickets").asMap(0))
                .isNotEmpty();
        SolrTestSupport.awaitHits(baseUrl, "tickets", "*:*", 0L);
    }

    @Test
    @Order(2)
    void coreProbeAfterTheNodeWentAwayIsRetriable() {
        container.stop();
        assertThatThrownBy(() -> manager.ensure("payments"))
                .isInstanceOf(RetriableException.class)
                .hasMessageContaining("check whether core 'payments' exists");
        assertThat(manager.isKnown("payments")).isFalse();
    }
}
