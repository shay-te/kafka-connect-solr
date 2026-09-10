package com.una.kafka.connect.solr;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Real Solr 9.4 standalone container: drives ExternalResourceManager's probe
 * branches (core exists, core missing) and the auto-create code path with no
 * mocks.
 */
class ExternalResourceManagerIT {

    private static GenericContainer<?> container;
    private static SolrClient client;
    private static String baseUrl;

    @BeforeAll
    static void start() throws Exception {
        container = new GenericContainer<>(DockerImageName.parse("solr:9.4"))
                .withExposedPorts(8983)
                .withCommand("solr-precreate", "preexisting");
        container.start();
        baseUrl = "http://" + container.getHost() + ":" + container.getMappedPort(8983) + "/solr";
        SolrTestSupport.awaitCoreReady(baseUrl, "preexisting");
        client = new Http2SolrClient.Builder(baseUrl).build();
    }

    @AfterAll
    static void stop() throws Exception {
        if (client != null) client.close();
        if (container != null) container.stop();
    }

    private SolrSinkConfig cfg(Map<String, String> o) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, baseUrl);
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "preexisting");
        p.putAll(o);
        return new SolrSinkConfig(p);
    }

    @Test
    void existingCoreProbeReturnsHit() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "REQUIRED");
        ExternalResourceManager mgr = new ExternalResourceManager(client, cfg(o));
        mgr.ensure("preexisting");
        assertThat(mgr.isKnown("preexisting")).isTrue();
    }

    @Test
    void missingCoreRequiredModeThrows() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "REQUIRED");
        ExternalResourceManager mgr = new ExternalResourceManager(client, cfg(o));
        assertThatThrownBy(() -> mgr.ensure("not-there"))
                .isInstanceOf(ConnectException.class);
    }

    @Test
    void unusedModeShortCircuitsWithoutNetworkIO() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "UNUSED");
        ExternalResourceManager mgr = new ExternalResourceManager(client, cfg(o));
        mgr.ensure("anything-arbitrary");
        // After UNUSED handling the resource is known without a probe.
        assertThat(mgr.isKnown("anything-arbitrary")).isTrue();
    }

    @Test
    void autoModeOnExistingCoreShortCircuitsAfterProbe() {
        // Real probe of an existing core; covers the AUTO + probe-hit branch.
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "AUTO");
        ExternalResourceManager mgr = new ExternalResourceManager(client, cfg(o));
        mgr.ensure("preexisting");
        assertThat(mgr.isKnown("preexisting")).isTrue();
        // Calling again is a no-op (known-set hit).
        mgr.ensure("preexisting");
    }

    @Test
    void autoModeWithAutoCreateDisabledThrows() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "AUTO");
        o.put(SolrSinkConfig.SCHEMA_AUTO_CREATE_CONFIG, "false");
        ExternalResourceManager mgr = new ExternalResourceManager(client, cfg(o));
        assertThatThrownBy(() -> mgr.ensure("disabled-create"))
                .isInstanceOf(ConnectException.class);
    }
}
