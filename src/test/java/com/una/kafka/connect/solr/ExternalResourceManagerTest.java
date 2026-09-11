package com.una.kafka.connect.solr;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.common.util.NamedList;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ExternalResourceManagerTest {

    private SolrSinkConfig cfg(Map<String, String> overrides) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "users");
        p.putAll(overrides);
        return new SolrSinkConfig(p);
    }

    @Test
    void unusedSkipsProbe() throws Exception {
        SolrClient client = mock(SolrClient.class);
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "UNUSED");
        new ExternalResourceManager(client, cfg(o)).ensure("users");
        // Cached, never probed.
        org.mockito.Mockito.verifyNoInteractions(client);
    }

    @Test
    void requiredAndMissingThrows() throws Exception {
        SolrClient client = mock(SolrClient.class);
        when(client.request(any(SolrRequest.class), any())).thenReturn(emptyCollections());

        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "REQUIRED");
        o.put(SolrSinkConfig.SOLR_ZK_HOST_CONFIG, "zk:2181");
        ExternalResourceManager mgr = new ExternalResourceManager(client, cfg(o));
        assertThatThrownBy(() -> mgr.ensure("missing"))
                .isInstanceOf(ConnectException.class);
    }

    @Test
    void autoModeCreatesWhenMissing() throws Exception {
        SolrClient client = mock(SolrClient.class);
        when(client.request(any(SolrRequest.class), any())).thenReturn(emptyCollections());

        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "AUTO");
        o.put(SolrSinkConfig.SOLR_ZK_HOST_CONFIG, "zk:2181");
        o.put(SolrSinkConfig.SCHEMA_AUTO_CREATE_CONFIG, "true");

        ExternalResourceManager mgr = new ExternalResourceManager(client, cfg(o));
        mgr.ensure("brand-new");
        assertThat(mgr.isKnown("brand-new")).isTrue();
    }

    @Test
    void autoModeFailsWhenAutoCreateDisabled() throws Exception {
        SolrClient client = mock(SolrClient.class);
        when(client.request(any(SolrRequest.class), any())).thenReturn(emptyCollections());

        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "AUTO");
        o.put(SolrSinkConfig.SOLR_ZK_HOST_CONFIG, "zk:2181");
        o.put(SolrSinkConfig.SCHEMA_AUTO_CREATE_CONFIG, "false");

        ExternalResourceManager mgr = new ExternalResourceManager(client, cfg(o));
        assertThatThrownBy(() -> mgr.ensure("missing"))
                .isInstanceOf(ConnectException.class);
    }

    @Test
    void existingCollectionIsMemoised() throws Exception {
        SolrClient client = mock(SolrClient.class);
        when(client.request(any(SolrRequest.class), any())).thenReturn(listingWith("users"));

        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "AUTO");
        o.put(SolrSinkConfig.SOLR_ZK_HOST_CONFIG, "zk:2181");
        ExternalResourceManager mgr = new ExternalResourceManager(client, cfg(o));
        mgr.ensure("users");
        mgr.ensure("users");
        // Only one probe.
        org.mockito.Mockito.verify(client, org.mockito.Mockito.times(1)).request(any(SolrRequest.class), any());
    }

    @Test
    void nullOrEmptyCollectionShortCircuits() {
        SolrClient client = mock(SolrClient.class);
        ExternalResourceManager mgr = new ExternalResourceManager(client, cfg(new HashMap<>()));
        mgr.ensure(null);
        mgr.ensure("");
        org.mockito.Mockito.verifyNoInteractions(client);
    }

    @Test
    void aTransientProbeErrorUnderZooKeeperIsRetriedNotReportedMissing() throws Exception {
        // Read as "missing", a network blip at task start failed the task for good.
        SolrClient client = mock(SolrClient.class);
        when(client.request(any(SolrRequest.class), any()))
                .thenThrow(new java.io.IOException("network blip"));

        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "REQUIRED");
        o.put(SolrSinkConfig.SOLR_ZK_HOST_CONFIG, "zk:2181");
        ExternalResourceManager mgr = new ExternalResourceManager(client, cfg(o));
        assertThatThrownBy(() -> mgr.ensure("anywhere"))
                .isInstanceOf(org.apache.kafka.connect.errors.RetriableException.class);
        assertThat(mgr.isKnown("anywhere")).isFalse();
    }

    @Test
    void anUnreachableSolrIsRetriableNeverMissing() throws Exception {
        // A real client with nothing listening. Read as "missing", this killed the task (REQUIRED)
        // or sent a CREATE for a collection that exists (AUTO).
        int port;
        try (java.net.ServerSocket socket = new java.net.ServerSocket(0)) {
            port = socket.getLocalPort();
        }
        String url = "http://127.0.0.1:" + port + "/solr";
        try (SolrClient client = new org.apache.solr.client.solrj.impl.Http2SolrClient.Builder(url).build()) {
            Map<String, String> o = new HashMap<>();
            o.put(SolrSinkConfig.SOLR_URL_CONFIG, url);
            o.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "REQUIRED");
            ExternalResourceManager mgr = new ExternalResourceManager(client, cfg(o));
            assertThatThrownBy(() -> mgr.ensure("users"))
                    .isInstanceOf(org.apache.kafka.connect.errors.RetriableException.class);
            assertThat(mgr.isKnown("users")).isFalse();
        }
    }

    private NamedList<Object> emptyCollections() {
        NamedList<Object> r = new NamedList<>();
        r.add("collections", java.util.Collections.emptyList());
        return r;
    }

    private NamedList<Object> listingWith(String... names) {
        NamedList<Object> r = new NamedList<>();
        r.add("collections", Arrays.asList(names));
        return r;
    }
}
