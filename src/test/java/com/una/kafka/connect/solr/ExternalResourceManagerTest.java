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
        when(client.request(any(SolrRequest.class))).thenReturn(emptyCollections());

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
        when(client.request(any(SolrRequest.class))).thenReturn(emptyCollections());

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
        when(client.request(any(SolrRequest.class))).thenReturn(emptyCollections());

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
        when(client.request(any(SolrRequest.class))).thenReturn(listingWith("users"));

        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "AUTO");
        o.put(SolrSinkConfig.SOLR_ZK_HOST_CONFIG, "zk:2181");
        ExternalResourceManager mgr = new ExternalResourceManager(client, cfg(o));
        mgr.ensure("users");
        mgr.ensure("users");
        // Only one probe.
        org.mockito.Mockito.verify(client, org.mockito.Mockito.times(1)).request(any(SolrRequest.class));
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
