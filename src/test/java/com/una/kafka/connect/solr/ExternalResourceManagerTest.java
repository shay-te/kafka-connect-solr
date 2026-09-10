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
    void probeFailureFallsThroughToCreateOrThrow() throws Exception {
        // Simulate a probe error (network failure during listCollections).
        SolrClient client = mock(SolrClient.class);
        when(client.request(any(SolrRequest.class), any()))
                .thenThrow(new java.io.IOException("network blip"));

        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "REQUIRED");
        o.put(SolrSinkConfig.SOLR_ZK_HOST_CONFIG, "zk:2181");
        ExternalResourceManager mgr = new ExternalResourceManager(client, cfg(o));
        assertThatThrownBy(() -> mgr.ensure("anywhere"))
                .isInstanceOf(ConnectException.class);
    }

    @Test
    void aPlainUrlToASolrCloudNodeFindsAnExistingCollection() throws Exception {
        // 2026-09-10 rehearsal: SOLR_ZK_HOST was empty, so the sink got `solr.url` for a SolrCloud
        // node. The CoreAdmin probe looked for a core literally named after the collection (cores
        // are `<collection>_shard1_replica_n1`), reported it missing, and killed every task.
        SolrClient client = mock(SolrClient.class);
        when(client.request(any(SolrRequest.class), any())).thenReturn(listingWith("users"));

        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "REQUIRED");
        ExternalResourceManager mgr = new ExternalResourceManager(client, cfg(o));
        mgr.ensure("users");
        assertThat(mgr.isKnown("users")).isTrue();
    }

    @Test
    void aStandaloneSolrIsRecognisedOnceAndNotAskedForCollectionsAgain() throws Exception {
        SolrClient client = mock(SolrClient.class);
        when(client.request(any(SolrRequest.class), any())).thenThrow(
                new org.apache.solr.client.solrj.impl.BaseHttpSolrClient.RemoteSolrException(
                        "http://x", 400, "Solr instance is not running in SolrCloud mode.", null));

        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "REQUIRED");
        ExternalResourceManager mgr = new ExternalResourceManager(client, cfg(o));
        assertThatThrownBy(() -> mgr.ensure("a")).isInstanceOf(ConnectException.class);
        assertThatThrownBy(() -> mgr.ensure("b")).isInstanceOf(ConnectException.class);
        // "a": Collections attempt + CoreAdmin probe; "b": CoreAdmin probe only (standalone cached).
        org.mockito.Mockito.verify(client, org.mockito.Mockito.times(3)).request(any(SolrRequest.class), any());
    }

    @Test
    void aTransientErrorIsNotCachedAsStandalone() throws Exception {
        SolrClient client = mock(SolrClient.class);
        when(client.request(any(SolrRequest.class), any())).thenThrow(new java.io.IOException("network blip"));

        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "REQUIRED");
        ExternalResourceManager mgr = new ExternalResourceManager(client, cfg(o));
        assertThatThrownBy(() -> mgr.ensure("a")).isInstanceOf(ConnectException.class);
        assertThatThrownBy(() -> mgr.ensure("b")).isInstanceOf(ConnectException.class);
        // Each ensure asks the Collections API again (nothing cached) and then CoreAdmin: 2 + 2.
        org.mockito.Mockito.verify(client, org.mockito.Mockito.times(4)).request(any(SolrRequest.class), any());
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
