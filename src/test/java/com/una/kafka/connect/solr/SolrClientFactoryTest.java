package com.una.kafka.connect.solr;

import org.apache.kafka.common.config.ConfigException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateHttp2SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.LBHttp2SolrClient;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SolrClientFactoryTest {

    private SolrSinkConfig newConfig(Map<String, String> extras) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "users");
        p.putAll(extras);
        return new SolrSinkConfig(p);
    }

    @Test
    void buildsSingleHttp2Client() throws Exception {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://solr:8983/solr");
        try (SolrClient c = SolrClientFactory.create(newConfig(p))) {
            assertThat(c).isInstanceOf(Http2SolrClient.class);
        }
    }

    @Test
    void buildsLoadBalancedClient() throws Exception {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://solr1:8983/solr,http://solr2:8983/solr");
        try (SolrClient c = SolrClientFactory.create(newConfig(p))) {
            assertThat(c).isInstanceOf(LBHttp2SolrClient.class);
        }
    }

    @Test
    void buildsCloudClient() throws Exception {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_ZK_HOST_CONFIG, "zk1:2181,zk2:2181/solr");
        try (SolrClient c = SolrClientFactory.create(newConfig(p))) {
            assertThat(c).isInstanceOf(CloudHttp2SolrClient.class);
        }
    }

    @Test
    void buildsCloudClientWithCredentials() throws Exception {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://solr:8983/solr");
        p.put(SolrSinkConfig.CONNECTION_USERNAME_CONFIG, "user");
        p.put(SolrSinkConfig.CONNECTION_PASSWORD_CONFIG, "pass");
        try (SolrClient c = SolrClientFactory.create(newConfig(p))) {
            assertThat(c).isNotNull();
        }
    }

    @Test
    void emptyUrlListBuildsClient() throws Exception {
        // Edge: empty list falls through the single-client path with empty URL.
        Map<String, String> p = new HashMap<>();
        try (SolrClient c = SolrClientFactory.create(newConfig(p))) {
            assertThat(c).isInstanceOf(Http2SolrClient.class);
        }
    }

    @Test
    void buildsStreamingClient() throws Exception {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://solr:8983/solr");
        p.put(SolrSinkConfig.STREAMING_ENABLED_CONFIG, "true");
        try (SolrClient c = SolrClientFactory.create(newConfig(p))) {
            // The subclass that keeps what Solr refused, so a flush cannot succeed over it.
            assertThat(c).isInstanceOf(ConcurrentUpdateHttp2SolrClient.class).isInstanceOf(StreamingSolrClient.class);
        }
    }

    @Test
    void refusesKerberosInsteadOfSendingUnauthenticatedRequests() throws Exception {
        Path keytab = Files.createTempFile("solr-sink", ".keytab");
        try {
            Map<String, String> p = new HashMap<>();
            p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://solr:8983/solr");
            p.put(SolrSinkConfig.KERBEROS_PRINCIPAL_CONFIG, "user@REALM");
            p.put(SolrSinkConfig.KERBEROS_KEYTAB_PATH_CONFIG, keytab.toString());
            assertThatThrownBy(() -> SolrClientFactory.create(newConfig(p)))
                    .isInstanceOf(ConfigException.class).hasMessageContaining("not supported");
        } finally {
            Files.delete(keytab);
        }
    }

    @Test
    void streamingIgnoredOnCloud() throws Exception {
        // Streaming + ZK should fall back to CloudHttp2SolrClient with a warning.
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_ZK_HOST_CONFIG, "zk1:2181,zk2:2181/solr");
        p.put(SolrSinkConfig.STREAMING_ENABLED_CONFIG, "true");
        try (SolrClient c = SolrClientFactory.create(newConfig(p))) {
            assertThat(c).isInstanceOf(CloudHttp2SolrClient.class);
        }
    }
}
