package com.una.kafka.connect.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.LBHttp2SolrClient;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

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
}
