package com.una.kafka.connect.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Client construction when optional settings are empty or arrive as explicit JSON nulls through
 * the Connect REST API: no default collection is pinned on a SolrCloud client, and a null
 * username sends no credentials. Building the clients opens no connection.
 */
class SolrClientFactoryNullSettingsTest {

    private static SolrSinkConfig config(String... kv) {
        Map<String, String> p = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) p.put(kv[i], kv[i + 1]);
        return new SolrSinkConfig(p);
    }

    @Test
    void cloudClientWithoutADefaultCollectionRoutesPerRequest() throws Exception {
        for (String collection : new String[]{"", null}) {
            try (SolrClient client = SolrClientFactory.create(config(
                    SolrSinkConfig.SOLR_ZK_HOST_CONFIG, "zk1:2181,zk2:2181/solr",
                    SolrSinkConfig.SOLR_COLLECTION_CONFIG, collection))) {
                assertThat(client).isInstanceOf(CloudHttp2SolrClient.class);
                assertThat(((CloudHttp2SolrClient) client).getDefaultCollection()).isNull();
            }
        }
    }

    @Test
    void nullUsernameBuildsAnAnonymousClient() throws Exception {
        try (SolrClient client = SolrClientFactory.create(config(
                SolrSinkConfig.SOLR_URL_CONFIG, "http://127.0.0.1:8983/solr",
                SolrSinkConfig.CONNECTION_USERNAME_CONFIG, null,
                SolrSinkConfig.CONNECTION_PASSWORD_CONFIG, "secret"))) {
            assertThat(client).isInstanceOf(Http2SolrClient.class);
        }
    }
}
