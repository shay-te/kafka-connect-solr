package com.una.kafka.connect.solr;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A real Solr 9.4 serving HTTPS only, with key material generated at test time by keytool. The
 * connector's SSL client (keystore + truststore through {@link SslConfigBuilder} and SolrJ's
 * SSLConfig) must complete the TLS handshake and index documents; nothing is mocked.
 */
class SolrClientFactorySslIT {

    private static final String CORE = "secure";
    private static final String STORE_IN_CONTAINER = "/var/solr/solr-ssl.keystore.p12";

    private static final Schema ACCOUNT = SchemaBuilder.struct().name("account")
            .field("id", Schema.STRING_SCHEMA)
            .field("owner", Schema.STRING_SCHEMA)
            .field("roles", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
            .build();

    private static TestKeystores stores;
    private static GenericContainer<?> container;
    private static String baseUrl;

    @BeforeAll
    static void start() throws Exception {
        stores = TestKeystores.generate(DockerClientFactory.instance().dockerHostIpAddress());
        container = new GenericContainer<>(DockerImageName.parse("solr:9.4"))
                .withExposedPorts(8983)
                .withCopyFileToContainer(MountableFile.forHostPath(stores.keystore, 0644), STORE_IN_CONTAINER)
                .withEnv("SOLR_SSL_ENABLED", "true")
                .withEnv("SOLR_SSL_KEY_STORE", STORE_IN_CONTAINER)
                .withEnv("SOLR_SSL_KEY_STORE_PASSWORD", TestKeystores.PASSWORD)
                .withEnv("SOLR_SSL_KEY_STORE_TYPE", TestKeystores.TYPE)
                .withEnv("SOLR_SSL_TRUST_STORE", STORE_IN_CONTAINER)
                .withEnv("SOLR_SSL_TRUST_STORE_PASSWORD", TestKeystores.PASSWORD)
                .withEnv("SOLR_SSL_TRUST_STORE_TYPE", TestKeystores.TYPE)
                .withEnv("SOLR_SSL_NEED_CLIENT_AUTH", "false")
                .withEnv("SOLR_SSL_WANT_CLIENT_AUTH", "false")
                .withCommand("solr-precreate", CORE);
        container.start();
        baseUrl = "https://" + container.getHost() + ":" + container.getMappedPort(8983) + "/solr";
    }

    @AfterAll
    static void stop() {
        if (container != null) container.stop();
    }

    private static Map<String, String> sslProps() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, baseUrl);
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, CORE);
        p.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "REQUIRED");
        p.put(SolrSinkConfig.SECURITY_PROTOCOL_CONFIG, "SSL");
        p.put(SolrSinkConfig.SSL_KEYSTORE_LOCATION_CONFIG, stores.keystore.toString());
        p.put(SolrSinkConfig.SSL_KEYSTORE_PASSWORD_CONFIG, TestKeystores.PASSWORD);
        p.put(SolrSinkConfig.SSL_KEYSTORE_TYPE_CONFIG, TestKeystores.TYPE);
        p.put(SolrSinkConfig.SSL_TRUSTSTORE_LOCATION_CONFIG, stores.truststore.toString());
        p.put(SolrSinkConfig.SSL_TRUSTSTORE_PASSWORD_CONFIG, TestKeystores.PASSWORD);
        p.put(SolrSinkConfig.SSL_TRUSTSTORE_TYPE_CONFIG, TestKeystores.TYPE);
        return p;
    }

    /** The port opens before the core loads; poll through the TLS client itself. */
    private static void awaitReady(SolrClient client) throws Exception {
        Exception last = null;
        for (int i = 0; i < 240; i++) {
            try {
                client.query(CORE, new SolrQuery("*:*").setRows(0));
                return;
            } catch (Exception e) {
                last = e;
                Thread.sleep(250);
            }
        }
        throw new IllegalStateException("TLS Solr core never became ready", last);
    }

    @Test
    void sslClientCompletesTheHandshakeAndIndexesThroughTheWriter() throws Exception {
        SolrSinkConfig cfg = new SolrSinkConfig(sslProps());
        SolrClient client = SolrClientFactory.create(cfg);
        assertThat(client).isInstanceOf(Http2SolrClient.class);
        awaitReady(client);
        try (SolrWriter writer = new SolrWriter(client, cfg)) {
            for (int i = 1; i <= 3; i++) {
                Struct account = new Struct(ACCOUNT)
                        .put("id", "acct-" + i)
                        .put("owner", "owner " + i)
                        .put("roles", Arrays.asList("reader", i == 1 ? "admin" : "writer"));
                writer.write(new SinkRecord("accounts", 0, Schema.STRING_SCHEMA, "acct-" + i, ACCOUNT, account, i));
            }
            writer.flush();
            assertThat(writer.recordsWritten()).isEqualTo(3);
        }
        // Read back over a second TLS client built the same way.
        try (SolrClient reader = SolrClientFactory.create(cfg)) {
            long hits = 0;
            for (int i = 0; i < 100 && hits != 1; i++) {
                hits = reader.query(CORE, new SolrQuery("roles:admin")).getResults().getNumFound();
                if (hits != 1) Thread.sleep(100);
            }
            assertThat(hits).isEqualTo(1);
        }
    }
}
