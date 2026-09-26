package com.una.kafka.connect.solr;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Real Solr 9.4 container: drives SolrSchemaManager.evolveIfNeeded against a
 * managed schema so AddField requests actually hit the network and we can
 * verify the fields appear in Solr's schema afterwards.
 */
class SolrSchemaManagerIT {

    private static GenericContainer<?> container;
    private static SolrClient client;
    private static String baseUrl;

    @BeforeAll
    static void start() throws Exception {
        container = new GenericContainer<>(DockerImageName.parse("solr:9.4"))
                .withExposedPorts(8983)
                .withCommand("solr-precreate", "schema_demo");
        container.start();
        baseUrl = "http://" + container.getHost() + ":" + container.getMappedPort(8983) + "/solr";
        SolrTestSupport.awaitCoreReady(baseUrl, "schema_demo");
        client = new Http2SolrClient.Builder(baseUrl).build();
    }

    @AfterAll
    static void stop() throws Exception {
        if (client != null) client.close();
        if (container != null) container.stop();
    }

    private SolrSinkConfig cfg() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, baseUrl);
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "schema_demo");
        p.put(SolrSinkConfig.SCHEMA_AUTO_EVOLVE_CONFIG, "true");
        return new SolrSinkConfig(p);
    }

    @Test
    void evolveAddsScalarAndArrayFieldsToManagedSchema() throws Exception {
        SolrSchemaManager mgr = new SolrSchemaManager(client, cfg());
        Schema arr = SchemaBuilder.array(Schema.STRING_SCHEMA).build();
        Schema schema = SchemaBuilder.struct()
                .field("title_text", Schema.STRING_SCHEMA)
                .field("price_amount", Schema.FLOAT64_SCHEMA)
                .field("tags_list", arr)
                .build();
        mgr.evolveIfNeeded("schema_demo", schema);

        // Read back the schema's fields and verify our new ones are present.
        SchemaResponse.FieldsResponse resp =
                new SchemaRequest.Fields().process(client, "schema_demo");
        List<String> names = resp.getFields().stream()
                .map(f -> String.valueOf(f.get("name")))
                .collect(Collectors.toList());
        assertThat(names).contains("title_text", "price_amount", "tags_list");

        // Calling again must short-circuit via the seen-set (no further AddField).
        mgr.evolveIfNeeded("schema_demo", schema);
    }

    @Test
    void evolveNestedStructUsesDottedFieldNames() throws Exception {
        SolrSchemaManager mgr = new SolrSchemaManager(client, cfg());
        Schema addr = SchemaBuilder.struct()
                .field("city_text", Schema.STRING_SCHEMA)
                .field("zip_text", Schema.STRING_SCHEMA)
                .build();
        Schema schema = SchemaBuilder.struct().field("addr_struct", addr).build();
        mgr.evolveIfNeeded("schema_demo", schema);

        SchemaResponse.FieldsResponse resp =
                new SchemaRequest.Fields().process(client, "schema_demo");
        List<String> names = resp.getFields().stream()
                .map(f -> String.valueOf(f.get("name")))
                .collect(Collectors.toList());
        assertThat(names).contains("addr_struct.city_text", "addr_struct.zip_text");
    }
}
