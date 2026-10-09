package com.una.kafka.connect.solr;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The README's quick start: the shipped connector config passes the connector's own validation, the REST call
 * posts it as JSON, and the compose file runs the broker the Connect worker points at.
 */
class QuickstartExampleTest {

    private static final Path PROPERTIES = Paths.get("config", "quickstart-solr.properties");
    private static final Path REST_BODY = Paths.get("config", "quickstart-solr.json");
    private static final Pattern COMPOSE_SERVICE = Pattern.compile("^  ([\\w.-]+):\\s*$", Pattern.MULTILINE);
    private static final Pattern BOOTSTRAP_HOST = Pattern.compile("CONNECT_BOOTSTRAP_SERVERS:\\s*\"?([\\w.-]+):\\d+");
    private static final Pattern REPLICATION_FACTOR =
            Pattern.compile("CONNECT_(CONFIG|OFFSET|STATUS)_STORAGE_REPLICATION_FACTOR:\\s*\"?(\\d+)");
    private static final Pattern PROPERTIES_BLOCK = Pattern.compile("```properties\\r?\\n(.*?)```", Pattern.DOTALL);
    private static final Pattern IN_FLIGHT = Pattern.compile("^\\s*max\\.in\\.flight\\.requests\\s*=\\s*(\\d+)",
            Pattern.MULTILINE);
    private static final Pattern ORDERING_GUARANTEE = Pattern.compile(
            "^\\s*(ordering\\.lanes\\.enabled\\s*=\\s*true|kafka\\.offset\\.version\\.field\\s*=\\s*\\S)",
            Pattern.MULTILINE);
    private static final Pattern LANES_ON = Pattern.compile("^\\s*ordering\\.lanes\\.enabled\\s*=\\s*true",
            Pattern.MULTILINE);

    private static String read(Path path) throws IOException {
        return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    }

    private static Map<String, String> shippedProperties() throws IOException {
        Properties properties = new Properties();
        try (Reader reader = Files.newBufferedReader(PROPERTIES, StandardCharsets.UTF_8)) {
            properties.load(reader);
        }
        Map<String, String> config = new TreeMap<>();
        for (String key : properties.stringPropertyNames()) {
            config.put(key, properties.getProperty(key));
        }
        return config;
    }

    private static List<String> validationErrors(Map<String, String> config) {
        List<String> errors = new ArrayList<>();
        for (ConfigValue value : new SolrSinkConnector().validate(config).configValues()) {
            for (String message : value.errorMessages()) {
                errors.add(value.name() + ": " + message);
            }
        }
        return errors;
    }

    @Test
    void theShippedConfigPassesTheConnectorsOwnValidation() throws IOException {
        assertThat(validationErrors(shippedProperties())).isEmpty();
    }

    @Test
    void theRestBodyIsJsonCarryingTheShippedConfig() throws IOException {
        JsonNode body = new ObjectMapper().readTree(REST_BODY.toFile());
        Map<String, String> config = new TreeMap<>();
        body.get("config").fields().forEachRemaining(entry -> config.put(entry.getKey(), entry.getValue().asText()));

        assertThat(config).isEqualTo(shippedProperties());
        assertThat(body.get("name").asText()).isEqualTo(config.get("name"));
    }

    @Test
    void theReadmePostsTheJsonBody() throws IOException {
        String readme = read(Paths.get("README.md"));

        assertThat(readme).contains("--data @" + REST_BODY.toString().replace('\\', '/'));
        assertThat(readme).doesNotContain("--data @" + PROPERTIES.toString().replace('\\', '/'));
    }

    private static List<Path> documentation() throws IOException {
        List<Path> docs = new ArrayList<>();
        docs.add(Paths.get("README.md"));
        try (java.util.stream.Stream<Path> files = Files.list(Paths.get("docs"))) {
            files.filter(path -> path.toString().endsWith(".md")).forEach(docs::add);
        }
        return docs;
    }

    // A documented block's numeric setting, or the connector's default when the block leaves it out.
    private static long setting(String block, String key) {
        Matcher value = Pattern.compile("^\\s*" + Pattern.quote(key) + "\\s*=\\s*(\\d+)", Pattern.MULTILINE)
                .matcher(block);
        return value.find() ? Long.parseLong(value.group(1))
                : ((Number) SolrSinkConfig.config().configKeys().get(key).defaultValue).longValue();
    }

    @Test
    void everyDocumentedConfigWithLanesBuffersAFullBatchPerLane() throws IOException {
        List<String> refused = new ArrayList<>();
        for (Path doc : documentation()) {
            Matcher block = PROPERTIES_BLOCK.matcher(read(doc));
            while (block.find()) {
                if (!LANES_ON.matcher(block.group(1)).find()) {
                    continue;
                }
                long batch = setting(block.group(1), SolrSinkConfig.BATCH_SIZE_CONFIG);
                long lanes = setting(block.group(1), SolrSinkConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG);
                long buffered = setting(block.group(1), SolrSinkConfig.MAX_BUFFERED_RECORDS_CONFIG);
                if (buffered < batch * lanes) {
                    refused.add(doc + ": max.buffered.records " + buffered + " < " + batch + " x " + lanes);
                }
            }
        }
        assertThat(refused).as("the connector refuses these: each lane fills its own batch").isEmpty();
    }

    @Test
    void everyDocumentedConfigThatRaisesInFlightAlsoKeepsOrdering() throws IOException {
        List<String> refused = new ArrayList<>();
        for (Path doc : documentation()) {
            Matcher block = PROPERTIES_BLOCK.matcher(read(doc));
            while (block.find()) {
                Matcher inFlight = IN_FLIGHT.matcher(block.group(1));
                if (inFlight.find() && Integer.parseInt(inFlight.group(1)) > 1
                        && !ORDERING_GUARANTEE.matcher(block.group(1)).find()) {
                    refused.add(doc + ": " + inFlight.group().trim());
                }
            }
        }
        assertThat(refused).as("the connector refuses these: in-flight above 1 needs lanes or a version field")
                .isEmpty();
    }

    @Test
    void theComposeRunsTheBrokerTheWorkerConnectsTo() throws IOException {
        String compose = read(Paths.get("docker-compose.yaml"));
        Set<String> services = new TreeSet<>();
        Matcher service = COMPOSE_SERVICE.matcher(compose);
        while (service.find()) {
            services.add(service.group(1));
        }
        Matcher bootstrap = BOOTSTRAP_HOST.matcher(compose);
        assertThat(bootstrap.find()).as("CONNECT_BOOTSTRAP_SERVERS is set").isTrue();
        assertThat(services).contains(bootstrap.group(1));

        // One broker: Connect's own topics cannot ask for the default replication factor of 3.
        Map<String, String> factors = new TreeMap<>();
        Matcher factor = REPLICATION_FACTOR.matcher(compose);
        while (factor.find()) {
            factors.put(factor.group(1), factor.group(2));
        }
        assertThat(factors).containsOnlyKeys("CONFIG", "OFFSET", "STATUS").containsValues("1");
        assertThat(new TreeSet<>(factors.values())).containsExactly("1");
    }
}
