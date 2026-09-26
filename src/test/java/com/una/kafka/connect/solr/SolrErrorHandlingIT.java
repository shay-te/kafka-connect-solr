package com.una.kafka.connect.solr;

import org.apache.kafka.connect.errors.RetriableException;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Regression IT for the {@code URLDecoder "QT"} error-masking bug: when Solr returns an
 * error over HTTP, the connector must surface the REAL Solr error, not an opaque
 * URL-decoding exception. Uses a real Solr container (the bug only manifests over HTTP via
 * Http2SolrClient — an in-process EmbeddedSolrServer can't reproduce it).
 */
class SolrErrorHandlingIT {

    private static GenericContainer<?> solr;
    private static String baseUrl;

    @BeforeAll
    static void start() {
        solr = new GenericContainer<>(DockerImageName.parse("solr:9.4"))
                .withExposedPorts(8983)
                .withCommand("solr-precreate", "errcore")
                .waitingFor(Wait.forHttp("/solr/errcore/select?q=*:*")
                        .forPort(8983).forStatusCode(200)
                        .withStartupTimeout(Duration.ofMinutes(2)));
        solr.start();
        baseUrl = "http://" + solr.getHost() + ":" + solr.getMappedPort(8983) + "/solr";
    }

    @AfterAll
    static void stop() {
        if (solr != null) solr.stop();
    }

    private SolrSinkConfig config() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, baseUrl);
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "errcore");
        p.put(SolrSinkConfig.MAX_RETRIES_CONFIG, "0");
        return new SolrSinkConfig(p);
    }

    @Test
    void solrErrorSurfacesRealMessageNotUrlDecoder() {
        try (Http2SolrClient client = new Http2SolrClient.Builder(baseUrl).build();
             SolrBulkProcessor bulk = new SolrBulkProcessor(client, config())) {

            // Two values for the single-valued uniqueKey 'id' — the exact error that surfaced
            // during the benchmark and got masked by the URLDecoder bug.
            SolrInputDocument doc = new SolrInputDocument();
            doc.addField("id", "dup-a");
            doc.addField("id", "dup-b");
            bulk.upsert("errcore", doc, null);

            Throwable thrown = catchThrowable(bulk::flushSync);
            // A Solr 400 is permanent — it must be non-retriable (else the pipeline loops forever
            // on this poison-pill record). RetriableException extends ConnectException, so we assert
            // it is a ConnectException but NOT a RetriableException.
            assertThat(thrown).isInstanceOf(org.apache.kafka.connect.errors.ConnectException.class);
            assertThat(thrown).isNotInstanceOf(RetriableException.class);

            StringWriter sw = new StringWriter();
            thrown.printStackTrace(new PrintWriter(sw));
            String trace = sw.toString();

            assertThat(trace)
                    .as("the REAL Solr error (multiple values for uniqueKey) must be diagnosable")
                    .containsIgnoringCase("multiple values");
            assertThat(trace)
                    .as("the URLDecoder masking bug must be gone")
                    .doesNotContain("URLDecoder");
        }
    }
}
