package com.una.kafka.connect.solr;

import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The existence probe's retry policy over REAL HTTP (a JDK server answering the Collections API):
 * a transient answer is retried, a refusal fails the task and names the status.
 */
class ExternalResourceManagerHttpErrorTest {

    private static final String LISTING = "<?xml version=\"1.0\"?><response><lst name=\"responseHeader\">"
            + "<int name=\"status\">0</int></lst><arr name=\"collections\"><str>users</str></arr></response>";

    private final Deque<Integer> statuses = new ArrayDeque<>();
    private HttpServer server;
    private SolrClient client;

    private ExternalResourceManager manager(Integer... answers) throws Exception {
        for (Integer answer : answers) statuses.add(answer);
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/solr/admin/collections", exchange -> {
            int status = statuses.size() > 1 ? statuses.poll() : statuses.peek();
            byte[] body = (status == 200 ? LISTING : "<response/>").getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/xml");
            exchange.sendResponseHeaders(status, body.length);
            try (OutputStream out = exchange.getResponseBody()) {
                out.write(body);
            }
        });
        server.start();
        String url = "http://127.0.0.1:" + server.getAddress().getPort() + "/solr";
        client = new Http2SolrClient.Builder(url).useHttp1_1(true)     // the JDK server speaks HTTP/1.1
                .withResponseParser(new XMLResponseParser()).build();
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, url);
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "users");
        p.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "REQUIRED");
        return new ExternalResourceManager(client, new SolrSinkConfig(p));
    }

    @AfterEach
    void stop() throws Exception {
        if (client != null) client.close();
        if (server != null) server.stop(0);
    }

    @Test
    void anIdentityTheCollectionsApiRefusesFailsTheTaskAndNamesTheStatus() throws Exception {
        // Retried as transient, the task stayed RUNNING and indexed nothing.
        ExternalResourceManager mgr = manager(401);
        assertThatThrownBy(() -> mgr.ensure("users"))
                .isInstanceOf(ConnectException.class)
                .isNotInstanceOf(RetriableException.class)
                .hasMessageContaining("401");
    }

    @Test
    void aTransientErrorOnALaterCollectionIsRetriedNotReportedMissing() throws Exception {
        ExternalResourceManager mgr = manager(200, 503);
        mgr.ensure("users");
        assertThat(mgr.isKnown("users")).isTrue();
        assertThatThrownBy(() -> mgr.ensure("orders")).isInstanceOf(RetriableException.class);
        assertThat(mgr.isKnown("orders")).isFalse();
    }
}
