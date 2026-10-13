package com.una.kafka.connect.solr;

import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateHttp2SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The streaming client sends from its own runner threads and hands Solr's answer only to handleError, so a
 * streamed batch Solr refuses must fail the flush: preCommit then commits nothing past it.
 */
class SolrBulkProcessorStreamingRefusalTest {

    private HttpServer solr;
    // The HTTP status this stand-in Solr answers every update with.
    private volatile int status;

    @BeforeEach
    void startSolr() throws IOException {
        solr = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        solr.createContext("/solr", exchange -> {
            exchange.getRequestBody().readAllBytes();
            exchange.sendResponseHeaders(status, -1);
            exchange.close();
        });
        solr.start();
    }

    @AfterEach
    void stopSolr() {
        solr.stop(0);
    }

    private StreamingSolrClient streamingClient() {
        String url = "http://127.0.0.1:" + solr.getAddress().getPort() + "/solr";
        // The JDK server speaks HTTP/1.1 only.
        Http2SolrClient http = new Http2SolrClient.Builder(url).useHttp1_1(true).build();
        return new StreamingSolrClient(new ConcurrentUpdateHttp2SolrClient.Builder(url, http, true));
    }

    private static SolrSinkConfig cfg() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "users");
        p.put(SolrSinkConfig.LINGER_MS_CONFIG, "100000");
        p.put(SolrSinkConfig.MAX_RETRIES_CONFIG, "0");
        p.put(SolrSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, "warn");
        return new SolrSinkConfig(p);
    }

    private static void bufferOneDocument(SolrBulkProcessor bulk) {
        SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", "1");
        bulk.upsert("users", doc, -1L, new OffsetState(new TopicPartition("t", 0), 1L),
                new SinkRecord("t", 0, Schema.STRING_SCHEMA, "k", null, "v", 1L));
    }

    @Test
    void aStreamedBatchSolrRefusesStopsTheTask() throws Exception {
        status = 400;
        try (StreamingSolrClient client = streamingClient()) {
            SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg());
            bufferOneDocument(bulk);

            assertThatThrownBy(bulk::flushSync).isInstanceOf(ConnectException.class)
                    .isNotInstanceOf(RetriableException.class);
            assertThatThrownBy(bulk::throwIfFatal).hasMessageContaining("Solr streaming flush failed");
            bulk.close();
        }
    }

    @Test
    void aStreamedBatchSolrCannotTakeYetIsRetried() throws Exception {
        status = 503;
        try (StreamingSolrClient client = streamingClient()) {
            SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg());
            bufferOneDocument(bulk);

            assertThatThrownBy(bulk::flushSync).isInstanceOf(RetriableException.class);
            bulk.throwIfFatal();
            bulk.close();
        }
    }

    @Test
    void aStreamedBatchSolrAcceptsFlushesCleanly() throws Exception {
        status = 200;
        try (StreamingSolrClient client = streamingClient()) {
            SolrBulkProcessor bulk = new SolrBulkProcessor(client, cfg());
            bufferOneDocument(bulk);

            bulk.flushSync();
            bulk.throwIfFatal();
            bulk.close();
        }
    }
}
