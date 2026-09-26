package com.una.kafka.connect.solr;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * The task's linger wake-up against a REAL in-process Solr and the real {@link SolrWriter}: a
 * partial batch left at the end of a burst must become searchable after linger.ms, without another
 * record arriving. Only Kafka Connect's {@link SinkTaskContext} (the framework) is a stand-in.
 */
class SolrSinkTaskLingerEmbeddedTest {

    private static final Schema USER = SchemaBuilder.struct().name("user")
            .field("id", Schema.STRING_SCHEMA)
            .field("name", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    private EmbeddedSolrServer solr;

    @BeforeEach
    void setUp() throws Exception {
        solr = EmbeddedSolrSupport.start();
    }

    @AfterEach
    void tearDown() {
        EmbeddedSolrSupport.stop(solr);
    }

    private final class EmbeddedTask extends SolrSinkTask {
        @Override
        protected SolrClient createClient(SolrSinkConfig config) {
            return solr;
        }
    }

    private Map<String, String> props(String batchSize, String lingerMs) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://embedded/solr");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, EmbeddedSolrSupport.CORE);
        p.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "UNUSED");
        p.put(SolrSinkConfig.BATCH_SIZE_CONFIG, batchSize);
        p.put(SolrSinkConfig.LINGER_MS_CONFIG, lingerMs);
        return p;
    }

    private static SinkRecord user(String id, long offset) {
        Struct value = new Struct(USER).put("id", id).put("name", "user " + id);
        return new SinkRecord(EmbeddedSolrSupport.CORE, 0, Schema.STRING_SCHEMA, id, USER, value, offset);
    }

    private long found(String id) throws Exception {
        solr.commit(EmbeddedSolrSupport.CORE);
        return solr.query(EmbeddedSolrSupport.CORE, new SolrQuery("id:" + id)).getResults().getNumFound();
    }

    @Test
    void aPartialBatchBecomesSearchableAfterLingerWithoutAnotherRecord() throws Exception {
        SinkTaskContext context = mock(SinkTaskContext.class);
        EmbeddedTask task = new EmbeddedTask();
        task.initialize(context);
        task.start(props("100", "75"));
        try {
            task.put(Collections.singletonList(user("tail", 1L)));
            verify(context).timeout(75L);
            assertThat(found("tail")).as("still buffered before linger").isZero();

            Thread.sleep(150);
            task.put(Collections.emptyList());                  // Connect's idle wake-up

            long hits = 0;
            for (int i = 0; i < 100 && hits == 0; i++) {
                hits = found("tail");
                if (hits == 0) Thread.sleep(50);
            }
            assertThat(hits).isEqualTo(1);
        } finally {
            task.stop();
        }
    }

    @Test
    void nothingBufferedAsksForNoEarlyWakeUp() throws Exception {
        SinkTaskContext context = mock(SinkTaskContext.class);
        EmbeddedTask task = new EmbeddedTask();
        task.initialize(context);
        task.start(props("1", "75"));                          // batch.size=1: flushed on write
        try {
            task.put(Collections.singletonList(user("full", 1L)));
            task.put(Collections.emptyList());
            verify(context, never()).timeout(anyLong());
        } finally {
            task.stop();
        }
    }
}
