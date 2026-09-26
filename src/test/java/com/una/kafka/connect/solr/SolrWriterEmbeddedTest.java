package com.una.kafka.connect.solr;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link SolrWriter}'s full public write path — convert → buffer → flush → real Solr — against
 * an in-process {@link EmbeddedSolrServer}. Exercises the actual converter integration, offset
 * tracking and tombstone→delete behaviour, then queries the genuine index to prove persistence.
 * external.resource.usage=UNUSED + schema.ignore=true keep it on the standalone core (no cloud ops).
 */
class SolrWriterEmbeddedTest {

    private EmbeddedSolrServer solr;

    @BeforeEach
    void setUp() throws Exception {
        solr = EmbeddedSolrSupport.start();
    }

    @AfterEach
    void tearDown() {
        EmbeddedSolrSupport.stop(solr);
    }

    private SolrSinkConfig config(String... kv) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://embedded/solr");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, EmbeddedSolrSupport.CORE);
        p.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "UNUSED");  // no cloud collection ops
        p.put(SolrSinkConfig.SCHEMA_IGNORE_CONFIG, "true");             // no schema evolution
        for (int i = 0; i < kv.length; i += 2) p.put(kv[i], kv[i + 1]);
        return new SolrSinkConfig(p);
    }

    private SolrWriter writer(SolrSinkConfig config) {
        return new SolrWriter(solr, config, new SyncOffsetTracker());
    }

    private static final Schema SCHEMA = SchemaBuilder.struct()
            .field("first_name", Schema.STRING_SCHEMA)
            .field("age", Schema.INT32_SCHEMA)
            .field("active", Schema.BOOLEAN_SCHEMA)
            .build();

    private static SinkRecord record(String key, String name, int age, boolean active, long offset) {
        Struct v = new Struct(SCHEMA).put("first_name", name).put("age", age).put("active", active);
        return new SinkRecord(EmbeddedSolrSupport.CORE, 0, Schema.STRING_SCHEMA, key, SCHEMA, v, offset);
    }

    private long numFound(String q) throws Exception {
        QueryResponse rsp = solr.query(EmbeddedSolrSupport.CORE, new SolrQuery(q));
        return rsp.getResults().getNumFound();
    }

    @Test
    void writesRealRecordsThroughTheWholePipelineIntoSolr() throws Exception {
        try (SolrWriter w = writer(config())) {
            w.write(record("u1", "Ada", 36, true, 1L));
            w.write(record("u2", "Grace", 40, false, 2L));
            w.flush();
            solr.commit(EmbeddedSolrSupport.CORE);

            assertThat(numFound("*:*")).isEqualTo(2);
            assertThat(w.recordsWritten()).isEqualTo(2);

            SolrDocument u1 = solr.query(EmbeddedSolrSupport.CORE, new SolrQuery("id:u1"))
                    .getResults().get(0);
            assertThat(u1.getFieldValues("first_name")).contains("Ada");
            assertThat(u1.getFieldValues("age")).contains("36");       // stored via dynamic string field
            assertThat(u1.getFieldValues("active")).contains("true");
        }
    }

    @Test
    void tombstoneDeletesWhenBehaviorIsDelete() throws Exception {
        try (SolrWriter w = writer(config(SolrSinkConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG, "delete"))) {
            w.write(record("victim", "temp", 1, true, 1L));
            w.flush();
            solr.commit(EmbeddedSolrSupport.CORE);
            assertThat(numFound("id:victim")).isEqualTo(1);

            // Null value = tombstone -> real deleteById against Solr.
            w.write(new SinkRecord(EmbeddedSolrSupport.CORE, 0, Schema.STRING_SCHEMA, "victim", null, null, 2L));
            w.flush();
            solr.commit(EmbeddedSolrSupport.CORE);
            assertThat(numFound("id:victim")).isEqualTo(0);
        }
    }

    @Test
    void tombstoneIgnoredByDefaultLeavesDocumentIntact() throws Exception {
        try (SolrWriter w = writer(config())) {  // behavior.on.null.values defaults to "ignore"
            w.write(record("keep", "stays", 1, true, 1L));
            w.write(new SinkRecord(EmbeddedSolrSupport.CORE, 0, Schema.STRING_SCHEMA, "keep", null, null, 2L));
            w.flush();
            solr.commit(EmbeddedSolrSupport.CORE);
            assertThat(numFound("id:keep")).isEqualTo(1); // tombstone was a no-op
        }
    }

    @Test
    void manyRecordsAcrossFlushesAllPersist() throws Exception {
        try (SolrWriter w = writer(config(SolrSinkConfig.BATCH_SIZE_CONFIG, "10"))) {
            for (int i = 0; i < 25; i++) {
                w.write(record("k" + i, "n" + i, i, i % 2 == 0, i));
            }
            w.flush();
            solr.commit(EmbeddedSolrSupport.CORE);
            assertThat(numFound("*:*")).isEqualTo(25);
            assertThat(w.recordsWritten()).isEqualTo(25);
        }
    }
}
