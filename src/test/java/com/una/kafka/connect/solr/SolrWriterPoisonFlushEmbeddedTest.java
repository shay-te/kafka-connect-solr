package com.una.kafka.connect.solr;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Full-pipeline poison-doc handling: a record that CONVERTS fine (so the conversion-time
 * DataException path never sees it) but that a REAL Solr rejects at flush time — a non-numeric
 * value in the schema's plong {@code height_cm} field. behavior.on.malformed.documents must be
 * honored at the flush seam too: warn/ignore keeps the task alive, indexes the healthy records
 * and skips only the poison one; fail pins the old kill-the-task behavior.
 */
class SolrWriterPoisonFlushEmbeddedTest {

    private static final String CORE = EmbeddedSolrSupport.CORE;

    private EmbeddedSolrServer solr;

    @BeforeEach
    void setUp() throws Exception {
        solr = EmbeddedSolrSupport.start();
    }

    @AfterEach
    void tearDown() {
        EmbeddedSolrSupport.stop(solr);
    }

    private SolrWriter writer(String... kv) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://embedded/solr");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, CORE);
        p.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "UNUSED"); // no cloud collection ops
        p.put(SolrSinkConfig.SCHEMA_IGNORE_CONFIG, "true");             // no schema evolution
        for (int i = 0; i < kv.length; i += 2) p.put(kv[i], kv[i + 1]);
        return new SolrWriter(solr, new SolrSinkConfig(p), new SyncOffsetTracker());
    }

    private static SinkRecord rec(String id, String name, long offset) {
        Map<String, Object> v = new LinkedHashMap<>();
        v.put("name", name);
        v.put("height_cm", 170L);
        return new SinkRecord(CORE, 0, Schema.STRING_SCHEMA, id, null, v, offset);
    }

    /** Converts without a DataException; only the real Solr flush rejects it (plong conflict). */
    private static SinkRecord poisonRec(String id, long offset) {
        Map<String, Object> v = new LinkedHashMap<>();
        v.put("name", "conflicted");
        v.put("height_cm", "one-eighty");
        return new SinkRecord(CORE, 0, Schema.STRING_SCHEMA, id, null, v, offset);
    }

    private long numFound(String q) throws Exception {
        return solr.query(CORE, new SolrQuery(q)).getResults().getNumFound();
    }

    @Test
    void warnModeKeepsTaskAliveIndexesHealthyRecordsAndCountsThePoisonFailed() throws Exception {
        try (SolrWriter w = writer(SolrSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, "warn")) {
            w.write(rec("u1", "Ada", 1L));
            w.write(poisonRec("px", 2L));
            w.write(rec("u2", "Grace", 3L));

            w.flush(); // must NOT throw — the task does not die
            solr.commit(CORE);

            assertThat(numFound("id:u1")).isEqualTo(1);
            assertThat(numFound("id:u2")).isEqualTo(1);
            assertThat(numFound("id:px")).as("Solr-rejected record must be skipped").isEqualTo(0);
            assertThat(w.recordsWritten()).isEqualTo(2);
            assertThat(w.recordsFailed()).isEqualTo(1);

            // The pipeline keeps flowing after the poison record.
            w.write(rec("u3", "Barbara", 4L));
            w.flush();
            solr.commit(CORE);
            assertThat(numFound("id:u3")).isEqualTo(1);
        }
    }

    @Test
    void failModeStillKillsTheFlushPinningCurrentBehavior() throws Exception {
        try (SolrWriter w = writer(SolrSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, "fail")) {
            w.write(rec("u1", "Ada", 1L));
            w.write(poisonRec("px", 2L));
            w.write(rec("u2", "Grace", 3L));

            assertThatThrownBy(w::flush)
                    .isInstanceOf(ConnectException.class)
                    .isNotInstanceOf(RetriableException.class);
        }
    }
}
