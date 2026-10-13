package com.una.kafka.connect.solr;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.SolrDocument;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Fix for out-of-order same-key updates: the connector stamps each doc with its Kafka offset
 * (kafka.offset.version.field) and Solr's DocBasedVersionConstraints ignores any write whose
 * offset is older than the stored one. So even with max.in.flight.requests > 1, a late-arriving
 * OLDER update can never clobber a NEWER version of the same id.
 *
 * <p>Runs against a REAL in-process Solr (EmbeddedSolrServer) whose configset has the version
 * constraint — the full update chain runs, no mocks.
 */
class SolrOrderingEmbeddedTest {

    private EmbeddedSolrServer solr;

    @BeforeEach
    void setUp() throws Exception {
        solr = EmbeddedSolrSupport.start("embedded-solr-versioned");
    }

    @AfterEach
    void tearDown() {
        EmbeddedSolrSupport.stop(solr);
    }

    private SolrWriter writer() {
        Map<String, String> p = new LinkedHashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://embedded/solr");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, EmbeddedSolrSupport.CORE);
        p.put(SolrSinkConfig.KAFKA_OFFSET_VERSION_FIELD_CONFIG, "_offset_ver");
        p.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "UNUSED");
        p.put(SolrSinkConfig.SCHEMA_IGNORE_CONFIG, "true");
        p.put(SolrSinkConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG, "delete");
        return new SolrWriter(solr, new SolrSinkConfig(p), new SyncOffsetTracker());
    }

    private static SinkRecord rec(String id, String name, long offset) {
        Map<String, Object> v = new LinkedHashMap<>();
        v.put("id", id);
        v.put("name", name);
        return new SinkRecord("aggregated-user-data-v1", 0, Schema.STRING_SCHEMA, id, null, v, offset);
    }

    private static SinkRecord tombstone(String id, long offset) {
        return new SinkRecord("aggregated-user-data-v1", 0, Schema.STRING_SCHEMA, id, null, null, offset);
    }

    private String currentName(String id) throws Exception {
        solr.commit(EmbeddedSolrSupport.CORE);
        List<SolrDocument> r = solr.query(EmbeddedSolrSupport.CORE, new SolrQuery("id:" + id)).getResults();
        return r.isEmpty() ? null : (String) r.get(0).getFirstValue("name");
    }

    @Test
    void olderOffsetWriteIsIgnoredButNewerIsApplied() throws Exception {
        try (SolrWriter w = writer()) {
            // Newer version arrives first (offset 10).
            w.write(rec("1", "NEW", 10));
            w.flush();
            assertThat(currentName("1")).isEqualTo("NEW");

            // An OLDER update (offset 5) arrives late (out of order under concurrency) — ignored.
            w.write(rec("1", "STALE", 5));
            w.flush();
            assertThat(currentName("1")).as("older-offset write must NOT overwrite the newer doc").isEqualTo("NEW");

            // A genuinely newer update (offset 20) IS applied.
            w.write(rec("1", "NEWEST", 20));
            w.flush();
            assertThat(currentName("1")).as("newer-offset write must apply").isEqualTo("NEWEST");
        }
    }

    @Test
    void tombstoneDeleteStillWorksUnderTheVersionConstraint() throws Exception {
        try (SolrWriter w = writer()) {
            w.write(rec("5", "alive", 10));
            w.flush();
            assertThat(currentName("5")).isEqualTo("alive");

            w.write(tombstone("5", 20));
            w.flush();
            solr.commit(EmbeddedSolrSupport.CORE);
            // Soft-delete: the doc becomes a versioned tombstone marked _deleted; a query that
            // filters tombstones (as the app must) returns nothing — the user is "deleted".
            long live = solr.query(EmbeddedSolrSupport.CORE,
                    new SolrQuery("id:5").addFilterQuery("-_deleted:true")).getResults().getNumFound();
            assertThat(live).as("filtered query must not return the soft-deleted user").isEqualTo(0L);
        }
    }

    @Test
    void staleUpdateArrivingAfterADeleteMustNotResurrectTheUser() throws Exception {
        try (SolrWriter w = writer()) {
            w.write(rec("6", "v10", 10));
            w.flush();
            w.write(tombstone("6", 20));
            w.flush();
            assertThat(currentName("6")).as("deleted at offset 20").isNull();

            // Out-of-order: an update with offset 15 (OLDER than the delete@20) arrives late.
            w.write(rec("6", "resurrected", 15));
            w.flush();
            assertThat(currentName("6"))
                    .as("a stale pre-delete update must NOT resurrect a deleted user").isNull();
        }
    }
}
