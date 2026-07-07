package com.una.kafka.connect.solr;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Validates the PRODUCTION Solr configset (objective_love_web/docker/solr/aggregated_user_data,
 * mirrored under embedded-solr-prod) actually loads and does what the connector needs: the
 * spatial location field powers geofilt, and a tombstone hard-deletes the user (soft-deleted
 * users are kept out of Solr entirely by the streamer, so there is no _deleted field to filter).
 * A broken configset = a broken deploy, so we boot it for real.
 */
class ProdConfigsetValidationTest {

    private static SolrWriter writer(EmbeddedSolrServer solr) {
        Map<String, String> p = new LinkedHashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://embedded/solr");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, EmbeddedSolrSupport.CORE);
        p.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "UNUSED");
        p.put(SolrSinkConfig.SCHEMA_IGNORE_CONFIG, "true");
        p.put(SolrSinkConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG, "delete");
        p.put(SolrSinkConfig.SOURCE_FIELD_CONFIG, "_src");
        p.put(SolrSinkConfig.SOURCE_EXCLUDE_FIELDS_CONFIG, "custom_field_,funnel_lead_");
        return new SolrWriter(solr, new SolrSinkConfig(p), new SyncOffsetTracker());
    }

    private static SinkRecord user(String id, String location, long offset) {
        Map<String, Object> v = new LinkedHashMap<>();
        v.put("id", id);
        v.put("first_name", "Ada");
        if (location != null) v.put("location", location);
        return new SinkRecord("aggregated-user-data-v1", 0, Schema.STRING_SCHEMA, id, null, v, offset);
    }

    private static SinkRecord tombstone(String id, long offset) {
        return new SinkRecord("aggregated-user-data-v1", 0, Schema.STRING_SCHEMA, id, null, null, offset);
    }

    private static SinkRecord record(String id, long offset, Map<String, Object> fields) {
        Map<String, Object> v = new LinkedHashMap<>();
        v.put("id", id);
        v.putAll(fields);
        return new SinkRecord("aggregated-user-data-v1", 0, Schema.STRING_SCHEMA, id, null, v, offset);
    }

    private static long hits(EmbeddedSolrServer solr, SolrQuery q) throws Exception {
        return solr.query(EmbeddedSolrSupport.CORE, q).getResults().getNumFound();
    }

    @Test
    void numericRangeCaseInsensitiveSearchAndSortWork() throws Exception {
        EmbeddedSolrServer solr = EmbeddedSolrSupport.start("embedded-solr-prod");
        try (SolrWriter w = writer(solr)) {
            Map<String, Object> ada = new LinkedHashMap<>();
            ada.put("first_name", "Ada"); ada.put("email", "Ada@X.io"); ada.put("height", 170);
            ada.put("custom_field_5_value_4", java.util.List.of("Gold"));
            ada.put("custom_field_5_sort_value", "Gold");
            Map<String, Object> grace = new LinkedHashMap<>();
            grace.put("first_name", "Grace"); grace.put("email", "grace@x.io"); grace.put("height", 90);
            grace.put("custom_field_5_value_4", java.util.List.of("Silver"));
            grace.put("custom_field_5_sort_value", "Silver");
            w.write(record("1", 10, ada));
            w.write(record("2", 11, grace));
            w.flush();
            solr.commit(EmbeddedSolrSupport.CORE);

            // (1) NUMERIC range: height>=100 matches Ada(170) but NOT Grace(90). A lexicographic
            // string range would wrongly include "90" > "100", so this proves numeric typing.
            assertThat(hits(solr, new SolrQuery("*:*").addFilterQuery("height:[100 TO *]")))
                    .as("numeric range must treat height as a number").isEqualTo(1L);

            // (2) CASE-INSENSITIVE search: lower-case query matches mixed-case stored value.
            assertThat(hits(solr, new SolrQuery("first_name:ada")))
                    .as("first_name CI").isEqualTo(1L);
            assertThat(hits(solr, new SolrQuery("email:ada@x.io")))
                    .as("email CI").isEqualTo(1L);
            assertThat(hits(solr, new SolrQuery("_text:silver")))
                    .as("search-anything catch-all is CI").isEqualTo(1L);

            // (3) EXACT custom-field filter on the denormalized field.
            assertThat(hits(solr, new SolrQuery("custom_field_5_value_4:Gold")))
                    .as("exact custom-field terms").isEqualTo(1L);

            // (4) SORT on the docValues sort key.
            String firstId = solr.query(EmbeddedSolrSupport.CORE,
                            new SolrQuery("*:*").setSort("custom_field_5_sort_value", SolrQuery.ORDER.asc)
                                    .setRows(1))
                    .getResults().get(0).getFirstValue("id").toString();
            assertThat(firstId).as("asc sort by sort_value -> Gold(id 1) first").isEqualTo("1");
        } finally {
            EmbeddedSolrSupport.stop(solr);
        }
    }

    @Test
    void configsetLoadsAndSpatialWorksAndTombstoneHardDeletes() throws Exception {
        EmbeddedSolrServer solr = EmbeddedSolrSupport.start("embedded-solr-prod");
        try (SolrWriter w = writer(solr)) {
            w.write(user("1", "40.7128,-74.0060", 10));  // NYC
            w.flush();
            solr.commit(EmbeddedSolrSupport.CORE);

            // Spatial field works: geofilt within 10km of the point finds the user...
            assertThat(hits(solr, new SolrQuery("*:*")
                    .addFilterQuery("{!geofilt sfield=location pt=40.7128,-74.0060 d=10}")))
                    .as("geofilt near the point must match").isEqualTo(1L);
            // ...and a point far away does not.
            assertThat(hits(solr, new SolrQuery("*:*")
                    .addFilterQuery("{!geofilt sfield=location pt=0,0 d=10}")))
                    .as("geofilt far away must not match").isEqualTo(0L);

            // A tombstone (the streamer's soft-delete signal) hard-deletes the user: it is gone,
            // not merely marked. Solr never holds a soft-deleted user.
            w.write(tombstone("1", 20));
            w.flush();
            solr.commit(EmbeddedSolrSupport.CORE);
            assertThat(hits(solr, new SolrQuery("id:1")))
                    .as("tombstone must hard-delete the user").isEqualTo(0L);
        } finally {
            EmbeddedSolrSupport.stop(solr);
        }
    }

    @Test
    void sourceFieldRoundTripsTheOriginalNestedDocument() throws Exception {
        EmbeddedSolrServer solr = EmbeddedSolrSupport.start("embedded-solr-prod");
        try (SolrWriter w = writer(solr)) {
            // A user with a nested custom_fields array — the shape Solr flattening would drop.
            Map<String, Object> customField = new LinkedHashMap<>();
            customField.put("custom_field_id", 5);
            customField.put("type", 4);
            customField.put("value_4", "Gold");
            Map<String, Object> value = new LinkedHashMap<>();
            value.put("id", "7");
            value.put("first_name", "Ada");
            value.put("custom_fields", java.util.List.of(customField));
            value.put("custom_field_5_value_4", "Gold");   // index-only denormalized field
            value.put("funnel_lead_3_score", 88);          // index-only denormalized field
            w.write(new SinkRecord("aggregated-user-data-v1", 0, Schema.STRING_SCHEMA, "7", null, value, 30));
            w.flush();
            solr.commit(EmbeddedSolrSupport.CORE);

            Object src = solr.query(EmbeddedSolrSupport.CORE, new SolrQuery("id:7"))
                    .getResults().get(0).getFirstValue("_src");
            assertThat(src).as("_src stores the raw document JSON").isNotNull();
            @SuppressWarnings("unchecked")
            Map<String, Object> parsed = new com.fasterxml.jackson.databind.ObjectMapper()
                    .readValue(src.toString(), Map.class);
            // The nested array survives verbatim — this is what the query layer returns to the UI.
            assertThat(parsed.get("first_name")).isEqualTo("Ada");
            java.util.List<?> cfs = (java.util.List<?>) parsed.get("custom_fields");
            assertThat(cfs).hasSize(1);
            assertThat(((Map<?, ?>) cfs.get(0)).get("value_4")).isEqualTo("Gold");
            // ...but the index-only denormalized keys are EXCLUDED from the stored source.
            assertThat(parsed).as("denormalized filter fields must not pollute _src")
                    .doesNotContainKeys("custom_field_5_value_4", "funnel_lead_3_score");
        } finally {
            EmbeddedSolrSupport.stop(solr);
        }
    }
}
