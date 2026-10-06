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
 * Validates the PRODUCTION Solr configset (ob-love-admin-backend/docker/solr/aggregated_user_data,
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

    private static java.util.List<String> ids(EmbeddedSolrServer solr, SolrQuery q) throws Exception {
        java.util.List<String> ids = new java.util.ArrayList<>();
        solr.query(EmbeddedSolrSupport.CORE, q).getResults()
                .forEach(doc -> ids.add(doc.getFirstValue("id").toString()));
        return ids;
    }

    private static SolrQuery sorted(String... fieldAndOrder) {
        SolrQuery q = new SolrQuery("*:*").setRows(10);
        for (int i = 0; i < fieldAndOrder.length; i += 2) {
            q.addSort(fieldAndOrder[i], SolrQuery.ORDER.valueOf(fieldAndOrder[i + 1]));
        }
        return q;
    }

    /**
     * The integer columns and the numeric custom-field sort key behave as NUMBERS, with the exact
     * query shapes the admin backend sends: quoted exact matches ({@code status:"4"}), a range on
     * status, and the grid sorts. The old schema left these in the catch-all string, so 269 sorted
     * before 56 and {@code status <= 4} also matched 10.
     */
    @Test
    void integerColumnsAndNumericSortKeysSortAndFilterAsNumbers() throws Exception {
        EmbeddedSolrServer solr = EmbeddedSolrSupport.start("embedded-solr-prod");
        try (SolrWriter w = writer(solr)) {
            Map<String, Object> one = new LinkedHashMap<>();
            one.put("status", 1); one.put("height_to_cm", 91); one.put("conversation_id", 9);
            one.put("custom_field_5_sort_number", 2.0); one.put("custom_field_5_sort_value", "2");
            one.put("custom_field_6_sort_value", "banana");
            Map<String, Object> two = new LinkedHashMap<>();
            two.put("status", 4); two.put("height_to_cm", 269); two.put("conversation_id", 10);
            two.put("custom_field_5_sort_number", 10.0); two.put("custom_field_5_sort_value", "10");
            two.put("custom_field_6_sort_value", "apple");
            Map<String, Object> three = new LinkedHashMap<>();
            three.put("status", 10); three.put("height_to_cm", 56); three.put("conversation_id", 100);
            three.put("custom_field_5_sort_number", 100.0); three.put("custom_field_5_sort_value", "100");
            three.put("custom_field_6_sort_value", "cherry");
            Map<String, Object> four = new LinkedHashMap<>();   // no numbers at all
            four.put("custom_field_5_sort_value", "abc");
            w.write(record("1", 10, one));
            w.write(record("2", 11, two));
            w.write(record("3", 12, three));
            w.write(record("4", 13, four));
            w.flush();
            solr.commit(EmbeddedSolrSupport.CORE);

            // status: exact (quoted, as the query builder writes it), IN, and a real numeric range.
            assertThat(hits(solr, new SolrQuery("*:*").addFilterQuery("status:\"4\""))).isEqualTo(1L);
            assertThat(hits(solr, new SolrQuery("*:*").addFilterQuery("status:(\"1\" OR \"4\")"))).isEqualTo(2L);
            assertThat(hits(solr, new SolrQuery("*:*").addFilterQuery("status:[* TO 4]")))
                    .as("status 10 is above 4 numerically (a text range would include it)").isEqualTo(2L);

            // height_to_cm: 269 > 91 > 56; a user without a value is last in BOTH directions.
            assertThat(ids(solr, sorted("height_to_cm", "desc", "id", "asc")))
                    .containsExactly("2", "1", "3", "4");
            assertThat(ids(solr, sorted("height_to_cm", "asc", "id", "asc")))
                    .containsExactly("3", "1", "2", "4");

            // conversation_id: 100 > 10 > 9 (the relationship view).
            assertThat(ids(solr, sorted("conversation_id", "desc", "id", "asc")))
                    .containsExactly("3", "2", "1", "4");

            // NUMBER custom field: numeric key first, then the text key. 2 < 10 < 100; "abc" has no
            // number so it is last in both directions.
            assertThat(ids(solr, sorted("custom_field_5_sort_number", "asc", "custom_field_5_sort_value", "asc", "id", "asc")))
                    .containsExactly("1", "2", "3", "4");
            assertThat(ids(solr, sorted("custom_field_5_sort_number", "desc", "custom_field_5_sort_value", "desc", "id", "asc")))
                    .containsExactly("3", "2", "1", "4");

            // A field type with no numeric key: its sort_number ties on every document (the dynamic
            // field is declared, so naming it is valid) and the text key decides.
            assertThat(ids(solr, sorted("custom_field_6_sort_number", "asc", "custom_field_6_sort_value", "asc", "id", "asc")))
                    .containsExactly("2", "1", "3", "4");
        } finally {
            EmbeddedSolrSupport.stop(solr);
        }
    }

    /**
     * The admin backend's owned-user-ids paging (scheduled saved queries): the organization's own users
     * by {@code owner_organization_id} (no explicit field, so the catch-all string), in NUMBER order on
     * {@code id_sort} (copied from the string uniqueKey {@code id}), continuing after the last id of the
     * previous page with an exclusive range.
     */
    @Test
    void ownedUserIdsPageInNumberOrderAfterACursor() throws Exception {
        EmbeddedSolrServer solr = EmbeddedSolrSupport.start("embedded-solr-prod");
        try (SolrWriter w = writer(solr)) {
            long offset = 10;
            for (String id : new String[] {"2", "9", "10", "100", "11"}) {
                w.write(record(id, offset++, Map.of("owner_organization_id", 5)));
            }
            w.write(record("12", offset, Map.of("owner_organization_id", 6)));
            w.flush();
            solr.commit(EmbeddedSolrSupport.CORE);

            SolrQuery firstPage = sorted("id_sort", "asc").addFilterQuery("owner_organization_id:\"5\"");
            assertThat(ids(solr, firstPage)).containsExactly("2", "9", "10", "11", "100");

            SolrQuery nextPage = sorted("id_sort", "asc").addFilterQuery("owner_organization_id:\"5\"")
                    .addFilterQuery("id_sort:{10 TO *]");
            assertThat(ids(solr, nextPage)).as("strictly after the last id returned").containsExactly("11", "100");
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
