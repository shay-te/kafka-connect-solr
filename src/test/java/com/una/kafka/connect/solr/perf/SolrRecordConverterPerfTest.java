package com.una.kafka.connect.solr.perf;

import com.una.kafka.connect.solr.SolrRecordConverter;
import com.una.kafka.connect.solr.SolrSinkConfig;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Throughput test for the pure-CPU conversion stage. No I/O.
 * Excluded from the default test run; opt in with {@code mvn test -Pperf}.
 */
@Tag("performance")
class SolrRecordConverterPerfTest {

    private static final int RECORDS = Integer.getInteger("perf.records", 200_000);

    @Test
    void convertsAtLeast50kRecordsPerSecond() {
        Map<String, String> props = new HashMap<>();
        props.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        props.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "perf");
        SolrRecordConverter converter = new SolrRecordConverter(new SolrSinkConfig(props));

        Schema schema = userLikeSchema();
        Struct sample = sampleStruct(schema);
        SinkRecord prototype = new SinkRecord("perf", 0, Schema.STRING_SCHEMA, "k", schema, sample, 1L);

        // Warm-up
        for (int i = 0; i < 5_000; i++) {
            converter.convert(prototype);
        }

        long t0 = System.nanoTime();
        long sum = 0;
        for (int i = 0; i < RECORDS; i++) {
            SolrInputDocument doc = converter.convert(prototype);
            sum += doc.size(); // prevent JIT from eliding the loop
        }
        long ns = System.nanoTime() - t0;
        double rps = RECORDS / (ns / 1_000_000_000.0);
        System.out.printf("SolrRecordConverter: %d records in %d ms (%.0f r/s, sentinel=%d)%n",
                RECORDS, ns / 1_000_000L, rps, sum);

        assertThat(rps).isGreaterThan(50_000.0);
    }

    /**
     * The REAL production shape: a schemaless JSON document (Map) as kstreams emits it — nested
     * custom_fields / funnel_leads / promises arrays + denormalized filter fields — converted AND
     * serialized to the _src source field (the two things the connector does per Debezium record).
     */
    @Test
    void convertsRealisticNestedDocumentWithSource() throws Exception {
        Map<String, String> props = new HashMap<>();
        props.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        props.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "perf");
        SolrRecordConverter converter = new SolrRecordConverter(new SolrSinkConfig(props));
        com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();

        Map<String, Object> value = realisticUser();
        SinkRecord prototype = new SinkRecord("perf", 0, null, "k", null, value, 1L);

        for (int i = 0; i < 5_000; i++) {
            converter.convert(prototype);
            mapper.writeValueAsString(value);
        }
        long t0 = System.nanoTime();
        long sum = 0;
        for (int i = 0; i < RECORDS; i++) {
            sum += converter.convert(prototype).size();
            sum += mapper.writeValueAsString(value).length();  // _src serialization
        }
        long ns = System.nanoTime() - t0;
        double rps = RECORDS / (ns / 1_000_000_000.0);
        System.out.printf("SolrRecordConverter[realistic+_src]: %d records in %d ms (%.0f r/s, sentinel=%d)%n",
                RECORDS, ns / 1_000_000L, rps, sum);
        assertThat(rps).isGreaterThan(50_000.0);
    }

    private static Map<String, Object> realisticUser() {
        Map<String, Object> u = new HashMap<>();
        u.put("id", 12345);
        u.put("first_name", "Ada"); u.put("last_name", "Lovelace"); u.put("email", "ada@example.com");
        u.put("status", 1); u.put("gender", 2); u.put("height", 172); u.put("completion", 80);
        u.put("location", "40.7484,-73.9857"); u.put("created_at", "2026-01-01");
        u.put("organization_id", 42); u.put("conversation_id", 100); u.put("package_name", "premium");
        java.util.List<Map<String, Object>> customFields = new java.util.ArrayList<>();
        for (int c = 1; c <= 5; c++) {
            Map<String, Object> cf = new HashMap<>();
            cf.put("id", 900 + c); cf.put("custom_field_id", c); cf.put("type", 2);
            cf.put("value_2", "value-" + c); cf.put("sort_value", "value-" + c); cf.put("deleted_at_token", 0);
            customFields.add(cf);
            u.put("custom_field_" + c + "_value_2", "value-" + c);       // denormalized
            u.put("custom_field_" + c + "_sort_value", "value-" + c);
        }
        u.put("custom_fields", customFields);
        java.util.List<Map<String, Object>> leads = new java.util.ArrayList<>();
        for (int s = 1; s <= 2; s++) {
            Map<String, Object> l = new HashMap<>();
            l.put("id", s); l.put("stage_id", s); l.put("score", 80 + s);
            l.put("entered_stage_at", "2026-06-0" + s); l.put("deleted_at", null);
            leads.add(l);
            u.put("funnel_lead_" + s + "_score", 80 + s);
        }
        u.put("funnel_leads", leads);
        java.util.List<Map<String, Object>> promises = new java.util.ArrayList<>();
        for (int p = 1; p <= 3; p++) {
            Map<String, Object> pr = new HashMap<>();
            pr.put("id", p); pr.put("status", p); pr.put("due_date", "2026-08-0" + p);
            promises.add(pr);
        }
        u.put("promises", promises);
        Map<String, Object> summary = new HashMap<>();
        summary.put("total", 3); summary.put("worst_status", 1);
        u.put("promises_summary", summary);
        return u;
    }

    static Schema userLikeSchema() {
        return SchemaBuilder.struct()
                .field("user_id", Schema.INT64_SCHEMA)
                .field("first_name", Schema.STRING_SCHEMA)
                .field("last_name", Schema.STRING_SCHEMA)
                .field("email", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .field("height", Schema.FLOAT64_SCHEMA)
                .field("status", Schema.INT32_SCHEMA)
                .field("gender", Schema.INT32_SCHEMA)
                .field("location", Schema.OPTIONAL_STRING_SCHEMA)
                .field("language", Schema.OPTIONAL_STRING_SCHEMA)
                .field("created_at", Schema.STRING_SCHEMA)
                .field("updated_at", Schema.STRING_SCHEMA)
                .field("organization_id", Schema.INT64_SCHEMA)
                .field("package_name", Schema.OPTIONAL_STRING_SCHEMA)
                .field("completion", Schema.INT32_SCHEMA)
                .build();
    }

    static Struct sampleStruct(Schema schema) {
        return new Struct(schema)
                .put("user_id", 12345L)
                .put("first_name", "Ada")
                .put("last_name", "Lovelace")
                .put("email", "ada@example.com")
                .put("age", 36)
                .put("height", 1.72)
                .put("status", 1)
                .put("gender", 2)
                .put("location", "40.7484,-73.9857")
                .put("language", "en")
                .put("created_at", "2026-01-01")
                .put("updated_at", "2026-06-05")
                .put("organization_id", 42L)
                .put("package_name", "premium")
                .put("completion", 80);
    }
}
