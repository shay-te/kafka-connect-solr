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

    private static final int RECORDS = 200_000;

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
