package com.una.kafka.connect.solr;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the byte-size accumulator that the converter feeds to {@link SolrBulkProcessor}
 * so it can skip a second-pass walk of the doc when {@code bulk.size.bytes > 0}.
 *
 * <p>Contract:</p>
 * <ul>
 *   <li>When {@code bulk.size.bytes} is unset (default), {@link SolrRecordConverter#lastConversionByteEstimate()}
 *       returns {@code -1} — signals "no estimate available" and the bulk processor falls back to
 *       its static {@code estimateBytes(doc)}.</li>
 *   <li>When enabled, the inline estimate must match (within a small tolerance) the result of the
 *       standalone {@link SolrBulkProcessor#estimateBytes(SolrInputDocument)} pass.</li>
 *   <li>Accumulator must reset across consecutive {@code convert()} calls — no bleeding.</li>
 * </ul>
 */
class SolrRecordConverterByteTrackingTest {

    private SolrSinkConfig cfg(Map<String, String> extras) {
        Map<String, String> props = new HashMap<>();
        props.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        props.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "c");
        props.putAll(extras);
        return new SolrSinkConfig(props);
    }

    private SolrSinkConfig tracking() {
        Map<String, String> extras = new HashMap<>();
        extras.put(SolrSinkConfig.BULK_SIZE_BYTES_CONFIG, "1048576");
        return cfg(extras);
    }

    private SinkRecord rec(Schema schema, Struct value) {
        return new SinkRecord("c", 0, Schema.STRING_SCHEMA, "key-1", schema, value, 1L);
    }

    @Test
    void disabledTrackingReturnsSentinel() {
        // Default bulk.size.bytes is 5 MiB (tracking enabled). Explicitly disable with 0.
        Map<String, String> extras = new HashMap<>();
        extras.put(SolrSinkConfig.BULK_SIZE_BYTES_CONFIG, "0");
        SolrRecordConverter converter = new SolrRecordConverter(cfg(extras));
        Schema schema = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        Struct value = new Struct(schema).put("x", "y");
        converter.convert(rec(schema, value));
        assertThat(converter.lastConversionByteEstimate())
                .as("returns -1 when bulk.size.bytes=0 (tracking explicitly disabled)")
                .isEqualTo(-1L);
    }

    @Test
    void enabledTrackingAgreesWithStandaloneEstimateForScalarFields() {
        SolrRecordConverter converter = new SolrRecordConverter(tracking());
        Schema schema = SchemaBuilder.struct()
                .field("first_name", Schema.STRING_SCHEMA)
                .field("last_name", Schema.STRING_SCHEMA)
                .field("age", Schema.INT32_SCHEMA)
                .field("active", Schema.BOOLEAN_SCHEMA)
                .build();
        Struct value = new Struct(schema)
                .put("first_name", "Ada")
                .put("last_name", "Lovelace")
                .put("age", 36)
                .put("active", true);
        SolrInputDocument doc = converter.convert(rec(schema, value));

        long inline = converter.lastConversionByteEstimate();
        long standalone = SolrBulkProcessor.estimateBytes(doc);
        assertThat(inline)
                .as("inline accumulator should match the standalone walk")
                .isEqualTo(standalone);
    }

    @Test
    void enabledTrackingAgreesWithStandaloneEstimateForBytesAndLogicalTypes() {
        SolrRecordConverter converter = new SolrRecordConverter(tracking());
        Schema schema = SchemaBuilder.struct()
                .field("payload", Schema.BYTES_SCHEMA)
                .field("created", org.apache.kafka.connect.data.Timestamp.SCHEMA)
                .field("price", org.apache.kafka.connect.data.Decimal.schema(2))
                .build();
        Struct value = new Struct(schema)
                .put("payload", new byte[]{1, 2, 3, 4, 5, 6, 7, 8})
                .put("created", new java.util.Date(0L))
                .put("price", new java.math.BigDecimal("9.99"));
        SolrInputDocument doc = converter.convert(rec(schema, value));

        long inline = converter.lastConversionByteEstimate();
        long standalone = SolrBulkProcessor.estimateBytes(doc);
        assertThat(inline).isEqualTo(standalone);
    }

    @Test
    void arrayFieldEstimateOverCountsBySafeMargin() {
        // Each array element is added via a separate addField call. The inline accumulator
        // counts the field name (`tags`) on every call; the standalone walk counts it once
        // per field. For N array items, inline overshoots by (N - 1) × nameLength × 2 bytes.
        //
        // This over-count is the SAFE direction for the bulk.size.bytes threshold — it can
        // only trigger slightly-earlier flushes, never larger-than-configured batches.
        SolrRecordConverter converter = new SolrRecordConverter(tracking());
        Schema arraySchema = SchemaBuilder.array(Schema.STRING_SCHEMA).build();
        Schema schema = SchemaBuilder.struct().field("tags", arraySchema).build();
        java.util.List<String> items = java.util.Arrays.asList("alpha", "beta", "gamma");
        Struct value = new Struct(schema).put("tags", items);
        SolrInputDocument doc = converter.convert(rec(schema, value));

        long inline = converter.lastConversionByteEstimate();
        long standalone = SolrBulkProcessor.estimateBytes(doc);
        long expectedOverCount = (long) (items.size() - 1) * "tags".length() * 2L;
        assertThat(inline)
                .as("inline must over-count arrays by exactly (N-1)*name*2 bytes")
                .isEqualTo(standalone + expectedOverCount);
    }

    @Test
    void nestedStructEstimateAgreesWithStandalone() {
        SolrRecordConverter converter = new SolrRecordConverter(tracking());
        Schema innerSchema = SchemaBuilder.struct()
                .field("city", Schema.STRING_SCHEMA)
                .field("zip", Schema.STRING_SCHEMA)
                .build();
        Schema outerSchema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("addr", innerSchema)
                .build();
        Struct innerValue = new Struct(innerSchema).put("city", "Tel Aviv").put("zip", "6100000");
        Struct outerValue = new Struct(outerSchema).put("name", "Ada").put("addr", innerValue);
        SolrInputDocument doc = converter.convert(rec(outerSchema, outerValue));

        long inline = converter.lastConversionByteEstimate();
        long standalone = SolrBulkProcessor.estimateBytes(doc);
        assertThat(inline).isEqualTo(standalone);
    }

    @Test
    void schemalessTopLevelMapAgreesWithStandalone() {
        SolrRecordConverter converter = new SolrRecordConverter(tracking());
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("name", "Alice");
        value.put("age", 30);
        value.put("active", false);
        SinkRecord record = new SinkRecord("c", 0, Schema.STRING_SCHEMA, "k", null, value, 1L);
        SolrInputDocument doc = converter.convert(record);

        long inline = converter.lastConversionByteEstimate();
        long standalone = SolrBulkProcessor.estimateBytes(doc);
        assertThat(inline).isEqualTo(standalone);
    }

    @Test
    void accumulatorResetsBetweenRecords() {
        SolrRecordConverter converter = new SolrRecordConverter(tracking());
        Schema schema = SchemaBuilder.struct().field("name", Schema.STRING_SCHEMA).build();

        Struct small = new Struct(schema).put("name", "a");
        converter.convert(rec(schema, small));
        long firstEstimate = converter.lastConversionByteEstimate();

        Struct large = new Struct(schema).put("name", "this-is-a-longer-string-value");
        converter.convert(rec(schema, large));
        long secondEstimate = converter.lastConversionByteEstimate();

        // Second estimate must reflect ONLY the second record (no bleed from the first).
        assertThat(secondEstimate).isGreaterThan(firstEstimate);
        // And both must independently match their respective standalone walks.
        converter.convert(rec(schema, small));
        long re = converter.lastConversionByteEstimate();
        assertThat(re).isEqualTo(firstEstimate);
    }

    @Test
    void mappingVersionIncludedInEstimate() {
        Map<String, String> extras = new HashMap<>();
        extras.put(SolrSinkConfig.BULK_SIZE_BYTES_CONFIG, "1048576");
        extras.put(SolrSinkConfig.MAPPING_VERSION_CONFIG, "v3");
        SolrRecordConverter converter = new SolrRecordConverter(cfg(extras));
        Schema schema = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        Struct value = new Struct(schema).put("x", "y");
        SolrInputDocument doc = converter.convert(rec(schema, value));

        long inline = converter.lastConversionByteEstimate();
        long standalone = SolrBulkProcessor.estimateBytes(doc);
        assertThat(inline).isEqualTo(standalone);
        // sanity: doc has both id, _mapping_version, x
        assertThat(doc.getFieldNames()).containsExactlyInAnyOrder("id", "_mapping_version", "x");
    }

    @Test
    void manyRecordsRunHoldsAccumulatorStable() {
        // Defensive: 1k iterations check there's no slow drift bug (e.g. accidentally summing across records).
        SolrRecordConverter converter = new SolrRecordConverter(tracking());
        Schema schema = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        Struct value = new Struct(schema).put("x", "same-value-every-time");

        SolrInputDocument firstDoc = converter.convert(rec(schema, value));
        long baseline = converter.lastConversionByteEstimate();
        long standaloneBaseline = SolrBulkProcessor.estimateBytes(firstDoc);
        assertThat(baseline).isEqualTo(standaloneBaseline);

        for (int i = 0; i < 1_000; i++) {
            converter.convert(rec(schema, value));
            assertThat(converter.lastConversionByteEstimate())
                    .as("iteration %d", i)
                    .isEqualTo(baseline);
        }
    }
}
