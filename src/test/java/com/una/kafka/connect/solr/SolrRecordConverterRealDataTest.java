package com.una.kafka.connect.solr;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.common.SolrInputDocument;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Real, complex, adversarial data through {@link SolrRecordConverter} — deeply nested CDC
 * structs, every logical type (as fields AND array elements), byte encodings, maps with
 * non-String keys in both layout modes, atomic-update rewriting, id-strategy fallbacks,
 * unicode/emoji, and large payloads. No mocks — actual Connect Structs and Solr documents,
 * asserting the exact emitted field values.
 */
class SolrRecordConverterRealDataTest {

    private SolrSinkConfig config(String... kv) {
        Map<String, String> props = new LinkedHashMap<>();
        props.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://solr:8983/solr");
        props.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "users");
        for (int i = 0; i < kv.length; i += 2) props.put(kv[i], kv[i + 1]);
        return new SolrSinkConfig(props);
    }

    private SinkRecord rec(Schema schema, Object value, Object key) {
        return new SinkRecord("users", 3, key == null ? null : Schema.STRING_SCHEMA, key, schema, value, 99L);
    }

    // ---------- deeply nested structs -> dotted leaf fields ----------

    @Test
    void deeplyNestedStructFlattensToDottedLeafFields() {
        SolrRecordConverter c = new SolrRecordConverter(config());
        Schema geo = SchemaBuilder.struct().name("Geo")
                .field("lat", Schema.FLOAT64_SCHEMA).field("lon", Schema.FLOAT64_SCHEMA).build();
        Schema address = SchemaBuilder.struct()
                .field("city", Schema.STRING_SCHEMA).field("geo", geo).build();
        Schema employer = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA).field("address", address).build();
        Schema user = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA)
                .field("address", address)
                .field("employer", employer).build();

        Struct value = new Struct(user)
                .put("name", "Ada")
                .put("address", new Struct(address).put("city", "London")
                        .put("geo", new Struct(geo).put("lat", 51.5).put("lon", -0.12)))
                .put("employer", new Struct(employer).put("name", "Analytical Engine Co")
                        .put("address", new Struct(address).put("city", "Cambridge")
                                .put("geo", new Struct(geo).put("lat", 52.2).put("lon", 0.12))));

        SolrInputDocument doc = c.convert(rec(user, value, "u1"));
        assertThat(doc.getFieldValue("name")).isEqualTo("Ada");
        assertThat(doc.getFieldValue("address.city")).isEqualTo("London");
        assertThat(doc.getFieldValue("address.geo.lat")).isEqualTo(51.5);
        assertThat(doc.getFieldValue("employer.name")).isEqualTo("Analytical Engine Co");
        assertThat(doc.getFieldValue("employer.address.city")).isEqualTo("Cambridge");
        assertThat(doc.getFieldValue("employer.address.geo.lon")).isEqualTo(0.12);
    }

    // ---------- every logical type as a struct field ----------

    @Test
    void allLogicalTypesRenderAsIsoOrDecimalStrings() {
        SolrRecordConverter c = new SolrRecordConverter(config());
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        Schema schema = SchemaBuilder.struct()
                .field("balance", Decimal.schema(2))
                .field("created_at", Timestamp.SCHEMA)
                .field("birth", Date.SCHEMA)
                .field("login", Time.SCHEMA)
                .build();
        java.util.Date ts = java.util.Date.from(Instant.parse("2026-07-06T12:34:56Z"));
        Struct v = new Struct(schema)
                .put("balance", new BigDecimal("1234.56"))
                .put("created_at", ts)
                .put("birth", java.util.Date.from(Instant.parse("1815-12-10T00:00:00Z")))
                .put("login", new java.util.Date(3_600_000L)); // 01:00:00

        SolrInputDocument doc = c.convert(rec(schema, v, "u1"));
        assertThat(doc.getFieldValue("balance")).isEqualTo("1234.56");
        assertThat(doc.getFieldValue("created_at")).isEqualTo("2026-07-06T12:34:56Z");
        assertThat(doc.getFieldValue("birth").toString()).startsWith("1815-12-10T00:00:00Z");
        assertThat(doc.getFieldValue("login")).isEqualTo("1970-01-01T01:00:00Z");
    }

    // ---------- arrays: logical elements, bytes, structs ----------

    @Test
    void arraysOfLogicalBytesAndScalarElements() {
        SolrRecordConverter c = new SolrRecordConverter(config());
        Schema schema = SchemaBuilder.struct()
                .field("tags", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                .field("scores", SchemaBuilder.array(Schema.INT32_SCHEMA).build())
                .field("stamps", SchemaBuilder.array(Timestamp.SCHEMA).build())
                .field("amounts", SchemaBuilder.array(Decimal.schema(2)).build())
                .field("blobs", SchemaBuilder.array(Schema.BYTES_SCHEMA).build())
                .build();
        Struct v = new Struct(schema)
                .put("tags", List.of("a", "b", "c"))
                .put("scores", List.of(10, 20, 30))
                .put("stamps", List.of(java.util.Date.from(Instant.parse("2026-01-01T00:00:00Z"))))
                .put("amounts", List.of(new BigDecimal("9.99"), new BigDecimal("0.01")))
                .put("blobs", List.of(new byte[]{1, 2, 3}, ByteBuffer.wrap(new byte[]{4, 5})));

        SolrInputDocument doc = c.convert(rec(schema, v, "u1"));
        assertThat(doc.getFieldValues("tags")).containsExactly("a", "b", "c");
        assertThat(doc.getFieldValues("scores")).containsExactly(10, 20, 30);
        assertThat(doc.getFieldValues("stamps")).containsExactly("2026-01-01T00:00:00Z");
        assertThat(doc.getFieldValues("amounts")).containsExactly("9.99", "0.01");
        assertThat(doc.getFieldValues("blobs")).containsExactly(
                Base64.getEncoder().encodeToString(new byte[]{1, 2, 3}),
                Base64.getEncoder().encodeToString(new byte[]{4, 5}));
    }

    // ---------- byte[] and ByteBuffer top-level fields ----------

    @Test
    void bytesAndByteBufferFieldsBase64Encoded() {
        SolrRecordConverter c = new SolrRecordConverter(config());
        Schema schema = SchemaBuilder.struct()
                .field("avatar", Schema.BYTES_SCHEMA)
                .field("signature", Schema.BYTES_SCHEMA).build();
        byte[] avatar = "avatar-bytes".getBytes();
        Struct v = new Struct(schema).put("avatar", avatar).put("signature", ByteBuffer.wrap(new byte[]{9, 8, 7}));

        SolrInputDocument doc = c.convert(rec(schema, v, "u1"));
        assertThat(doc.getFieldValue("avatar")).isEqualTo(Base64.getEncoder().encodeToString(avatar));
        assertThat(doc.getFieldValue("signature")).isEqualTo(Base64.getEncoder().encodeToString(new byte[]{9, 8, 7}));
    }

    // ---------- maps with non-String keys, both layout modes ----------

    @Test
    void schemalessMapWithNonStringKeysCompactMode() {
        SolrRecordConverter c = new SolrRecordConverter(config(SolrSinkConfig.COMPACT_MAP_ENTRIES_CONFIG, "true"));
        Map<Object, Object> value = new LinkedHashMap<>();
        Map<Integer, Object> scores = new LinkedHashMap<>();
        scores.put(42, "answer");
        scores.put(7, 700);
        value.put("scores", scores);              // nested map with Integer keys -> "scores.42"
        value.put(100, "topLevelIntKey");         // non-String key at root -> "100"

        SolrInputDocument doc = c.convert(rec(null, value, "u1"));
        assertThat(doc.getFieldValue("scores.42")).isEqualTo("answer");
        assertThat(doc.getFieldValue("scores.7")).isEqualTo(700);
        assertThat(doc.getFieldValue("100")).isEqualTo("topLevelIntKey");
    }

    @Test
    void schemalessMapWithNonStringKeysNonCompactRoot() {
        SolrRecordConverter c = new SolrRecordConverter(config(SolrSinkConfig.COMPACT_MAP_ENTRIES_CONFIG, "false"));
        Map<Object, Object> value = new LinkedHashMap<>();
        value.put(2026, "year");                  // non-String root key -> String.valueOf -> "2026"
        value.put("name", "Ada");

        SolrInputDocument doc = c.convert(rec(null, value, "u1"));
        assertThat(doc.getFieldValue("2026")).isEqualTo("year");
        assertThat(doc.getFieldValue("name")).isEqualTo("Ada");
    }

    @Test
    void nonCompactNestedMapWrapsEachEntryAsKeyValue() {
        SolrRecordConverter c = new SolrRecordConverter(config(SolrSinkConfig.COMPACT_MAP_ENTRIES_CONFIG, "false"));
        Schema schema = SchemaBuilder.struct()
                .field("attrs", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build())
                .build();
        Map<String, String> attrs = new LinkedHashMap<>();
        attrs.put("hair", "brown");
        attrs.put("eyes", "blue");
        Struct v = new Struct(schema).put("attrs", attrs);

        SolrInputDocument doc = c.convert(rec(schema, v, "u1"));
        Collection<Object> entries = doc.getFieldValues("attrs");
        assertThat(entries).hasSize(2);
        // Each entry is a {key,value} LinkedHashMap preserving the original (typed) key.
        assertThat(entries).allSatisfy(e -> {
            @SuppressWarnings("unchecked")
            Map<String, Object> m = (Map<String, Object>) e;
            assertThat(m).containsKeys("key", "value");
        });
    }

    // ---------- named non-logical schema hits the switch default ----------

    @Test
    void namedNonLogicalStructSchemaStillFlattens() {
        SolrRecordConverter c = new SolrRecordConverter(config());
        // A STRUCT field whose schema carries a NAME that is not a Connect logical type —
        // exercises the "named but not logical" default branch in encoderFor.
        Schema named = SchemaBuilder.struct().name("com.acme.Money")
                .field("amount", Schema.INT64_SCHEMA).field("currency", Schema.STRING_SCHEMA).build();
        Schema schema = SchemaBuilder.struct().field("price", named).build();
        Struct v = new Struct(schema).put("price", new Struct(named).put("amount", 500L).put("currency", "USD"));

        SolrInputDocument doc = c.convert(rec(schema, v, "u1"));
        assertThat(doc.getFieldValue("price.amount")).isEqualTo(500L);
        assertThat(doc.getFieldValue("price.currency")).isEqualTo("USD");
    }

    // ---------- id strategies ----------

    @Test
    void keyIgnoredWithKafkaKeyStrategyFallsBackToTopicPartitionOffset() {
        SolrRecordConverter c = new SolrRecordConverter(config()); // id.strategy defaults KAFKA_KEY
        Schema schema = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        Struct v = new Struct(schema).put("x", "y");
        // keyIgnored=true forces TOPIC_PARTITION_OFFSET even though a key is present.
        SolrInputDocument doc = c.convert(rec(schema, v, "real-key"), true);
        assertThat(doc.getFieldValue("id")).isEqualTo("users-3-99");
    }

    @Test
    void recordFieldStrategyExtractsNestedIdPath() {
        SolrRecordConverter c = new SolrRecordConverter(config(
                SolrSinkConfig.ID_STRATEGY_CONFIG, "RECORD_FIELD",
                SolrSinkConfig.ID_FIELD_CONFIG, "account.id"));
        Schema account = SchemaBuilder.struct().field("id", Schema.INT64_SCHEMA).build();
        Schema schema = SchemaBuilder.struct().field("account", account).field("x", Schema.STRING_SCHEMA).build();
        Struct v = new Struct(schema).put("account", new Struct(account).put("id", 8_675_309L)).put("x", "z");

        SolrInputDocument doc = c.convert(rec(schema, v, null));
        assertThat(doc.getFieldValue("id")).isEqualTo("8675309"); // coerced to string by default
    }

    @Test
    void idCoerceToStringFalseKeepsNumericKafkaKey() {
        SolrRecordConverter c = new SolrRecordConverter(config(SolrSinkConfig.ID_COERCE_TO_STRING_CONFIG, "false"));
        Schema schema = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        SinkRecord r = new SinkRecord("users", 0, Schema.INT64_SCHEMA, 777L, schema,
                new Struct(schema).put("x", "y"), 1L);
        SolrInputDocument doc = c.convert(r);
        assertThat(doc.getFieldValue("id")).isEqualTo(777L); // primitive Long preserved
    }

    // ---------- atomic update rewrites every field except id ----------

    @Test
    void atomicUpdateWrapsEveryNonIdFieldWithSetModifier() {
        SolrRecordConverter c = new SolrRecordConverter(config(SolrSinkConfig.WRITE_METHOD_CONFIG, "ATOMIC_UPDATE"));
        Schema schema = SchemaBuilder.struct()
                .field("first_name", Schema.STRING_SCHEMA).field("age", Schema.INT32_SCHEMA).build();
        Struct v = new Struct(schema).put("first_name", "Ada").put("age", 36);

        SolrInputDocument doc = c.convert(rec(schema, v, "u1"));
        assertThat(doc.getFieldValue("id")).isEqualTo("u1"); // id NOT wrapped
        assertThat(doc.getFieldValue("first_name")).isEqualTo(Map.of("set", "Ada"));
        assertThat(doc.getFieldValue("age")).isEqualTo(Map.of("set", 36));
    }

    // ---------- mapping.version stamped as a field ----------

    @Test
    void mappingVersionStampedWhenConfigured() {
        SolrRecordConverter c = new SolrRecordConverter(config(SolrSinkConfig.MAPPING_VERSION_CONFIG, "v7"));
        Schema schema = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        SolrInputDocument doc = c.convert(rec(schema, new Struct(schema).put("x", "y"), "u1"));
        assertThat(doc.getFieldValue("_mapping_version")).isEqualTo("v7");
    }

    // ---------- challenging payloads: unicode, emoji, very deep, very wide ----------

    @Test
    void unicodeEmojiAndLargePayloadsSurviveIntact() {
        SolrRecordConverter c = new SolrRecordConverter(config());
        String emoji = "Ada 💙 λόγος 日本語 🧮";
        StringBuilder big = new StringBuilder();
        for (int i = 0; i < 20_000; i++) big.append('x');
        Schema schema = SchemaBuilder.struct()
                .field("bio", Schema.STRING_SCHEMA)
                .field("huge", Schema.STRING_SCHEMA)
                .field("tags", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
                .build();
        List<String> wide = new ArrayList<>();
        for (int i = 0; i < 1000; i++) wide.add("tag-" + i);
        Struct v = new Struct(schema).put("bio", emoji).put("huge", big.toString()).put("tags", wide);

        SolrInputDocument doc = c.convert(rec(schema, v, "u1"));
        assertThat(doc.getFieldValue("bio")).isEqualTo(emoji);
        assertThat(((String) doc.getFieldValue("huge")).length()).isEqualTo(20_000);
        assertThat(doc.getFieldValues("tags")).hasSize(1000);
    }

    // ---------- addScalar directly with logical schemas (typed schemaless entry) ----------

    @Test
    void addScalarWithLogicalSchemasFormatsCorrectly() {
        SolrRecordConverter c = new SolrRecordConverter(config());
        SolrInputDocument doc = new SolrInputDocument();
        c.addScalar(doc, "ts", java.util.Date.from(Instant.parse("2026-07-06T00:00:00Z")), Timestamp.SCHEMA);
        c.addScalar(doc, "d", java.util.Date.from(Instant.parse("2000-01-02T00:00:00Z")), Date.SCHEMA);
        c.addScalar(doc, "t", new java.util.Date(7_200_000L), Time.SCHEMA);
        c.addScalar(doc, "money", new BigDecimal("3.14"), Decimal.schema(2));
        c.addScalar(doc, "raw", ByteBuffer.wrap(new byte[]{1, 2}), Schema.BYTES_SCHEMA);
        c.addScalar(doc, "skip", null, Schema.STRING_SCHEMA); // null value ignored

        assertThat(doc.getFieldValue("ts")).isEqualTo("2026-07-06T00:00:00Z");
        assertThat(doc.getFieldValue("t")).isEqualTo("1970-01-01T02:00:00Z");
        assertThat(doc.getFieldValue("money")).isEqualTo("3.14");
        assertThat(doc.getFieldValue("raw")).isEqualTo(Base64.getEncoder().encodeToString(new byte[]{1, 2}));
        assertThat(doc.getField("skip")).isNull();
    }

    // ---------- bulk byte tracking with a real record ----------

    @Test
    void bulkSizeTrackingAccumulatesByteEstimate() {
        SolrRecordConverter c = new SolrRecordConverter(config(SolrSinkConfig.BULK_SIZE_BYTES_CONFIG, "1024"));
        Schema schema = SchemaBuilder.struct()
                .field("name", Schema.STRING_SCHEMA).field("age", Schema.INT32_SCHEMA)
                .field("avatar", Schema.BYTES_SCHEMA).build();
        Struct v = new Struct(schema).put("name", "Ada").put("age", 36).put("avatar", new byte[]{1, 2, 3});

        c.convert(rec(schema, v, "u1"));
        // id + name(string) + age(number) + avatar(base64 string) all counted -> > 0.
        assertThat(c.lastConversionByteEstimate()).isGreaterThan(0L);

        // A null value through addField with tracking on still counts the name bytes then returns.
        SolrInputDocument doc = new SolrInputDocument();
        c.addField(doc, "explicitNull", null);
        assertThat(doc.getField("explicitNull").getValue()).isNull();
    }

    @Test
    void trackingDisabledReturnsNegativeOneEstimate() {
        SolrRecordConverter c = new SolrRecordConverter(config(SolrSinkConfig.BULK_SIZE_BYTES_CONFIG, "0"));
        Schema schema = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        c.convert(rec(schema, new Struct(schema).put("x", "y"), "u1"));
        assertThat(c.lastConversionByteEstimate()).isEqualTo(-1L);
    }

    // ---------- remaining id strategies + branch completions ----------

    @Test
    void uuidStrategyProducesRandomUuidId() {
        SolrRecordConverter c = new SolrRecordConverter(config(SolrSinkConfig.ID_STRATEGY_CONFIG, "UUID"));
        Schema schema = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        SolrInputDocument doc = c.convert(rec(schema, new Struct(schema).put("x", "y"), null));
        String id = (String) doc.getFieldValue("id");
        assertThat(java.util.UUID.fromString(id)).isNotNull(); // parses as a real UUID
    }

    @Test
    void keyIgnoredWithNonKafkaKeyStrategyIsUnaffected() {
        // keyIgnored only rewrites KAFKA_KEY; RECORD_FIELD must still extract from the value.
        SolrRecordConverter c = new SolrRecordConverter(config(
                SolrSinkConfig.ID_STRATEGY_CONFIG, "RECORD_FIELD", SolrSinkConfig.ID_FIELD_CONFIG, "uid"));
        Schema schema = SchemaBuilder.struct().field("uid", Schema.STRING_SCHEMA).build();
        SolrInputDocument doc = c.convert(rec(schema, new Struct(schema).put("uid", "abc"), "ignored-key"), true);
        assertThat(doc.getFieldValue("id")).isEqualTo("abc");
    }

    @Test
    void recordFieldStrategyThrowsWhenValueIsNull() {
        SolrRecordConverter c = new SolrRecordConverter(config(SolrSinkConfig.ID_STRATEGY_CONFIG, "RECORD_FIELD"));
        SinkRecord nullValue = new SinkRecord("users", 0, Schema.STRING_SCHEMA, "k", null, null, 1L);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> c.deriveId(nullValue))
                .isInstanceOf(org.apache.kafka.connect.errors.DataException.class);
    }

    @Test
    void addScalarWithNonLogicalNamedSchemaFallsThroughToValue() {
        SolrRecordConverter c = new SolrRecordConverter(config());
        Schema email = SchemaBuilder.string().name("com.acme.Email").build();
        SolrInputDocument doc = new SolrInputDocument();
        c.addScalar(doc, "email", "ada@x.io", email); // named but not a logical type
        assertThat(doc.getFieldValue("email")).isEqualTo("ada@x.io");
    }

    @Test
    void arrayOfNonLogicalNamedElementSchema() {
        SolrRecordConverter c = new SolrRecordConverter(config());
        Schema email = SchemaBuilder.string().name("com.acme.Email").build();
        Schema schema = SchemaBuilder.struct().field("emails", SchemaBuilder.array(email).build()).build();
        Struct v = new Struct(schema).put("emails", List.of("a@x.io", "b@x.io"));
        SolrInputDocument doc = c.convert(rec(schema, v, "u1"));
        assertThat(doc.getFieldValues("emails")).containsExactly("a@x.io", "b@x.io");
    }

    // ---------- direct null-guard on populateFromStruct ----------

    @Test
    void populateFromStructIsNoOpForNullStructOrSchema() {
        SolrRecordConverter c = new SolrRecordConverter(config());
        SolrInputDocument doc = new SolrInputDocument();
        c.populateFromStruct(doc, null, null, "");
        Schema schema = SchemaBuilder.struct().field("x", Schema.STRING_SCHEMA).build();
        c.populateFromStruct(doc, null, schema, "");         // null struct
        c.populateFromStruct(doc, new Struct(schema), null, ""); // null schema
        assertThat(doc.getFieldNames()).isEmpty();
    }
}
