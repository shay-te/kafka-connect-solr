package com.una.kafka.connect.solr;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code partition.fanout.enabled=true} against a real in-process Solr: records from several
 * topics and partitions flow through per-partition processors, and the writer's metrics must
 * aggregate across them (sums, max, batch-weighted averages that skip idle partitions).
 */
class SolrWriterFanoutEmbeddedTest {

    private static final String CORE = EmbeddedSolrSupport.CORE;

    private static final Schema ADDRESS = SchemaBuilder.struct().name("address").optional()
            .field("city", Schema.STRING_SCHEMA)
            .field("zip", Schema.OPTIONAL_STRING_SCHEMA)
            .build();
    private static final Schema USER = SchemaBuilder.struct().name("user")
            .field("id", Schema.STRING_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .field("age", Schema.OPTIONAL_INT32_SCHEMA)
            .field("address", ADDRESS)
            .field("tags", SchemaBuilder.array(Schema.STRING_SCHEMA).optional().build())
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

    private SolrSinkConfig config() {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://embedded/solr");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, CORE);
        p.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "UNUSED");
        p.put(SolrSinkConfig.PARTITION_FANOUT_ENABLED_CONFIG, "true");
        p.put(SolrSinkConfig.BATCH_SIZE_CONFIG, "100");
        p.put(SolrSinkConfig.LINGER_MS_CONFIG, "600000");
        return new SolrSinkConfig(p);
    }

    private static SinkRecord user(String topic, int partition, String id, long offset) {
        Struct value = new Struct(USER)
                .put("id", id)
                .put("name", "User " + id)
                .put("age", 30 + (int) offset)
                .put("address", new Struct(ADDRESS).put("city", "Eilat").put("zip", "88000"))
                .put("tags", Arrays.asList("fanout", topic));
        return new SinkRecord(topic, partition, Schema.STRING_SCHEMA, id, USER, value, offset);
    }

    private long found(String q) throws Exception {
        solr.commit(CORE);
        return solr.query(CORE, new SolrQuery(q)).getResults().getNumFound();
    }

    @Test
    void recordsAcrossTopicsAndPartitionsLandAndMetricsAggregate() throws Exception {
        SolrWriter writer = new SolrWriter(new EmbeddedSolrServer(solr.getCoreContainer(), CORE), config());
        try {
            writer.write(user("users", 0, "a1", 1));
            writer.write(user("users", 0, "a2", 2));      // same partition: cached processor
            writer.write(user("users", 1, "b1", 1));
            writer.write(user("users_cdc", 1, "c1", 1));  // same partition number, other topic
            writer.write(user("users", 0, "a3", 3));      // back to a known partition
            assertThat(writer.hasBuffered()).isTrue();

            writer.flush();
            assertThat(writer.hasBuffered()).as("every processor drained").isFalse();
            assertThat(found("tags:fanout")).isEqualTo(5);
            assertThat(writer.recordsWritten()).isEqualTo(5);
            assertThat(writer.batchCount()).isEqualTo(3);  // one batch per partition processor
            assertThat(writer.solrCallCount()).isEqualTo(3);
            long lastSuccess = writer.lastSuccessEpochMs();
            assertThat(lastSuccess).isPositive();

            // A fresh partition with a buffered record has no batches yet: it must not drag the
            // weighted averages to zero, nor the max success time.
            writer.write(user("users", 2, "d1", 1));
            assertThat(writer.avgBatchLatencyMs()).isPositive();
            assertThat(writer.avgSolrCallLatencyMs()).isPositive();
            assertThat(writer.lastSuccessEpochMs()).isEqualTo(lastSuccess);
            assertThat(writer.queueDepth()).isEqualTo(1);
            assertThat(writer.recordsFailed()).isZero();
            assertThat(writer.retries()).isZero();

            // Revoking an unassigned partition, then one that is not the cached last processor.
            writer.partitionRevoked(new TopicPartition("users", 9));
            writer.partitionRevoked(new TopicPartition("users", 0));
            writer.write(user("users", 2, "d2", 2));      // last processor survived the revoke
            writer.partitionRevoked(new TopicPartition("users", 2)); // closes it: final flush
            assertThat(found("id:d*")).isEqualTo(2);
        } finally {
            writer.close();
        }
    }
}
