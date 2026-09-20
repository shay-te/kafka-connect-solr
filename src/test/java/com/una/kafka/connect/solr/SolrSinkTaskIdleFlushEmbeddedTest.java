package com.una.kafka.connect.solr;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link SolrSinkTask} against a real in-process Solr: the idle {@code put()} that enforces
 * linger.ms when the flush it triggers fails, and async offset tracking for two topics that share
 * a partition number. The only forced edge is an interrupt on the task thread — what Connect does
 * to a task that overruns its shutdown.
 */
class SolrSinkTaskIdleFlushEmbeddedTest {

    private static final long LINGER_MS = 300L;
    private static final Schema CONTACT = SchemaBuilder.struct().name("contact")
            .field("id", Schema.STRING_SCHEMA)
            .field("email", Schema.STRING_SCHEMA)
            .field("lists", SchemaBuilder.array(Schema.STRING_SCHEMA).build())
            .build();

    private EmbeddedSolrServer solr;

    @BeforeEach
    void setUp() throws Exception {
        solr = EmbeddedSolrSupport.start();
    }

    @AfterEach
    void tearDown() {
        Thread.interrupted();
        EmbeddedSolrSupport.stop(solr);
    }

    private final class EmbeddedTask extends SolrSinkTask {
        @Override
        protected SolrClient createClient(SolrSinkConfig config) {
            return new EmbeddedSolrServer(solr.getCoreContainer(), EmbeddedSolrSupport.CORE);
        }
    }

    private static Map<String, String> props(String... kv) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://embedded/solr");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, EmbeddedSolrSupport.CORE);
        p.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "UNUSED");
        p.put(SolrSinkConfig.BATCH_SIZE_CONFIG, "100");
        p.put(SolrSinkConfig.LINGER_MS_CONFIG, Long.toString(LINGER_MS));
        for (int i = 0; i < kv.length; i += 2) p.put(kv[i], kv[i + 1]);
        return p;
    }

    private static SinkRecord contact(String topic, String id, long offset) {
        Struct v = new Struct(CONTACT).put("id", id).put("email", id + "@example.com")
                .put("lists", Arrays.asList("newsletter", topic));
        return new SinkRecord(topic, 0, Schema.STRING_SCHEMA, id, CONTACT, v, offset);
    }

    private long found(String q) throws Exception {
        solr.commit(EmbeddedSolrSupport.CORE);
        return solr.query(EmbeddedSolrSupport.CORE, new SolrQuery(q)).getResults().getNumFound();
    }

    /** Buffers a record whatever the linger clock says: an interrupted flush puts records back. */
    private static void bufferWhileInterrupted(SolrSinkTask task, SinkRecord r) {
        Thread.currentThread().interrupt();
        try {
            task.put(Collections.singletonList(r));
        } catch (RetriableException lingerFlushRefused) {
            // linger had already elapsed: the flush was refused and the record kept
        }
    }

    private static void waitPastLinger() throws InterruptedException {
        Thread.interrupted();
        Thread.sleep(LINGER_MS + 100);
    }

    @Test
    void interruptedIdleFlushIsRetriableAndTheRecordIsWrittenOnTheNextIdlePut() throws Exception {
        EmbeddedTask task = new EmbeddedTask();
        task.start(props());
        try {
            bufferWhileInterrupted(task, contact("contacts", "ct-1", 1));
            waitPastLinger();

            Thread.currentThread().interrupt();
            assertThatThrownBy(() -> task.put(Collections.emptyList()))
                    .isInstanceOf(RetriableException.class)
                    .hasCauseInstanceOf(InterruptedException.class);
            assertThat(Thread.interrupted()).isTrue();
            assertThat(found("id:ct-1")).isZero();

            task.put(Collections.emptyList()); // Connect's next wake-up: linger still elapsed
            long hits = 0;
            for (int i = 0; i < 100 && hits == 0; i++) {
                hits = found("id:ct-1");
                if (hits == 0) Thread.sleep(50);
            }
            assertThat(hits).isEqualTo(1);
        } finally {
            task.stop();
        }
    }

    @Test
    void idleFlushAfterAnInterruptedStopIsWrappedAsRetriable() throws Exception {
        EmbeddedTask task = new EmbeddedTask();
        task.start(props(
                SolrSinkConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG, "2",
                SolrSinkConfig.ORDERING_LANES_ENABLED_CONFIG, "true"));
        bufferWhileInterrupted(task, contact("contacts", "ct-2", 1));
        // Still interrupted: the final flush is refused, the record stays buffered, lanes stop.
        task.stop();
        waitPastLinger();

        assertThatThrownBy(() -> task.put(Collections.emptyList()))
                .isInstanceOf(RetriableException.class)
                .hasMessage("Idle linger flush failed")
                .hasCauseInstanceOf(RejectedExecutionException.class);
        assertThat(found("id:ct-2")).as("never sent, so its offset was never committed").isZero();
    }

    @Test
    void asyncOffsetsForTwoTopicsOnTheSamePartitionNumberAdvanceIndependently() throws Exception {
        EmbeddedTask task = new EmbeddedTask();
        task.start(props(SolrSinkConfig.FLUSH_SYNCHRONOUSLY_CONFIG, "false"));
        try {
            task.put(Arrays.asList(
                    contact("contacts", "a-10", 10),
                    contact("contacts_cdc", "b-20", 20),
                    contact("contacts", "a-11", 11)));
            TopicPartition contacts = new TopicPartition("contacts", 0);
            TopicPartition cdc = new TopicPartition("contacts_cdc", 0);
            Map<TopicPartition, OffsetAndMetadata> current = new HashMap<>();
            current.put(contacts, new OffsetAndMetadata(12));
            current.put(cdc, new OffsetAndMetadata(21));

            task.flush(current); // wait for the acks so preCommit is deterministic
            Map<TopicPartition, OffsetAndMetadata> safe = task.preCommit(current);
            assertThat(safe.get(contacts).offset()).isEqualTo(12);
            assertThat(safe.get(cdc).offset()).isEqualTo(21);

            // Revoke the topic that is not the tracker's cached last partition.
            task.close(Collections.singletonList(cdc));
            task.put(Collections.singletonList(contact("contacts", "a-12", 12)));
            task.flush(current);
            current.put(contacts, new OffsetAndMetadata(13));
            assertThat(task.preCommit(current).get(contacts).offset()).isEqualTo(13);
            assertThat(found("lists:newsletter")).isEqualTo(4);
        } finally {
            task.stop();
        }
    }
}
