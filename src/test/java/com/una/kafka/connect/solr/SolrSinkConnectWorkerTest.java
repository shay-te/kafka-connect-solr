package com.una.kafka.connect.solr;

import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.connector.policy.AllConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.connector.policy.ConnectorClientConfigOverridePolicy;
import org.apache.kafka.connect.runtime.Herder;
import org.apache.kafka.connect.runtime.Worker;
import org.apache.kafka.connect.runtime.isolation.Plugins;
import org.apache.kafka.connect.runtime.rest.entities.ConnectorInfo;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.runtime.standalone.StandaloneHerder;
import org.apache.kafka.connect.storage.FileOffsetBackingStore;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.FutureCallback;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.util.NamedList;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The sink inside a real Connect worker and Kafka broker: the framework's offset commits, rewind and
 * redelivery and its dead-letter queue drive the connector as in production. Only Solr is in-process; the
 * worker runs without its REST server, whose Jetty 9.4 would clash with SolrJ's Jetty 10.
 */
class SolrSinkConnectWorkerTest {

    private static final Duration WAIT = Duration.ofSeconds(60);

    private static Path home;
    private static ServerCnxnFactory zookeeper;
    private static KafkaServer broker;
    private static String bootstrapServers;
    private static EmbeddedSolrServer embedded;
    private static StandaloneHerder herder;

    /** The connector under test, its tasks writing to this test's Solr instead of over HTTP. */
    public static class WorkerSolrSinkConnector extends SolrSinkConnector {
        @Override
        public Class<? extends Task> taskClass() {
            return WorkerSolrSinkTask.class;
        }
    }

    public static class WorkerSolrSinkTask extends SolrSinkTask {
        @Override
        protected SolrClient createClient(SolrSinkConfig config) {
            return SOLR;
        }
    }

    /** The in-process Solr, which an outage switches off: every request then fails as a refused connection. */
    static final class SwitchableSolr extends SolrClient {
        volatile boolean down;

        @Override
        public NamedList<Object> request(SolrRequest<?> request, String collection) throws SolrServerException, IOException {
            if (down) {
                throw new SolrServerException("Solr is down");
            }
            return embedded.request(request, collection);
        }

        @Override
        public void close() {
            // The test owns the embedded server.
        }
    }

    static final SwitchableSolr SOLR = new SwitchableSolr();

    @BeforeAll
    static void startKafkaConnectAndSolr() throws Exception {
        home = Files.createTempDirectory("solr-sink-connect-worker");
        ZooKeeperServer zooKeeperServer = new ZooKeeperServer(home.resolve("zk").toFile(), home.resolve("zk").toFile(), 500);
        zookeeper = ServerCnxnFactory.createFactory(new InetSocketAddress("127.0.0.1", 0), 60);
        zookeeper.startup(zooKeeperServer);

        Properties brokerProps = new Properties();
        brokerProps.put("broker.id", "0");
        brokerProps.put("zookeeper.connect", "127.0.0.1:" + zookeeper.getLocalPort());
        brokerProps.put("listeners", "PLAINTEXT://127.0.0.1:0");
        brokerProps.put("log.dirs", home.resolve("kafka").toString());
        brokerProps.put("offsets.topic.replication.factor", "1");
        brokerProps.put("offsets.topic.num.partitions", "1");
        brokerProps.put("transaction.state.log.replication.factor", "1");
        brokerProps.put("transaction.state.log.min.isr", "1");
        brokerProps.put("group.initial.rebalance.delay.ms", "0");
        broker = new KafkaServer(KafkaConfig.fromProps(brokerProps), Time.SYSTEM, scala.Option.apply(null), false);
        broker.startup();
        bootstrapServers = "127.0.0.1:" + broker.boundPort(ListenerName.normalised("PLAINTEXT"));

        embedded = EmbeddedSolrSupport.start();

        Map<String, String> workerProps = new HashMap<>();
        workerProps.put("bootstrap.servers", bootstrapServers);
        workerProps.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        workerProps.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        workerProps.put("value.converter.schemas.enable", "false");
        workerProps.put("offset.storage.file.filename", home.resolve("connect.offsets").toString());
        workerProps.put("offset.flush.interval.ms", "500");
        StandaloneConfig config = new StandaloneConfig(workerProps);
        ConnectorClientConfigOverridePolicy policy = new AllConnectorClientConfigOverridePolicy();
        Plugins plugins = new Plugins(workerProps);
        // What the standalone launcher does: a sink never stores source offsets, but the worker needs a store.
        FileOffsetBackingStore offsets = new FileOffsetBackingStore(plugins.newInternalConverter(true,
                "org.apache.kafka.connect.json.JsonConverter", Map.of("schemas.enable", "false")));
        offsets.configure(config);
        Worker worker = new Worker("solr-sink-test-worker", Time.SYSTEM, plugins, config, offsets, policy);
        herder = new StandaloneHerder(worker, config.kafkaClusterId(), policy);
        herder.start();
    }

    @AfterAll
    static void stopAll() {
        if (herder != null) herder.stop();
        if (embedded != null) EmbeddedSolrSupport.stop(embedded);
        if (broker != null) {
            broker.shutdown();
            broker.awaitShutdown();
        }
        if (zookeeper != null) zookeeper.shutdown();
    }

    private static void startSink(String name, String topic, String dlq) throws Exception {
        startSink(name, topic, dlq, Map.of());
    }

    private static void startSink(String name, String topic, String dlq, Map<String, String> extra) throws Exception {
        Map<String, String> p = new HashMap<>();
        p.put("name", name);
        p.put("connector.class", WorkerSolrSinkConnector.class.getName());
        p.put("tasks.max", "1");
        p.put("topics", topic);
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://embedded/solr");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, EmbeddedSolrSupport.CORE);
        p.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "UNUSED");
        p.put(SolrSinkConfig.SCHEMA_IGNORE_CONFIG, "true");
        p.put(SolrSinkConfig.BEHAVIOR_ON_NULL_VALUES_CONFIG, "delete");
        p.put(SolrSinkConfig.BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, "warn");
        p.put(SolrSinkConfig.MAX_RETRIES_CONFIG, "1");
        p.put(SolrSinkConfig.RETRY_BACKOFF_MS_CONFIG, "50");
        p.put(SolrSinkConfig.LINGER_MS_CONFIG, "50");
        p.put("errors.tolerance", "all");
        p.put("errors.deadletterqueue.topic.name", dlq);
        p.put("errors.deadletterqueue.topic.replication.factor", "1");
        p.putAll(extra);
        FutureCallback<Herder.Created<ConnectorInfo>> created = new FutureCallback<>();
        herder.putConnectorConfig(name, p, true, created);
        created.get(30, TimeUnit.SECONDS);
    }

    private static void stopSink(String name) throws Exception {
        FutureCallback<Herder.Created<ConnectorInfo>> deleted = new FutureCallback<>();
        herder.deleteConnectorConfig(name, deleted);
        deleted.get(30, TimeUnit.SECONDS);
    }

    private static void produce(String topic, String key, String json) throws Exception {
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrapServers);
        try (KafkaProducer<String, String> producer =
                     new KafkaProducer<>(p, new StringSerializer(), new StringSerializer())) {
            producer.send(new ProducerRecord<>(topic, key, json)).get(30, TimeUnit.SECONDS);
        }
    }

    private static long docs(String idPrefix) throws Exception {
        embedded.commit(EmbeddedSolrSupport.CORE);
        return embedded.query(EmbeddedSolrSupport.CORE, new SolrQuery("id:" + idPrefix + "*")).getResults().getNumFound();
    }

    private static List<String> deadLetteredKeys(String dlq) {
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrapServers);
        p.put("group.id", "dlq-reader-" + dlq);
        p.put("auto.offset.reset", "earliest");
        List<String> keys = new ArrayList<>();
        try (KafkaConsumer<byte[], byte[]> consumer =
                     new KafkaConsumer<>(p, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            consumer.subscribe(List.of(dlq));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (System.nanoTime() < deadline) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
                records.forEach(r -> keys.add(new String(r.key(), java.nio.charset.StandardCharsets.UTF_8)));
            }
        }
        return keys;
    }

    private static void await(String what, Callable<Boolean> condition) throws Exception {
        long deadline = System.nanoTime() + WAIT.toNanos();
        while (!condition.call()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("timed out waiting for " + what);
            }
            Thread.sleep(200);
        }
    }

    private static String taskState(String name) {
        return herder.taskStatus(new ConnectorTaskId(name, 0)).state();
    }

    @Test
    void recordsAndTombstonesReachSolrThroughAWorker() throws Exception {
        startSink("happy", "happy-users", "happy-dlq");
        try {
            for (String id : List.of("happy-1", "happy-2", "happy-3")) {
                produce("happy-users", id, "{\"name\":\"user " + id + "\"}");
            }
            await("3 documents", () -> docs("happy-") == 3);

            produce("happy-users", "happy-2", null);
            await("the tombstone to delete happy-2", () -> docs("happy-") == 2 && docs("happy-2") == 0);
        } finally {
            stopSink("happy");
        }
    }

    @Test
    void aSolrOutageIsRetriedUntilSolrIsBackAndNothingIsLost() throws Exception {
        startSink("outage", "outage-users", "outage-dlq");
        try {
            produce("outage-users", "outage-1", "{\"name\":\"before\"}");
            await("the first document", () -> docs("outage-") == 1);

            SOLR.down = true;
            produce("outage-users", "outage-2", "{\"name\":\"during\"}");
            produce("outage-users", "outage-3", "{\"name\":\"during\"}");
            Thread.sleep(3_000);
            assertThat(docs("outage-")).as("nothing reaches a Solr that is down").isEqualTo(1);
            assertThat(taskState("outage")).as("an outage is retried, not fatal").isEqualTo("RUNNING");

            SOLR.down = false;
            await("the records sent during the outage", () -> docs("outage-") == 3);
            assertThat(deadLetteredKeys("outage-dlq")).as("an outage is not a bad document").isEmpty();
        } finally {
            SOLR.down = false;
            stopSink("outage");
        }
    }

    @Test
    void aSolrThatStaysDownPastTheRetryTimeoutFailsTheTask() throws Exception {
        // RUNNING must mean progress: the stall surfaces as FAILED, which the health check alerts on.
        startSink("stall", "stall-users", "stall-dlq", Map.of(SolrSinkConfig.RETRY_TIMEOUT_MS_CONFIG, "1000"));
        try {
            produce("stall-users", "stall-1", "{\"name\":\"before\"}");
            await("the first document", () -> docs("stall-") == 1);

            SOLR.down = true;
            produce("stall-users", "stall-2", "{\"name\":\"during\"}");
            await("the stalled task to fail", () -> "FAILED".equals(taskState("stall")));
            assertThat(docs("stall-")).isEqualTo(1);
        } finally {
            SOLR.down = false;
            stopSink("stall");
        }
    }

    @Test
    void aDocumentSolrRejectsGoesToTheDeadLetterQueueAndTheRestIndex() throws Exception {
        startSink("dlq", "dlq-users", "dlq-dlq");
        try {
            produce("dlq-users", "dlq-1", "{\"name\":\"good\"}");
            // height_cm is a plong in the test schema: Solr refuses this document (HTTP 400).
            produce("dlq-users", "dlq-2", "{\"name\":\"bad\",\"height_cm\":\"tall\"}");
            produce("dlq-users", "dlq-3", "{\"name\":\"good\"}");

            await("the two good documents", () -> docs("dlq-") == 2);
            assertThat(docs("dlq-2")).isZero();
            assertThat(deadLetteredKeys("dlq-dlq")).containsExactly("dlq-2");
            assertThat(taskState("dlq")).isEqualTo("RUNNING");
        } finally {
            stopSink("dlq");
        }
    }
}
