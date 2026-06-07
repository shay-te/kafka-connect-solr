package com.una.kafka.connect.solr.perf;

import com.una.kafka.connect.solr.SolrSinkConfig;
import com.una.kafka.connect.solr.SolrSinkTask;
import org.apache.http.HttpHost;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.xcontent.XContentType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Head-to-head benchmark. Same N records, same batch / concurrency settings,
 * Solr via our {@link SolrSinkTask} → real Solr container, Elasticsearch via
 * the same Java REST high-level client the Confluent ES sink wraps →
 * real ES container.
 *
 * The benchmark asserts Solr finishes first; either way it prints the
 * absolute wall-clock for both sides so you can see how they stack up
 * locally.
 *
 * Requires Docker. Opt in with: {@code mvn test -Pperf}
 */
@Tag("performance")
class SolrVsElasticsearchPerfTest {

    private static final int RECORDS = 30_000;
    private static final int BATCH_SIZE = 1_000;
    private static final int IN_FLIGHT = 8;

    private static GenericContainer<?> solrContainer;
    private static ElasticsearchContainer esContainer;

    private static String solrBaseUrl;
    private static String esHost;
    private static int esPort;

    @BeforeAll
    static void startContainers() {
        solrContainer = new GenericContainer<>(DockerImageName.parse("solr:9.4"))
                .withExposedPorts(8983)
                .withCommand("solr-precreate", "perf");
        solrContainer.start();
        solrBaseUrl = "http://" + solrContainer.getHost() + ":" + solrContainer.getMappedPort(8983) + "/solr";

        esContainer = new ElasticsearchContainer(
                DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch:7.17.24"))
                .withEnv("xpack.security.enabled", "false")
                .withEnv("discovery.type", "single-node")
                .withEnv("ES_JAVA_OPTS", "-Xms1g -Xmx1g");
        esContainer.start();
        esHost = esContainer.getHost();
        esPort = esContainer.getFirstMappedPort();
    }

    @AfterAll
    static void stopContainers() {
        if (solrContainer != null) solrContainer.stop();
        if (esContainer != null) esContainer.stop();
    }

    @Test
    void solrIsFasterThanElasticsearch() throws Exception {
        // -- Build the same data set for both runs.
        Schema schema = SolrRecordConverterPerfTest.userLikeSchema();
        List<SinkRecord> records = new ArrayList<>(RECORDS);
        for (int i = 0; i < RECORDS; i++) {
            Struct s = sample(schema, i);
            records.add(new SinkRecord("perf", 0, Schema.STRING_SCHEMA, "u-" + i, schema, s, i));
        }

        // Build a small warmup batch so neither side eats JIT cold-start on
        // the timed run. Use a different topic + small N so it doesn't
        // overlap the real measurement.
        List<SinkRecord> warmup = new ArrayList<>(2_000);
        for (int i = 0; i < 2_000; i++) {
            warmup.add(new SinkRecord("warmup", 0, Schema.STRING_SCHEMA, "w-" + i,
                    schema, sample(schema, i), i));
        }
        timeSolr(warmup);
        timeElasticsearch(warmup);

        // -- Solr side: drive through SolrSinkTask (real connector code).
        PhaseResult solr = measure("solr", () -> timeSolr(records));

        // -- Elasticsearch side: drive through the high-level REST bulk API
        //    (the Confluent ES connector wraps the same client).
        PhaseResult es = measure("elasticsearch", () -> timeElasticsearch(records));

        double solrRps = RECORDS * 1000.0 / solr.wallMs;
        double esRps = RECORDS * 1000.0 / es.wallMs;
        System.out.printf("%n[HEAD-TO-HEAD] records=%d batch=%d inFlight=%d%n",
                RECORDS, BATCH_SIZE, IN_FLIGHT);
        System.out.printf("[HEAD-TO-HEAD] Solr:          %5d ms  (%.0f r/s)%n", solr.wallMs, solrRps);
        System.out.printf("[HEAD-TO-HEAD] Elasticsearch: %5d ms  (%.0f r/s)%n", es.wallMs, esRps);
        System.out.printf("[HEAD-TO-HEAD] Solr wins by %.1fx%n", es.wallMs / (double) solr.wallMs);
        System.out.println("[HEAD-TO-HEAD] per-phase metrics (cpu, alloc, gc):");
        System.out.println(solr.delta.toTable("solr", solr.wallMs));
        System.out.println(es.delta.toTable("elasticsearch", es.wallMs));
        long solrMs = solr.wallMs;
        long esMs = es.wallMs;

        // The whole point of using HTTP/2 + concurrent in-flight is to beat
        // a single connection bulk indexer. Allow a 10% margin so a slow
        // CI runner with cold JIT can't flake.
        assertThat(solrMs).isLessThanOrEqualTo((long) (esMs * 0.9));
    }

    /**
     * Wrap a timed phase with JFR recording and CPU/alloc/GC delta capture.
     * The phase returns its own wall-clock measurement so we stay consistent
     * with the existing timing (System.nanoTime around the work, not around
     * setup or teardown).
     */
    private static PhaseResult measure(String phase, TimedWork work) throws Exception {
        try (JfrRecorder ignored = JfrRecorder.start(phase)) {
            BenchmarkMetrics.Snapshot before = BenchmarkMetrics.capture();
            long wallMs = work.run();
            BenchmarkMetrics.Delta delta = BenchmarkMetrics.capture().minus(before);
            return new PhaseResult(wallMs, delta);
        }
    }

    @FunctionalInterface
    private interface TimedWork {
        long run() throws Exception;
    }

    private static final class PhaseResult {
        final long wallMs;
        final BenchmarkMetrics.Delta delta;
        PhaseResult(long wallMs, BenchmarkMetrics.Delta delta) {
            this.wallMs = wallMs;
            this.delta = delta;
        }
    }

    private long timeSolr(List<SinkRecord> records) {
        Map<String, String> props = new HashMap<>();
        props.put(SolrSinkConfig.SOLR_URL_CONFIG, solrBaseUrl);
        props.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "perf");
        props.put(SolrSinkConfig.BATCH_SIZE_CONFIG, String.valueOf(BATCH_SIZE));
        props.put(SolrSinkConfig.LINGER_MS_CONFIG, "10");
        props.put(SolrSinkConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG, String.valueOf(IN_FLIGHT));
        props.put(SolrSinkConfig.MAX_BUFFERED_RECORDS_CONFIG, "50000");
        props.put(SolrSinkConfig.COMMIT_WITHIN_MS_CONFIG, "5000");
        props.put(SolrSinkConfig.FLUSH_TIMEOUT_MS_CONFIG, "120000");

        SolrSinkTask task = new SolrSinkTask();
        task.start(props);

        long t0 = System.nanoTime();
        // Mimic Kafka Connect feeding records in chunks.
        int chunk = 500;
        for (int i = 0; i < records.size(); i += chunk) {
            task.put(records.subList(i, Math.min(records.size(), i + chunk)));
        }
        task.preCommit(new HashMap<>());
        long ns = System.nanoTime() - t0;
        task.stop();
        return TimeUnit.NANOSECONDS.toMillis(ns);
    }

    private long timeElasticsearch(List<SinkRecord> records) throws Exception {
        @SuppressWarnings("deprecation")
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(new HttpHost(esHost, esPort, "http")));
        try {
            // Pre-create index so first request isn't slowed by mapping inference.
            client.indices().create(
                    new org.elasticsearch.client.indices.CreateIndexRequest("perf"),
                    RequestOptions.DEFAULT);
        } catch (Exception ignored) {
            // Already exists - fine.
        }

        ExecutorService exec = Executors.newFixedThreadPool(IN_FLIGHT);
        AtomicInteger failures = new AtomicInteger();
        long t0 = System.nanoTime();
        List<java.util.concurrent.Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < records.size(); i += BATCH_SIZE) {
            final int from = i;
            final int to = Math.min(records.size(), i + BATCH_SIZE);
            futures.add(exec.submit(() -> {
                BulkRequest bulk = new BulkRequest();
                for (int j = from; j < to; j++) {
                    SinkRecord r = records.get(j);
                    IndexRequest req = new IndexRequest("perf")
                            .id((String) r.key())
                            .source(structToJson((Struct) r.value()), XContentType.JSON);
                    bulk.add(req);
                }
                try {
                    BulkResponse resp = client.bulk(bulk, RequestOptions.DEFAULT);
                    if (resp.hasFailures()) {
                        failures.incrementAndGet();
                    }
                } catch (Exception e) {
                    failures.incrementAndGet();
                }
            }));
        }
        for (java.util.concurrent.Future<?> f : futures) {
            f.get();
        }
        // Refresh so we have a fair view of "finished".
        client.indices().refresh(new org.elasticsearch.action.admin.indices.refresh.RefreshRequest("perf"),
                RequestOptions.DEFAULT);
        long ns = System.nanoTime() - t0;
        exec.shutdown();
        client.close();
        assertThat(failures.get()).isZero();
        return TimeUnit.NANOSECONDS.toMillis(ns);
    }

    private static Struct sample(Schema schema, int i) {
        return new Struct(schema)
                .put("id", (long) i)
                .put("first_name", "Ada" + (i % 100))
                .put("last_name", "Lovelace" + (i % 100))
                .put("email", "user" + i + "@example.com")
                .put("age", 20 + (i % 60))
                .put("height", 1.5 + (i % 50) / 100.0)
                .put("status", i % 4)
                .put("gender", i % 3)
                .put("location", (40.0 + i % 10 / 100.0) + "," + (-73.0 - i % 10 / 100.0))
                .put("language", i % 2 == 0 ? "en" : "es")
                .put("created_at", "2026-01-01")
                .put("updated_at", "2026-06-05")
                .put("organization_id", (long) (i % 50))
                .put("package_name", i % 3 == 0 ? "premium" : "basic")
                .put("completion", i % 101);
    }

    private static String structToJson(Struct s) {
        StringBuilder sb = new StringBuilder(256);
        sb.append('{');
        boolean first = true;
        for (org.apache.kafka.connect.data.Field f : s.schema().fields()) {
            if (!first) sb.append(',');
            first = false;
            sb.append('"').append(f.name()).append('"').append(':');
            Object v = s.get(f);
            if (v instanceof Number || v instanceof Boolean) {
                sb.append(v);
            } else {
                sb.append('"').append(String.valueOf(v).replace("\"", "\\\"")).append('"');
            }
        }
        sb.append('}');
        return sb.toString();
    }
}
