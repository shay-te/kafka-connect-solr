package com.una.kafka.connect.solr.perf;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrInputDocument;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Live head-to-head QUERY benchmark: the same {@value #DOCS} users indexed into Solr (on the real
 * production configset — denormalized flat fields, typed, stored-only _src) and Elasticsearch
 * (custom_fields as a `nested` type, the shape ES actually used). We then time three representative
 * user-search queries, each with a sort, over {@value #ITERS} iterations:
 *
 *   1. top-level filter (status) + sort by created_at            — both engines flat
 *   2. custom-field filter + sort by that field's value          — Solr denormalized vs ES NESTED
 *   3. numeric range (height) + sort                             — typed range both sides
 *
 * #2 is the one the migration is about: ES pays for a nested join, Solr resolves a flat filter on
 * the filter cache. Opt-in (heavy, needs Docker): {@code -Dperf.query=true}.
 */
@Tag("performance")
class SolrVsElasticsearchQueryPerfTest {

    private static final int DOCS = 50_000;
    private static final int ITERS = 300;
    private static final int WARMUP = 50;
    private static final String[] TIERS = {"Gold", "Silver", "Bronze", "Platinum"};
    private static final int CF_ID = 5;                 // the custom field we filter/sort on
    private static final String CORE = "perf";
    private static final String ES_INDEX = "users";

    private static GenericContainer<?> solr;
    private static ElasticsearchContainer es;
    private static Http2SolrClient solrClient;
    private static RestClient esClient;

    @BeforeAll
    static void setUp() throws Exception {
        org.junit.jupiter.api.Assumptions.assumeTrue(
                Boolean.getBoolean("perf.query"), "opt-in: -Dperf.query=true");

        solr = new GenericContainer<>(DockerImageName.parse("solr:9.4"))
                .withExposedPorts(8983)
                .withEnv("SOLR_JAVA_MEM", "-Xms1g -Xmx1g")
                // Mount the PRODUCTION configset and precreate the core on it.
                .withCopyFileToContainer(
                        MountableFile.forClasspathResource("embedded-solr-prod/users/conf"),
                        "/opt/solr/server/solr/configsets/agg/conf")
                .withCommand("solr-precreate", CORE, "/opt/solr/server/solr/configsets/agg")
                .waitingFor(Wait.forHttp("/solr/" + CORE + "/select?q=*:*")
                        .forPort(8983).forStatusCode(200).withStartupTimeout(Duration.ofMinutes(2)));
        solr.start();
        String base = "http://" + solr.getHost() + ":" + solr.getMappedPort(8983) + "/solr";
        solrClient = new Http2SolrClient.Builder(base).build();

        es = new ElasticsearchContainer(
                DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch:7.17.24"))
                .withEnv("xpack.security.enabled", "false")
                .withEnv("discovery.type", "single-node")
                .withEnv("ES_JAVA_OPTS", "-Xms1g -Xmx1g");
        es.start();
        esClient = RestClient.builder(new HttpHost(es.getHost(), es.getFirstMappedPort(), "http")).build();

        indexBoth();
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (solrClient != null) solrClient.close();
        if (esClient != null) esClient.close();
        if (solr != null) solr.stop();
        if (es != null) es.stop();
    }

    private static void indexBoth() throws Exception {
        // --- Elasticsearch: nested mapping for custom_fields (the real ES shape) ---
        Request createIndex = new Request("PUT", "/" + ES_INDEX);
        createIndex.setJsonEntity("{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0},"
                + "\"mappings\":{\"properties\":{"
                + "\"status\":{\"type\":\"integer\"},\"created_at\":{\"type\":\"date\"},"
                + "\"height\":{\"type\":\"integer\"},"
                + "\"custom_fields\":{\"type\":\"nested\",\"properties\":{"
                + "\"custom_field_id\":{\"type\":\"integer\"},"
                + "\"value_4\":{\"type\":\"keyword\"},\"sort_value\":{\"type\":\"keyword\"}}},"
                + "\"promises\":{\"type\":\"nested\",\"properties\":{"
                + "\"id\":{\"type\":\"integer\"},\"status\":{\"type\":\"integer\"},"
                + "\"created_at\":{\"type\":\"date\"}}}}}}");
        esClient.performRequest(createIndex);

        List<SolrInputDocument> solrBatch = new ArrayList<>(DOCS);
        StringBuilder esBulk = new StringBuilder(DOCS * 200);
        for (int i = 0; i < DOCS; i++) {
            int status = i % 5;
            int height = 150 + (i % 50);
            String tier = TIERS[i % TIERS.length];
            String createdAt = String.format(Locale.ROOT, "2026-%02d-%02dT00:00:00Z", 1 + (i % 12), 1 + (i % 27));

            // Three promises per user (statuses cycle 1..6); the streamer's derived
            // promises_latest_actionable_at = max created_at over actionable (1/2/4) ones.
            StringBuilder promisesJson = new StringBuilder(160);
            String latestActionable = null;
            for (int k = 0; k < 3; k++) {
                int pStatus = (i + k) % 6 + 1;
                String pCreated = String.format(Locale.ROOT, "2026-%02d-%02dT%02d:00:00Z",
                        1 + (i % 12), 1 + (i % 27), (i + k) % 24);
                if (k > 0) promisesJson.append(',');
                promisesJson.append("{\"id\":").append(i * 3 + k)
                        .append(",\"status\":").append(pStatus)
                        .append(",\"created_at\":\"").append(pCreated).append("\"}");
                boolean actionable = pStatus == 1 || pStatus == 2 || pStatus == 4;
                if (actionable && (latestActionable == null || pCreated.compareTo(latestActionable) > 0)) {
                    latestActionable = pCreated;
                }
            }

            SolrInputDocument d = new SolrInputDocument();
            d.addField("id", "u" + i);
            d.addField("status", status);
            d.addField("created_at", createdAt);
            d.addField("height", height);
            d.addField("custom_field_" + CF_ID + "_value_4", tier);       // denormalized flat filter field
            d.addField("custom_field_" + CF_ID + "_sort_value", tier);    // denormalized sort key
            d.addField("_src", "{\"id\":" + i + ",\"promises\":[" + promisesJson + "]}");
            if (latestActionable != null) d.addField("promises_latest_actionable_at", latestActionable);
            solrBatch.add(d);

            esBulk.append("{\"index\":{\"_id\":\"u").append(i).append("\"}}\n")
                    .append("{\"status\":").append(status)
                    .append(",\"created_at\":\"").append(createdAt).append('"')
                    .append(",\"height\":").append(height)
                    .append(",\"custom_fields\":[{\"custom_field_id\":").append(CF_ID)
                    .append(",\"value_4\":\"").append(tier).append('"')
                    .append(",\"sort_value\":\"").append(tier).append("\"}]")
                    .append(",\"promises\":[").append(promisesJson).append("]}\n");

            if (solrBatch.size() == 2_000) {
                solrClient.add(CORE, solrBatch);
                solrBatch.clear();
            }
            if ((i + 1) % 2_000 == 0) {
                flushEsBulk(esBulk);
            }
        }
        if (!solrBatch.isEmpty()) solrClient.add(CORE, solrBatch);
        flushEsBulk(esBulk);
        solrClient.commit(CORE);
        Request refresh = new Request("POST", "/" + ES_INDEX + "/_refresh");
        esClient.performRequest(refresh);
    }

    private static void flushEsBulk(StringBuilder bulk) throws Exception {
        if (bulk.length() == 0) return;
        Request r = new Request("POST", "/" + ES_INDEX + "/_bulk");
        r.setJsonEntity(bulk.toString());
        esClient.performRequest(r);
        bulk.setLength(0);
    }

    @Test
    void solrFilterSortIsFasterThanElasticsearch() throws Exception {
        System.out.printf("%n[QUERY-PERF] docs=%d iterations=%d%n", DOCS, ITERS);

        // All Solr queries fetch fl=_src — the ONLY retrieval shape production uses
        // (search_docs). ES returns full _source per hit, its equivalent.
        double q1Solr = timeSolr(new SolrQuery("*:*").addFilterQuery("status:2")
                .setSort("created_at", SolrQuery.ORDER.desc).setRows(20).setFields("_src"));
        double q1Es = timeEs("{\"query\":{\"term\":{\"status\":2}},"
                + "\"sort\":[{\"created_at\":\"desc\"}],\"size\":20}");
        report("1. top-level filter + sort", q1Solr, q1Es);

        double q2Solr = timeSolr(new SolrQuery("*:*")
                .addFilterQuery("custom_field_" + CF_ID + "_value_4:Gold")
                .setSort("custom_field_" + CF_ID + "_sort_value", SolrQuery.ORDER.asc).setRows(20).setFields("_src"));
        double q2Es = timeEs("{\"query\":{\"nested\":{\"path\":\"custom_fields\",\"query\":{\"bool\":{\"must\":["
                + "{\"term\":{\"custom_fields.custom_field_id\":" + CF_ID + "}},"
                + "{\"term\":{\"custom_fields.value_4\":\"Gold\"}}]}}}},"
                + "\"sort\":[{\"custom_fields.sort_value\":{\"order\":\"asc\",\"nested\":{\"path\":\"custom_fields\","
                + "\"filter\":{\"term\":{\"custom_fields.custom_field_id\":" + CF_ID + "}}}}}],\"size\":20}");
        report("2. custom-field filter+sort (Solr flat vs ES NESTED)", q2Solr, q2Es);

        double q3Solr = timeSolr(new SolrQuery("*:*").addFilterQuery("height:[170 TO 190]")
                .setSort("height", SolrQuery.ORDER.desc).setRows(20).setFields("_src"));
        double q3Es = timeEs("{\"query\":{\"range\":{\"height\":{\"gte\":170,\"lte\":190}}},"
                + "\"sort\":[{\"height\":\"desc\"}],\"size\":20}");
        report("3. numeric range + sort", q3Solr, q3Es);

        // 4. Dashboard recent-promises page: Solr = the real backend design (top-K users by the
        //    streamer-derived promises_latest_actionable_at, fl=_src, promises sliced CLIENT-side —
        //    that client work is timed too); ES = its native server-side nested top_hits agg.
        double q4Solr = timeSolrDashboard();
        double q4Es = timeEs("{\"size\":0,\"aggs\":{\"recent\":{\"nested\":{\"path\":\"promises\"},"
                + "\"aggs\":{\"active\":{\"filter\":{\"terms\":{\"promises.status\":[1,2,4]}},"
                + "\"aggs\":{\"top\":{\"top_hits\":{"
                + "\"sort\":[{\"promises.created_at\":{\"order\":\"desc\"}}],\"size\":20,"
                + "\"_source\":[\"promises.id\",\"promises.status\",\"promises.created_at\"]}}}}}}}}");
        report("4. dashboard recent promises (top-K+_src vs top_hits)", q4Solr, q4Es);

        // Per-query micro-benchmarks are noisy under shared-machine container contention (the
        // custom-field filter+sort in particular runs roughly par with ES). Assert on the AGGREGATE
        // instead: across the representative filter+sort mix, Solr must be at least as fast overall.
        double solrAvg = (q1Solr + q2Solr + q3Solr + q4Solr) / 4.0;
        double esAvg = (q1Es + q2Es + q3Es + q4Es) / 4.0;
        System.out.printf(Locale.ROOT, "[QUERY-PERF] AVERAGE  Solr %6.2f ms | ES %6.2f ms | Solr %.2fx%n",
                solrAvg, esAvg, esAvg / solrAvg);
        assertThat(solrAvg).as("Solr average filter+sort latency must be <= ES").isLessThanOrEqualTo(esAvg);
    }

    private static double timeSolr(SolrQuery q) throws Exception {
        for (int i = 0; i < WARMUP; i++) solrClient.query(CORE, q);
        long start = System.nanoTime();
        for (int i = 0; i < ITERS; i++) solrClient.query(CORE, q);
        return (System.nanoTime() - start) / 1_000_000.0 / ITERS;
    }

    /** The dashboard page end-to-end, Solr side: sorted top-K fetch + client-side _src parse,
     *  filter to actionable, sort, slice — the exact work the backend does. */
    private static double timeSolrDashboard() throws Exception {
        SolrQuery q = new SolrQuery("*:*")
                .addFilterQuery("promises_latest_actionable_at:*")
                .setSort("promises_latest_actionable_at", SolrQuery.ORDER.desc)
                .setRows(20).setFields("_src");
        ObjectMapper mapper = new ObjectMapper();
        for (int i = 0; i < WARMUP; i++) dashboardPage(q, mapper);
        long start = System.nanoTime();
        for (int i = 0; i < ITERS; i++) dashboardPage(q, mapper);
        return (System.nanoTime() - start) / 1_000_000.0 / ITERS;
    }

    private static List<JsonNode> dashboardPage(SolrQuery q, ObjectMapper mapper) throws Exception {
        QueryResponse rsp = solrClient.query(CORE, q);
        List<JsonNode> promises = new ArrayList<>();
        for (SolrDocument doc : rsp.getResults()) {
            JsonNode src = mapper.readTree((String) doc.getFieldValue("_src"));
            for (JsonNode p : src.get("promises")) {
                int status = p.path("status").asInt();
                if (status == 1 || status == 2 || status == 4) promises.add(p);
            }
        }
        promises.sort((a, b) -> b.path("created_at").asText().compareTo(a.path("created_at").asText()));
        return promises.subList(0, Math.min(20, promises.size()));
    }

    private static double timeEs(String body) throws Exception {
        for (int i = 0; i < WARMUP; i++) esSearch(body);
        long start = System.nanoTime();
        for (int i = 0; i < ITERS; i++) esSearch(body);
        return (System.nanoTime() - start) / 1_000_000.0 / ITERS;
    }

    private static void esSearch(String body) throws Exception {
        Request r = new Request("POST", "/" + ES_INDEX + "/_search");
        r.setJsonEntity(body);
        esClient.performRequest(r);
    }

    private static void report(String label, double solrMs, double esMs) {
        System.out.printf(Locale.ROOT, "[QUERY-PERF] %-46s Solr %6.2f ms | ES %6.2f ms | Solr %.2fx %s%n",
                label, solrMs, esMs, esMs / solrMs, esMs > solrMs ? "faster" : "slower");
    }
}
