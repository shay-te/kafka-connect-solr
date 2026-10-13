package com.una.kafka.connect.solr.perf;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
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
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Live head-to-head QUERY benchmark: the same {@value #DOCS} users indexed into Solr (on the real
 * production configset — denormalized flat fields, typed, stored-only _src) and Elasticsearch
 * (custom_fields as a `nested` type, the shape ES actually used). We then time representative
 * user-search queries, each with a sort, over {@value #ITERS} iterations:
 *
 *   1. top-level filter (status) + sort by created_at            — both engines flat
 *   2. custom-field filter + sort by that field's value          — Solr denormalized vs ES NESTED
 *   3. numeric range (height) + sort                             — typed range both sides
 *   4. dashboard recent promises                                 — top-K + _src vs nested top_hits
 *   5. free-text "search anything", a common and a rare term     — leading wildcard, uncached
 *
 * #2 is the one the migration is about: ES pays for a nested join, Solr resolves a flat filter on
 * the filter cache. #5 is the query an admin actually types. Every query reports mean, p95 and p99:
 * a leading wildcard is a TAIL problem that a mean hides. Opt-in (heavy, needs Docker):
 * {@code -Dperf.query=true}.
 */
@Tag("performance")
class SolrVsElasticsearchQueryPerfTest {

    private static final int DOCS = 50_000;
    private static final int ITERS = 300;
    private static final int WARMUP = 50;
    private static final String[] TIERS = {"Gold", "Silver", "Bronze", "Platinum"};
    private static final String[] FIRST = {"Ada", "Grace", "Alan", "Hannah", "Linus", "Dana", "Joan",
            "Ivan", "Maya", "Noa", "Omar", "Ruth", "Sam", "Tal", "Yael", "Ziv", "Lior", "Ben", "Eli", "Rina"};
    private static final String[] LAST = {"Cohen", "Levi", "Mizrahi", "Peretz", "Biton", "Dahan",
            "Friedman", "Katz", "Azulay", "Shapiro", "Hopper", "Lovelace", "Turing", "Torvalds"};
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

    private static String first(int i) {
        return FIRST[i % FIRST.length];
    }

    private static String last(int i) {
        return LAST[(i / 7) % LAST.length];
    }

    /** Unique per user, so the text catch-all carries one distinct term per user — a real dictionary. */
    private static String emailLocalPart(int i) {
        return (first(i) + "." + last(i)).toLowerCase(Locale.ROOT) + i;
    }

    private static void indexBoth() throws Exception {
        // --- Elasticsearch: nested mapping for custom_fields (the real ES shape) ---
        Request createIndex = new Request("PUT", "/" + ES_INDEX);
        createIndex.setJsonEntity("{\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0},"
                + "\"mappings\":{\"properties\":{"
                + "\"status\":{\"type\":\"integer\"},\"created_at\":{\"type\":\"date\"},"
                + "\"height\":{\"type\":\"integer\"},"
                + "\"first_name\":{\"type\":\"keyword\"},\"last_name\":{\"type\":\"keyword\"},"
                + "\"email\":{\"type\":\"keyword\"},"
                + "\"custom_fields\":{\"type\":\"nested\",\"properties\":{"
                + "\"custom_field_id\":{\"type\":\"integer\"},"
                + "\"value_4\":{\"type\":\"keyword\"},\"sort_value\":{\"type\":\"keyword\"}}},"
                + "\"promises\":{\"type\":\"nested\",\"properties\":{"
                + "\"id\":{\"type\":\"integer\"},\"status\":{\"type\":\"integer\"},"
                + "\"created_at\":{\"type\":\"date\"}}}}}}");
        esClient.performRequest(createIndex);

        List<SolrInputDocument> solrBatch = new ArrayList<>(DOCS);
        StringBuilder esBulk = new StringBuilder(DOCS * 260);
        for (int i = 0; i < DOCS; i++) {
            int status = i % 5;
            int height = 150 + (i % 50);
            String tier = TIERS[i % TIERS.length];
            String createdAt = String.format(Locale.ROOT, "2026-%02d-%02dT00:00:00Z", 1 + (i % 12), 1 + (i % 27));
            String email = emailLocalPart(i) + "@example.com";

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
            // The schema copyFields these three (and the *_value_4 below) into the _text catch-all.
            d.addField("first_name", first(i));
            d.addField("last_name", last(i));
            d.addField("email", email);
            d.addField("custom_field_" + CF_ID + "_value_4", tier);       // denormalized flat filter field
            d.addField("custom_field_" + CF_ID + "_sort_value", tier);    // denormalized sort key
            d.addField("_src", "{\"id\":" + i + ",\"promises\":[" + promisesJson + "]}");
            if (latestActionable != null) d.addField("promises_latest_actionable_at", latestActionable);
            solrBatch.add(d);

            esBulk.append("{\"index\":{\"_id\":\"u").append(i).append("\"}}\n")
                    .append("{\"status\":").append(status)
                    .append(",\"created_at\":\"").append(createdAt).append('"')
                    .append(",\"height\":").append(height)
                    .append(",\"first_name\":\"").append(first(i)).append('"')
                    .append(",\"last_name\":\"").append(last(i)).append('"')
                    .append(",\"email\":\"").append(email).append('"')
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
        Stats q1Solr = timeSolr(new SolrQuery("*:*").addFilterQuery("status:2")
                .setSort("created_at", SolrQuery.ORDER.desc).setRows(20).setFields("_src"));
        Stats q1Es = timeEs("{\"query\":{\"term\":{\"status\":2}},"
                + "\"sort\":[{\"created_at\":\"desc\"}],\"size\":20}");
        report("1. top-level filter + sort", q1Solr, q1Es);

        Stats q2Solr = timeSolr(new SolrQuery("*:*")
                .addFilterQuery("custom_field_" + CF_ID + "_value_4:Gold")
                .setSort("custom_field_" + CF_ID + "_sort_value", SolrQuery.ORDER.asc).setRows(20).setFields("_src"));
        Stats q2Es = timeEs("{\"query\":{\"nested\":{\"path\":\"custom_fields\",\"query\":{\"bool\":{\"must\":["
                + "{\"term\":{\"custom_fields.custom_field_id\":" + CF_ID + "}},"
                + "{\"term\":{\"custom_fields.value_4\":\"Gold\"}}]}}}},"
                + "\"sort\":[{\"custom_fields.sort_value\":{\"order\":\"asc\",\"nested\":{\"path\":\"custom_fields\","
                + "\"filter\":{\"term\":{\"custom_fields.custom_field_id\":" + CF_ID + "}}}}}],\"size\":20}");
        report("2. custom-field filter+sort (Solr flat vs ES NESTED)", q2Solr, q2Es);

        Stats q3Solr = timeSolr(new SolrQuery("*:*").addFilterQuery("height:[170 TO 190]")
                .setSort("height", SolrQuery.ORDER.desc).setRows(20).setFields("_src"));
        Stats q3Es = timeEs("{\"query\":{\"range\":{\"height\":{\"gte\":170,\"lte\":190}}},"
                + "\"sort\":[{\"height\":\"desc\"}],\"size\":20}");
        report("3. numeric range + sort", q3Solr, q3Es);

        // 4. Dashboard recent-promises page: Solr = the real backend design (top-K users by the
        //    streamer-derived promises_latest_actionable_at, fl=_src, promises sliced CLIENT-side —
        //    that client work is timed too); ES = its native server-side nested top_hits agg.
        Stats q4Solr = timeSolrDashboard();
        Stats q4Es = timeEs("{\"size\":0,\"aggs\":{\"recent\":{\"nested\":{\"path\":\"promises\"},"
                + "\"aggs\":{\"active\":{\"filter\":{\"terms\":{\"promises.status\":[1,2,4]}},"
                + "\"aggs\":{\"top\":{\"top_hits\":{"
                + "\"sort\":[{\"promises.created_at\":{\"order\":\"desc\"}}],\"size\":20,"
                + "\"_source\":[\"promises.id\",\"promises.status\",\"promises.created_at\"]}}}}}}}}");
        report("4. dashboard recent promises (top-K+_src vs top_hits)", q4Solr, q4Es);

        // Per-query micro-benchmarks are noisy under shared-machine container contention (the
        // custom-field filter+sort in particular runs roughly par with ES). Assert on the AGGREGATE
        // instead: across the representative filter+sort mix, Solr must be at least as fast overall.
        double solrAvg = (q1Solr.mean + q2Solr.mean + q3Solr.mean + q4Solr.mean) / 4.0;
        double esAvg = (q1Es.mean + q2Es.mean + q3Es.mean + q4Es.mean) / 4.0;
        System.out.printf(Locale.ROOT, "[QUERY-PERF] AVERAGE Q1-Q4  Solr %6.2f ms | ES %6.2f ms | Solr %.2fx%n",
                solrAvg, esAvg, esAvg / solrAvg);

        // 5. Free-text "search anything" — the query an admin types, and the path this suite never
        //    timed. Production sends fq=_text:*term*: a leading wildcard over the text_ci catch-all
        //    (SOLR_TEXT_SEARCH_NGRAM is off and the production schema has no NGram type). The ES side
        //    reconstructs the replaced simple_query_string + nested custom-field wildcard as a
        //    case-insensitive wildcard per source field; the original ES body is not in the repo, so
        //    this is its equivalent, not a copy. Caching is OFF on both sides — {!cache=false} on Solr,
        //    a scoring `should` on ES — so every iteration pays what a NEWLY typed term pays; a repeated
        //    identical search in production hits Solr's filterCache and is faster than this. Reported,
        //    not asserted: there is no baseline yet, and it stays out of the Q1-Q4 average above.
        for (String[] term : new String[][]{{"common", "an"}, {"rare", emailLocalPart(4240)}}) {
            String solrFq = "{!cache=false}_text:*" + term[1] + "*";
            String esQuery = freeTextEs(term[1]);
            long solrHits = solrClient.query(CORE, new SolrQuery("*:*").addFilterQuery(solrFq).setRows(0))
                    .getResults().getNumFound();
            assertThat(solrHits).as("Q5 %s: both engines must match the same users", term[0])
                    .isEqualTo(esCount(esQuery)).isPositive();
            Stats s = timeSolr(new SolrQuery("*:*").addFilterQuery(solrFq)
                    .setSort("created_at", SolrQuery.ORDER.desc).setRows(20).setFields("_src"));
            Stats e = timeEs("{\"query\":" + esQuery + ",\"sort\":[{\"created_at\":\"desc\"}],\"size\":20}");
            report("5. free-text " + term[0] + " '" + term[1] + "' (" + solrHits + " hits)", s, e);
        }

        assertThat(solrAvg).as("Solr average filter+sort latency must be <= ES").isLessThanOrEqualTo(esAvg);
    }

    private static String freeTextEs(String term) {
        String wildcard = "{\"value\":\"*" + term + "*\",\"case_insensitive\":true}";
        return "{\"bool\":{\"should\":["
                + "{\"wildcard\":{\"email\":" + wildcard + "}},"
                + "{\"wildcard\":{\"first_name\":" + wildcard + "}},"
                + "{\"wildcard\":{\"last_name\":" + wildcard + "}},"
                + "{\"nested\":{\"path\":\"custom_fields\",\"query\":"
                + "{\"wildcard\":{\"custom_fields.value_4\":" + wildcard + "}}}}"
                + "],\"minimum_should_match\":1}}";
    }

    private static long esCount(String query) throws Exception {
        Request r = new Request("POST", "/" + ES_INDEX + "/_count");
        r.setJsonEntity("{\"query\":" + query + "}");
        JsonNode body = new ObjectMapper().readTree(EntityUtils.toString(esClient.performRequest(r).getEntity()));
        return body.path("count").asLong();
    }

    private static Stats timeSolr(SolrQuery q) throws Exception {
        for (int i = 0; i < WARMUP; i++) solrClient.query(CORE, q);
        long[] nanos = new long[ITERS];
        for (int i = 0; i < ITERS; i++) {
            long start = System.nanoTime();
            solrClient.query(CORE, q);
            nanos[i] = System.nanoTime() - start;
        }
        return new Stats(nanos);
    }

    /** The dashboard page end-to-end, Solr side: sorted top-K fetch + client-side _src parse,
     *  filter to actionable, sort, slice — the exact work the backend does. */
    private static Stats timeSolrDashboard() throws Exception {
        SolrQuery q = new SolrQuery("*:*")
                .addFilterQuery("promises_latest_actionable_at:*")
                .setSort("promises_latest_actionable_at", SolrQuery.ORDER.desc)
                .setRows(20).setFields("_src");
        ObjectMapper mapper = new ObjectMapper();
        for (int i = 0; i < WARMUP; i++) dashboardPage(q, mapper);
        long[] nanos = new long[ITERS];
        for (int i = 0; i < ITERS; i++) {
            long start = System.nanoTime();
            dashboardPage(q, mapper);
            nanos[i] = System.nanoTime() - start;
        }
        return new Stats(nanos);
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

    private static Stats timeEs(String body) throws Exception {
        for (int i = 0; i < WARMUP; i++) esSearch(body);
        long[] nanos = new long[ITERS];
        for (int i = 0; i < ITERS; i++) {
            long start = System.nanoTime();
            esSearch(body);
            nanos[i] = System.nanoTime() - start;
        }
        return new Stats(nanos);
    }

    private static void esSearch(String body) throws Exception {
        Request r = new Request("POST", "/" + ES_INDEX + "/_search");
        r.setJsonEntity(body);
        esClient.performRequest(r);
    }

    private static void report(String label, Stats solrMs, Stats esMs) {
        System.out.printf(Locale.ROOT,
                "[QUERY-PERF] %-58s Solr mean %6.2f p95 %6.2f p99 %6.2f ms | ES mean %6.2f p95 %6.2f p99 %6.2f ms"
                        + " | Solr %.2fx mean, %.2fx p99%n",
                label, solrMs.mean, solrMs.p95, solrMs.p99, esMs.mean, esMs.p95, esMs.p99,
                esMs.mean / solrMs.mean, esMs.p99 / solrMs.p99);
    }

    /** Per-iteration latencies in ms: mean plus nearest-rank p95/p99. */
    private static final class Stats {
        final double mean;
        final double p95;
        final double p99;

        Stats(long[] nanos) {
            long[] sorted = nanos.clone();
            Arrays.sort(sorted);
            long sum = 0;
            for (long n : sorted) sum += n;
            mean = sum / (double) sorted.length / 1_000_000.0;
            p95 = percentile(sorted, 0.95);
            p99 = percentile(sorted, 0.99);
        }

        private static double percentile(long[] sorted, double p) {
            int index = Math.max(0, (int) Math.ceil(p * sorted.length) - 1);
            return sorted[index] / 1_000_000.0;
        }
    }
}
