package com.una.kafka.connect.solr;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.Http2SolrClient;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Shared test helpers for Solr Testcontainers ITs.
 */
final class SolrTestSupport {

    /** Solr container startup can take up to a minute on first run; cores load after the port opens. */
    private static final long CORE_READY_TIMEOUT_MS = 60_000L;
    private static final long CORE_READY_POLL_MS = 250L;

    /** commitWithin is best-effort; wait up to this long for an indexed doc to become visible. */
    private static final long COMMIT_VISIBLE_TIMEOUT_MS = 5_000L;
    private static final long COMMIT_VISIBLE_POLL_MS = 50L;

    private SolrTestSupport() {
    }

    /**
     * Solr's HTTP port opens before its cores finish loading; without this
     * the first request often fails with "SolrCore is loading" (and worse,
     * SolrJ 9.4.1 chokes on the error response with a URLDecoder exception).
     */
    static void awaitCoreReady(String baseUrl, String core) throws Exception {
        long deadline = System.currentTimeMillis() + CORE_READY_TIMEOUT_MS;
        Exception last = null;
        try (Http2SolrClient client = new Http2SolrClient.Builder(baseUrl).build()) {
            while (System.currentTimeMillis() < deadline) {
                try {
                    client.query(core, new SolrQuery("*:*").setRows(0));
                    return;
                } catch (Exception e) {
                    last = e;
                    Thread.sleep(CORE_READY_POLL_MS);
                }
            }
        }
        throw new RuntimeException("Solr core '" + core + "' never became ready", last);
    }

    /** Poll Solr until {@code query} returns {@code expected} hits, or fail after the visibility timeout. */
    static void awaitHits(String baseUrl, String core, String query, long expected) throws Exception {
        long deadline = System.currentTimeMillis() + COMMIT_VISIBLE_TIMEOUT_MS;
        long last = -1;
        try (Http2SolrClient client = new Http2SolrClient.Builder(baseUrl).build()) {
            while (System.currentTimeMillis() < deadline) {
                last = client.query(core, new SolrQuery(query).setRows(0))
                        .getResults().getNumFound();
                if (last == expected) return;
                Thread.sleep(COMMIT_VISIBLE_POLL_MS);
            }
        }
        assertThat(last).as("expected %d hits for '%s'", expected, query).isEqualTo(expected);
    }
}
