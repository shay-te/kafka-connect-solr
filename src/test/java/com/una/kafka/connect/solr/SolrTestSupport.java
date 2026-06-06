package com.una.kafka.connect.solr;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.Http2SolrClient;

/**
 * Shared test helpers for Solr Testcontainers ITs.
 */
final class SolrTestSupport {

    private SolrTestSupport() {
    }

    /**
     * Solr's HTTP port opens before its cores finish loading; without this
     * the first request often fails with "SolrCore is loading" (and worse,
     * SolrJ 9.4.1 chokes on the error response with a URLDecoder exception).
     */
    static void awaitCoreReady(String baseUrl, String core) throws Exception {
        long deadline = System.currentTimeMillis() + 60_000L;
        Exception last = null;
        try (Http2SolrClient client = new Http2SolrClient.Builder(baseUrl).build()) {
            while (System.currentTimeMillis() < deadline) {
                try {
                    client.query(core, new SolrQuery("*:*").setRows(0));
                    return;
                } catch (Exception e) {
                    last = e;
                    Thread.sleep(250);
                }
            }
        }
        throw new RuntimeException("Solr core '" + core + "' never became ready", last);
    }
}
