package com.una.kafka.connect.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Probes /admin/info/system once to learn the remote Solr major version,
 * used for picking endpoint variants on older clusters.
 */
public final class SolrVersionDetector {

    private static final Logger log = LoggerFactory.getLogger(SolrVersionDetector.class);

    private SolrVersionDetector() {
    }

    public static int detectMajor(SolrClient client) {
        try {
            GenericSolrRequest req = new GenericSolrRequest(
                    SolrRequest.METHOD.GET, "/admin/info/system", new ModifiableSolrParams());
            NamedList<Object> resp = client.request(req);
            Object lucene = resp.findRecursive("lucene", "solr-spec-version");
            if (lucene == null) {
                return 9;
            }
            return Integer.parseInt(String.valueOf(lucene).split("\\.")[0]);
        } catch (Exception e) {
            log.debug("Solr version detection failed, assuming 9.x: {}", e.getMessage());
            return 9;
        }
    }
}
