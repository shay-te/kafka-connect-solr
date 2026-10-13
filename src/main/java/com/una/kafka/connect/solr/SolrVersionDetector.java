package com.una.kafka.connect.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Solr server's version, from {@code /admin/info/system}. Best effort, like the Elasticsearch connector's
 * version check: that handler needs Solr's {@code config-read} permission, so a login without it (or a Solr not
 * reachable yet) reads {@link #UNKNOWN} and the task still starts.
 */
public final class SolrVersionDetector {

    static final String UNKNOWN = "unknown";

    private static final Logger log = LoggerFactory.getLogger(SolrVersionDetector.class);

    private SolrVersionDetector() {
    }

    public static String detect(SolrClient client) {
        try {
            NamedList<Object> info = client.request(
                    new GenericSolrRequest(SolrRequest.METHOD.GET, CommonParams.SYSTEM_INFO_PATH), null);
            Object version = info == null ? null : info.findRecursive("lucene", "solr-spec-version");
            if (version != null) {
                return version.toString();
            }
            log.warn("Solr answered {} without a version", CommonParams.SYSTEM_INFO_PATH);
        } catch (Exception e) {
            log.warn("Could not read the Solr version from {} (needs the config-read permission): {}",
                    CommonParams.SYSTEM_INFO_PATH, e.getMessage());
        }
        return UNKNOWN;
    }
}
