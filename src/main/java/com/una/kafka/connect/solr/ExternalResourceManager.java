package com.una.kafka.connect.solr;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class ExternalResourceManager {

    private static final Logger log = LoggerFactory.getLogger(ExternalResourceManager.class);
    /** Standalone Solr's answer to a Collections API call; the only reply cached as standalone. */
    static final String STANDALONE_SIGNATURE = "not running in SolrCloud mode";

    private final SolrClient client;
    private final SolrSinkConfig config;
    private final Set<String> known = ConcurrentHashMap.newKeySet();
    // Whether the endpoint answers the Collections API. A plain `solr.url` may name a SolrCloud
    // NODE (e.g. SOLR_ZK_HOST left empty): there a collection is not a core of the same name —
    // cores are `<collection>_shard1_replica_n1` — so the CoreAdmin probe reported every existing
    // collection as missing and killed every task on its first write. Asked once, lazily.
    private volatile Boolean collectionsApi;
    // The listing that answered "is this SolrCloud?", reused by the probe right after it.
    private List<String> freshListing;

    public ExternalResourceManager(SolrClient client, SolrSinkConfig config) {
        this.client = client;
        this.config = config;
    }

    public void ensure(String collection) {
        if (collection == null || collection.isEmpty() || known.contains(collection)) {
            return;
        }
        ensureSlow(collection);
    }

    private synchronized void ensureSlow(String collection) {
        if (known.contains(collection)) {
            return;
        }
        SolrSinkConfig.ExternalResourceUsage usage = config.externalResourceUsage();
        if (usage == SolrSinkConfig.ExternalResourceUsage.UNUSED) {
            known.add(collection);
            return;
        }
        if (probe(collection)) {
            known.add(collection);
            return;
        }
        if (usage == SolrSinkConfig.ExternalResourceUsage.REQUIRED) {
            throw new ConnectException("Solr collection '" + collection + "' does not exist and "
                    + SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG + "=REQUIRED");
        }
        if (!config.schemaAutoCreate()) {
            throw new ConnectException("Solr collection '" + collection + "' does not exist; "
                    + "set " + SolrSinkConfig.SCHEMA_AUTO_CREATE_CONFIG + "=true to auto-create.");
        }
        create(collection);
        known.add(collection);
    }

    /**
     * True when collection semantics apply: always for a ZooKeeper-configured client, and for a
     * plain URL when that endpoint answers the Collections API (a SolrCloud node). Only
     * standalone's own reply is cached as "standalone"; any other failure goes to
     * {@link #probeFailure}, never to the CoreAdmin probe (which answers "missing" on a cloud node).
     */
    private boolean speaksCollectionsApi() {
        if (config.isCloud()) {
            return true;
        }
        Boolean cached = collectionsApi;
        if (cached != null) {
            return cached;
        }
        try {
            freshListing = CollectionAdminRequest.listCollections(client);
            collectionsApi = Boolean.TRUE;
            return true;
        } catch (Exception e) {
            if (String.valueOf(e.getMessage()).contains(STANDALONE_SIGNATURE)) {
                collectionsApi = Boolean.FALSE;
                return false;
            }
            throw probeFailure("tell whether Solr is SolrCloud", e);
        }
    }

    /**
     * Transient (I/O, 429, 5xx) is retried; a refusal (401/403) fails the task and names the status.
     * Read as "missing", a blip killed the task; read as retriable, a bad credential spun forever
     * with the task RUNNING and nothing indexed.
     */
    private static ConnectException probeFailure(String what, Exception error) {
        String reason = error instanceof BaseHttpSolrClient.RemoteSolrException
                ? "HTTP " + ((BaseHttpSolrClient.RemoteSolrException) error).code() + ": " + error.getMessage()
                : String.valueOf(error.getMessage());
        String message = "Could not " + what + " (" + reason + ")";
        return RetryUtil.isRetriable(error)
                ? new RetriableException(message, error) : new ConnectException(message, error);
    }

    private boolean probe(String collection) {
        try {
            if (speaksCollectionsApi()) {
                List<String> collections = freshListing != null
                        ? freshListing : CollectionAdminRequest.listCollections(client);
                freshListing = null;
                return collections != null && collections.contains(collection);
            }
            CoreAdminResponse status = CoreAdminRequest.getStatus(collection, client);
            org.apache.solr.common.util.NamedList<Object> coreStatus = status.getCoreStatus(collection);
            return coreStatus != null && !coreStatus.asMap(0).isEmpty();
        } catch (ConnectException e) {
            throw e;                    // already classified by probeFailure
        } catch (Exception e) {
            throw probeFailure("check whether '" + collection + "' exists", e);
        }
    }

    private void create(String collection) {
        log.info("Auto-creating Solr collection '{}' (shards={}, rf={}, configset={})",
                collection, config.autoCreateShards(), config.autoCreateReplicationFactor(),
                config.autoCreateConfigset());
        boolean cloud = speaksCollectionsApi();    // outside the try: retriable must stay retriable
        try {
            if (cloud) {
                CollectionAdminRequest.Create create = CollectionAdminRequest.createCollection(
                        collection,
                        config.autoCreateConfigset(),
                        config.autoCreateShards(),
                        config.autoCreateReplicationFactor());
                create.process(client);
            } else {
                CoreAdminRequest.Create create = new CoreAdminRequest.Create();
                create.setCoreName(collection);
                create.setConfigSet(config.autoCreateConfigset());
                create.process(client);
            }
        } catch (Exception e) {
            throw new ConnectException("Failed to auto-create collection '" + collection + "': "
                    + e.getMessage(), e);
        }
    }

    public boolean isKnown(String collection) {
        return known.contains(collection);
    }
}
