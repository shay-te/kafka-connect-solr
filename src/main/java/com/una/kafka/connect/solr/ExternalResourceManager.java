package com.una.kafka.connect.solr;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.solr.client.solrj.SolrClient;
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

    private final SolrClient client;
    private final SolrSinkConfig config;
    private final Set<String> known = ConcurrentHashMap.newKeySet();
    // Whether the endpoint answers the Collections API. A plain `solr.url` may name a SolrCloud
    // NODE (e.g. SOLR_ZK_HOST left empty): there a collection is not a core of the same name —
    // cores are `<collection>_shard1_replica_n1` — so the CoreAdmin probe reported every existing
    // collection as missing and killed every task on its first write. Asked once, lazily.
    private volatile Boolean collectionsApi;

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
     * plain URL when that endpoint answers the Collections API (a SolrCloud node). Only a reply
     * that says Solr is NOT in SolrCloud mode is remembered as standalone — any other failure
     * (auth, timeout) is not cached, so a transient error cannot pin the wrong semantics.
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
            CollectionAdminRequest.listCollections(client);
            collectionsApi = Boolean.TRUE;
            return true;
        } catch (Exception e) {
            String message = String.valueOf(e.getMessage());
            if (message.contains("SolrCloud")) {
                collectionsApi = Boolean.FALSE;
            }
            return false;
        }
    }

    private boolean probe(String collection) {
        try {
            if (speaksCollectionsApi()) {
                List<String> collections = CollectionAdminRequest.listCollections(client);
                return collections != null && collections.contains(collection);
            }
            CoreAdminResponse status = CoreAdminRequest.getStatus(collection, client);
            org.apache.solr.common.util.NamedList<Object> coreStatus = status.getCoreStatus(collection);
            return coreStatus != null && !coreStatus.asMap(0).isEmpty();
        } catch (Exception e) {
            log.warn("Resource existence probe for '{}' failed: {}", collection, e.getMessage());
            return false;
        }
    }

    private void create(String collection) {
        log.info("Auto-creating Solr collection '{}' (shards={}, rf={}, configset={})",
                collection, config.autoCreateShards(), config.autoCreateReplicationFactor(),
                config.autoCreateConfigset());
        try {
            if (speaksCollectionsApi()) {
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
