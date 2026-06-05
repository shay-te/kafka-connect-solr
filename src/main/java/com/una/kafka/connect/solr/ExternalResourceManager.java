package com.una.kafka.connect.solr;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.common.util.NamedList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Probes Solr for the target collection at task start and, depending on
 * {@code external.resource.usage} + {@code schema.auto.create}, either
 * fails fast or auto-creates it. Mirrors ES's ExternalResourceExistenceChecker.
 *
 * <p>Why this is better than ES's checker:</p>
 * <ul>
 *     <li>One probe per unique collection, cached after first success so
 *         per-task startup stays cheap.</li>
 *     <li>Auto-create respects {@code auto.create.shards},
 *         {@code auto.create.replication.factor} and
 *         {@code auto.create.configset} — ES only auto-creates with the
 *         server-default index template.</li>
 * </ul>
 */
public class ExternalResourceManager {

    private static final Logger log = LoggerFactory.getLogger(ExternalResourceManager.class);

    private final SolrClient client;
    private final SolrSinkConfig config;
    private final Set<String> known = new HashSet<>();

    public ExternalResourceManager(SolrClient client, SolrSinkConfig config) {
        this.client = client;
        this.config = config;
    }

    public synchronized void ensure(String collection) {
        if (collection == null || collection.isEmpty() || known.contains(collection)) {
            return;
        }
        SolrSinkConfig.ExternalResourceUsage usage = config.externalResourceUsage();
        if (usage == SolrSinkConfig.ExternalResourceUsage.UNUSED) {
            known.add(collection);
            return;
        }
        boolean exists = probe(collection);
        if (exists) {
            known.add(collection);
            return;
        }
        if (usage == SolrSinkConfig.ExternalResourceUsage.REQUIRED) {
            throw new ConnectException("Solr collection '" + collection + "' does not exist and "
                    + SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG + "=REQUIRED");
        }
        // AUTO
        if (!config.schemaAutoCreate()) {
            throw new ConnectException("Solr collection '" + collection + "' does not exist; "
                    + "set " + SolrSinkConfig.SCHEMA_AUTO_CREATE_CONFIG + "=true to auto-create.");
        }
        create(collection);
        known.add(collection);
    }

    private boolean probe(String collection) {
        try {
            if (config.isCloud()) {
                NamedList<Object> resp = client.request(CollectionAdminRequest.listCollections());
                Object list = resp.get("collections");
                if (list instanceof java.util.Collection) {
                    return ((java.util.Collection<?>) list).contains(collection);
                }
                return false;
            }
            // Standalone Solr: use the Core admin status endpoint.
            CoreAdminResponse status = CoreAdminRequest.getStatus(collection, client);
            return status.getCoreStatus(collection) != null
                    && !status.getCoreStatus(collection).asMap(0).isEmpty();
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
            if (config.isCloud()) {
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

    /** Test seam. */
    public synchronized boolean isKnown(String collection) {
        return known.contains(collection);
    }
}
