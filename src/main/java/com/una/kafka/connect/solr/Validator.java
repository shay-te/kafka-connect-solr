package com.una.kafka.connect.solr;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Connector-level validation. Mirrors and improves on the ES
 * connector's Validator: every error includes a remediation hint.
 *
 * <p>Better than ES:</p>
 * <ul>
 *     <li>Each error message points at exactly which config key to fix
 *         (ES often returns "invalid configuration" without naming the key).</li>
 *     <li>SSL files are verified existing on disk at validate() time, not
 *         silently at first request.</li>
 *     <li>Kerberos keytab is verified existing on disk.</li>
 *     <li>Throughput knobs are sanity-checked against each other
 *         (e.g. max.buffered.records &ge; batch.size).</li>
 * </ul>
 */
public final class Validator {

    private Validator() {
    }

    public static Config validate(Config base, Map<String, String> props) {
        Map<String, ConfigValue> byName = new HashMap<>();
        for (ConfigValue cv : base.configValues()) {
            byName.put(cv.name(), cv);
        }

        requireConnection(byName, props);
        requireExclusiveConnection(byName, props);
        requireSslFiles(byName, props);
        requireKerberos(byName, props);
        sanityCheckThroughput(byName, props);
        sanityCheckRegexCollection(byName, props);

        return base;
    }

    private static void requireConnection(Map<String, ConfigValue> v, Map<String, String> p) {
        boolean hasUrl = !nullOrEmpty(p.get(SolrSinkConfig.SOLR_URL_CONFIG));
        boolean hasZk = !nullOrEmpty(p.get(SolrSinkConfig.SOLR_ZK_HOST_CONFIG));
        if (!hasUrl && !hasZk) {
            err(v, SolrSinkConfig.SOLR_URL_CONFIG,
                    "Set either '" + SolrSinkConfig.SOLR_URL_CONFIG
                            + "' (e.g. http://solr:8983/solr) or '"
                            + SolrSinkConfig.SOLR_ZK_HOST_CONFIG
                            + "' (e.g. zk1:2181,zk2:2181/solr).");
            err(v, SolrSinkConfig.SOLR_ZK_HOST_CONFIG,
                    "Set either '" + SolrSinkConfig.SOLR_URL_CONFIG
                            + "' or '" + SolrSinkConfig.SOLR_ZK_HOST_CONFIG + "'.");
        }
    }

    private static void requireExclusiveConnection(Map<String, ConfigValue> v, Map<String, String> p) {
        boolean hasUrl = !nullOrEmpty(p.get(SolrSinkConfig.SOLR_URL_CONFIG));
        boolean hasZk = !nullOrEmpty(p.get(SolrSinkConfig.SOLR_ZK_HOST_CONFIG));
        if (hasUrl && hasZk) {
            err(v, SolrSinkConfig.SOLR_URL_CONFIG,
                    "Set either solr.url OR solr.zk.host, not both. Drop one.");
        }
    }

    private static void requireSslFiles(Map<String, ConfigValue> v, Map<String, String> p) {
        String protocol = p.getOrDefault(SolrSinkConfig.SECURITY_PROTOCOL_CONFIG, "PLAINTEXT");
        if (!"SSL".equalsIgnoreCase(protocol)) {
            return;
        }
        checkFile(v, SolrSinkConfig.SSL_KEYSTORE_LOCATION_CONFIG, p.get(SolrSinkConfig.SSL_KEYSTORE_LOCATION_CONFIG), false);
        checkFile(v, SolrSinkConfig.SSL_TRUSTSTORE_LOCATION_CONFIG, p.get(SolrSinkConfig.SSL_TRUSTSTORE_LOCATION_CONFIG), false);
    }

    private static void requireKerberos(Map<String, ConfigValue> v, Map<String, String> p) {
        String principal = p.get(SolrSinkConfig.KERBEROS_PRINCIPAL_CONFIG);
        String keytab = p.get(SolrSinkConfig.KERBEROS_KEYTAB_PATH_CONFIG);
        boolean hasPrincipal = !nullOrEmpty(principal);
        boolean hasKeytab = !nullOrEmpty(keytab);
        if (hasPrincipal != hasKeytab) {
            err(v, SolrSinkConfig.KERBEROS_PRINCIPAL_CONFIG,
                    "Kerberos requires BOTH 'kerberos.user.principal' and 'kerberos.keytab.path'.");
            err(v, SolrSinkConfig.KERBEROS_KEYTAB_PATH_CONFIG,
                    "Kerberos requires BOTH 'kerberos.user.principal' and 'kerberos.keytab.path'.");
            return;
        }
        if (hasKeytab) {
            checkFile(v, SolrSinkConfig.KERBEROS_KEYTAB_PATH_CONFIG, keytab, true);
        }
    }

    private static void sanityCheckThroughput(Map<String, ConfigValue> v, Map<String, String> p) {
        Integer batch = asInt(p.get(SolrSinkConfig.BATCH_SIZE_CONFIG));
        Integer maxBuf = asInt(p.get(SolrSinkConfig.MAX_BUFFERED_RECORDS_CONFIG));
        if (batch != null && maxBuf != null && maxBuf < batch) {
            err(v, SolrSinkConfig.MAX_BUFFERED_RECORDS_CONFIG,
                    "max.buffered.records (" + maxBuf + ") must be >= batch.size (" + batch + ").");
        }
        Integer inFlight = asInt(p.get(SolrSinkConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG));
        if (inFlight != null && inFlight < 1) {
            err(v, SolrSinkConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG,
                    "max.in.flight.requests must be >= 1.");
        }
    }

    private static void sanityCheckRegexCollection(Map<String, ConfigValue> v, Map<String, String> p) {
        String strategy = p.get(SolrSinkConfig.COLLECTION_NAMING_STRATEGY_CONFIG);
        if (!"TOPIC_REGEX".equalsIgnoreCase(strategy)) {
            return;
        }
        String coll = p.get(SolrSinkConfig.SOLR_COLLECTION_CONFIG);
        if (coll == null || !coll.contains("=>")) {
            err(v, SolrSinkConfig.SOLR_COLLECTION_CONFIG,
                    "collection.naming.strategy=TOPIC_REGEX requires '" + SolrSinkConfig.SOLR_COLLECTION_CONFIG
                            + "' in 'pattern=>replacement' form, e.g. 'logs-.*=>logs'.");
        }
    }

    private static void checkFile(Map<String, ConfigValue> v, String key, String path, boolean required) {
        if (nullOrEmpty(path)) {
            if (required) {
                err(v, key, "Required file path is empty.");
            }
            return;
        }
        if (!new File(path).exists()) {
            err(v, key, "File not found on disk: " + path);
        }
    }

    private static void err(Map<String, ConfigValue> v, String name, String message) {
        ConfigValue cv = v.get(name);
        if (cv == null) {
            return;
        }
        List<String> errors = cv.errorMessages();
        if (!errors.contains(message)) {
            cv.addErrorMessage(message);
        }
    }

    private static boolean nullOrEmpty(String s) {
        return s == null || s.trim().isEmpty();
    }

    private static Integer asInt(String s) {
        if (s == null) return null;
        try {
            return Integer.parseInt(s);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
