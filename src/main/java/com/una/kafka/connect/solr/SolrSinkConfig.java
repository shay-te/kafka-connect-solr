package com.una.kafka.connect.solr;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Connector + task configuration. Names mirror the Confluent
 * kafka-connect-elasticsearch connector wherever possible so that
 * operators can move a sink config over with minimal change.
 */
public class SolrSinkConfig extends AbstractConfig {

    public static final String SOLR_URL_CONFIG = "solr.url";
    public static final String SOLR_ZK_HOST_CONFIG = "solr.zk.host";
    public static final String SOLR_COLLECTION_CONFIG = "solr.collection";
    public static final String CONNECTION_USERNAME_CONFIG = "connection.username";
    public static final String CONNECTION_PASSWORD_CONFIG = "connection.password";
    public static final String CONNECTION_TIMEOUT_MS_CONFIG = "connection.timeout.ms";
    public static final String READ_TIMEOUT_MS_CONFIG = "read.timeout.ms";

    public static final String KEY_IGNORE_CONFIG = "key.ignore";
    public static final String SCHEMA_IGNORE_CONFIG = "schema.ignore";
    public static final String BEHAVIOR_ON_NULL_VALUES_CONFIG = "behavior.on.null.values";
    public static final String BEHAVIOR_ON_MALFORMED_DOCS_CONFIG = "behavior.on.malformed.documents";

    public static final String BATCH_SIZE_CONFIG = "batch.size";
    public static final String LINGER_MS_CONFIG = "linger.ms";
    public static final String FLUSH_TIMEOUT_MS_CONFIG = "flush.timeout.ms";
    public static final String MAX_IN_FLIGHT_REQUESTS_CONFIG = "max.in.flight.requests";
    public static final String MAX_BUFFERED_RECORDS_CONFIG = "max.buffered.records";
    public static final String MAX_RETRIES_CONFIG = "max.retries";
    public static final String RETRY_BACKOFF_MS_CONFIG = "retry.backoff.ms";

    public static final String WRITE_METHOD_CONFIG = "write.method";
    public static final String ID_STRATEGY_CONFIG = "id.strategy";
    public static final String ID_FIELD_CONFIG = "id.field";
    public static final String COLLECTION_NAMING_STRATEGY_CONFIG = "collection.naming.strategy";

    public static final String SCHEMA_AUTO_CREATE_CONFIG = "schema.auto.create";
    public static final String SCHEMA_AUTO_EVOLVE_CONFIG = "schema.auto.evolve";

    public static final String COMMIT_WITHIN_MS_CONFIG = "commit.within.ms";

    public enum BehaviorOnNullValues {
        IGNORE, DELETE, FAIL;
        public static BehaviorOnNullValues parse(String s) { return valueOf(s.toUpperCase(Locale.ROOT)); }
    }

    public enum WriteMethod {
        INDEX, UPSERT, ATOMIC_UPDATE;
        public static WriteMethod parse(String s) { return valueOf(s.toUpperCase(Locale.ROOT)); }
    }

    public enum IdStrategy {
        KAFKA_KEY, RECORD_FIELD, TOPIC_PARTITION_OFFSET, UUID;
        public static IdStrategy parse(String s) { return valueOf(s.toUpperCase(Locale.ROOT)); }
    }

    public enum BehaviorOnMalformed {
        IGNORE, WARN, FAIL;
        public static BehaviorOnMalformed parse(String s) { return valueOf(s.toUpperCase(Locale.ROOT)); }
    }

    public static ConfigDef config() {
        ConfigDef def = new ConfigDef();
        String connectionGroup = "Connector";
        int order = 0;

        def.define(SOLR_URL_CONFIG, Type.LIST, "", Importance.HIGH,
                "Comma-separated list of Solr base URLs, e.g. http://solr:8983/solr. "
                        + "Either solr.url or solr.zk.host must be set.",
                connectionGroup, ++order, Width.LONG, "Solr URL(s)");
        def.define(SOLR_ZK_HOST_CONFIG, Type.STRING, "", Importance.HIGH,
                "Zookeeper connection string for SolrCloud (zk1:2181,zk2:2181/solr). "
                        + "Leave blank for standalone Solr.",
                connectionGroup, ++order, Width.LONG, "Zookeeper host");
        def.define(SOLR_COLLECTION_CONFIG, Type.STRING, "", Importance.HIGH,
                "Default Solr collection / core. Falls back to the topic name when empty.",
                connectionGroup, ++order, Width.MEDIUM, "Collection");
        def.define(CONNECTION_USERNAME_CONFIG, Type.STRING, "", Importance.MEDIUM,
                "Username for basic auth (optional).",
                connectionGroup, ++order, Width.MEDIUM, "Username");
        def.define(CONNECTION_PASSWORD_CONFIG, Type.PASSWORD, "", Importance.MEDIUM,
                "Password for basic auth (optional).",
                connectionGroup, ++order, Width.MEDIUM, "Password");
        def.define(CONNECTION_TIMEOUT_MS_CONFIG, Type.INT, 5_000, Importance.LOW,
                "HTTP connect timeout in ms.",
                connectionGroup, ++order, Width.SHORT, "Connect timeout");
        def.define(READ_TIMEOUT_MS_CONFIG, Type.INT, 60_000, Importance.LOW,
                "HTTP socket read timeout in ms.",
                connectionGroup, ++order, Width.SHORT, "Read timeout");

        String behaviourGroup = "Behavior";
        order = 0;
        def.define(KEY_IGNORE_CONFIG, Type.BOOLEAN, false, Importance.HIGH,
                "If true the connector ignores the record key when generating the document id.",
                behaviourGroup, ++order, Width.SHORT, "Ignore key");
        def.define(SCHEMA_IGNORE_CONFIG, Type.BOOLEAN, false, Importance.MEDIUM,
                "If true the connector skips schema introspection.",
                behaviourGroup, ++order, Width.SHORT, "Ignore schema");
        def.define(BEHAVIOR_ON_NULL_VALUES_CONFIG, Type.STRING, "ignore", Importance.LOW,
                "Tombstones: ignore | delete | fail.",
                behaviourGroup, ++order, Width.MEDIUM, "Null value behavior");
        def.define(BEHAVIOR_ON_MALFORMED_DOCS_CONFIG, Type.STRING, "fail", Importance.LOW,
                "Malformed records: ignore | warn | fail.",
                behaviourGroup, ++order, Width.MEDIUM, "Malformed behavior");

        String throughputGroup = "Throughput";
        order = 0;
        def.define(BATCH_SIZE_CONFIG, Type.INT, 2000, Importance.HIGH,
                "Records per Solr bulk update.",
                throughputGroup, ++order, Width.SHORT, "Batch size");
        def.define(LINGER_MS_CONFIG, Type.LONG, 50L, Importance.MEDIUM,
                "Max milliseconds to wait while filling a batch before sending.",
                throughputGroup, ++order, Width.SHORT, "Linger ms");
        def.define(FLUSH_TIMEOUT_MS_CONFIG, Type.LONG, 30_000L, Importance.MEDIUM,
                "Max milliseconds to wait when flushing in-flight requests.",
                throughputGroup, ++order, Width.SHORT, "Flush timeout");
        def.define(MAX_IN_FLIGHT_REQUESTS_CONFIG, Type.INT, 8, Importance.MEDIUM,
                "Concurrent Solr requests per task. Higher than the ES connector default (5) "
                        + "for higher throughput.",
                throughputGroup, ++order, Width.SHORT, "Max in-flight requests");
        def.define(MAX_BUFFERED_RECORDS_CONFIG, Type.INT, 20_000, Importance.MEDIUM,
                "Maximum records buffered across all in-flight batches before back-pressure.",
                throughputGroup, ++order, Width.SHORT, "Max buffered records");
        def.define(MAX_RETRIES_CONFIG, Type.INT, 5, Importance.MEDIUM,
                "Maximum retry attempts on retryable Solr failures.",
                throughputGroup, ++order, Width.SHORT, "Max retries");
        def.define(RETRY_BACKOFF_MS_CONFIG, Type.LONG, 200L, Importance.LOW,
                "Initial backoff between retries; doubles per attempt up to 30s.",
                throughputGroup, ++order, Width.SHORT, "Retry backoff");

        String writeGroup = "Write";
        order = 0;
        def.define(WRITE_METHOD_CONFIG, Type.STRING, "INDEX", Importance.MEDIUM,
                "INDEX (full doc) | UPSERT (set semantics) | ATOMIC_UPDATE (per-field set/add/inc).",
                writeGroup, ++order, Width.MEDIUM, "Write method");
        def.define(ID_STRATEGY_CONFIG, Type.STRING, "KAFKA_KEY", Importance.MEDIUM,
                "KAFKA_KEY | RECORD_FIELD | TOPIC_PARTITION_OFFSET | UUID.",
                writeGroup, ++order, Width.MEDIUM, "Id strategy");
        def.define(ID_FIELD_CONFIG, Type.STRING, "id", Importance.LOW,
                "When id.strategy=RECORD_FIELD, the field path (dot-paths supported).",
                writeGroup, ++order, Width.MEDIUM, "Id field");
        def.define(COLLECTION_NAMING_STRATEGY_CONFIG, Type.STRING, "TOPIC", Importance.LOW,
                "TOPIC | STATIC | TOPIC_REGEX (solr.collection takes 'pattern=>replacement').",
                writeGroup, ++order, Width.LONG, "Collection naming");
        def.define(SCHEMA_AUTO_CREATE_CONFIG, Type.BOOLEAN, false, Importance.LOW,
                "If true, attempt to create the target collection on SolrCloud.",
                writeGroup, ++order, Width.SHORT, "Auto create");
        def.define(SCHEMA_AUTO_EVOLVE_CONFIG, Type.BOOLEAN, false, Importance.LOW,
                "If true the connector adds new fields to the managed schema.",
                writeGroup, ++order, Width.SHORT, "Auto evolve");
        def.define(COMMIT_WITHIN_MS_CONFIG, Type.LONG, 1_000L, Importance.LOW,
                "Solr commitWithin sent with every bulk request. 0 disables soft commits.",
                writeGroup, ++order, Width.SHORT, "commitWithin");

        return def;
    }

    public SolrSinkConfig(Map<String, String> originals) {
        super(config(), originals);
    }

    public List<String> solrUrls() { return getList(SOLR_URL_CONFIG); }
    public String zkHost() { return getString(SOLR_ZK_HOST_CONFIG); }
    public String defaultCollection() { return getString(SOLR_COLLECTION_CONFIG); }
    public String username() { return getString(CONNECTION_USERNAME_CONFIG); }
    public String password() {
        return getPassword(CONNECTION_PASSWORD_CONFIG) == null ? "" : getPassword(CONNECTION_PASSWORD_CONFIG).value();
    }
    public int connectionTimeoutMs() { return getInt(CONNECTION_TIMEOUT_MS_CONFIG); }
    public int readTimeoutMs() { return getInt(READ_TIMEOUT_MS_CONFIG); }

    public boolean keyIgnore() { return getBoolean(KEY_IGNORE_CONFIG); }
    public boolean schemaIgnore() { return getBoolean(SCHEMA_IGNORE_CONFIG); }
    public BehaviorOnNullValues behaviorOnNullValues() { return BehaviorOnNullValues.parse(getString(BEHAVIOR_ON_NULL_VALUES_CONFIG)); }
    public BehaviorOnMalformed behaviorOnMalformed() { return BehaviorOnMalformed.parse(getString(BEHAVIOR_ON_MALFORMED_DOCS_CONFIG)); }

    public int batchSize() { return getInt(BATCH_SIZE_CONFIG); }
    public long lingerMs() { return getLong(LINGER_MS_CONFIG); }
    public long flushTimeoutMs() { return getLong(FLUSH_TIMEOUT_MS_CONFIG); }
    public int maxInFlight() { return getInt(MAX_IN_FLIGHT_REQUESTS_CONFIG); }
    public int maxBufferedRecords() { return getInt(MAX_BUFFERED_RECORDS_CONFIG); }
    public int maxRetries() { return getInt(MAX_RETRIES_CONFIG); }
    public long retryBackoffMs() { return getLong(RETRY_BACKOFF_MS_CONFIG); }

    public WriteMethod writeMethod() { return WriteMethod.parse(getString(WRITE_METHOD_CONFIG)); }
    public IdStrategy idStrategy() { return IdStrategy.parse(getString(ID_STRATEGY_CONFIG)); }
    public String idField() { return getString(ID_FIELD_CONFIG); }
    public String collectionNamingStrategy() { return getString(COLLECTION_NAMING_STRATEGY_CONFIG); }

    public boolean schemaAutoCreate() { return getBoolean(SCHEMA_AUTO_CREATE_CONFIG); }
    public boolean schemaAutoEvolve() { return getBoolean(SCHEMA_AUTO_EVOLVE_CONFIG); }
    public long commitWithinMs() { return getLong(COMMIT_WITHIN_MS_CONFIG); }

    public boolean isCloud() {
        return zkHost() != null && !zkHost().isEmpty();
    }
}
