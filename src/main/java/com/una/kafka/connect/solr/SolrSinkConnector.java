package com.una.kafka.connect.solr;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SolrSinkConnector extends SinkConnector {

    private static final Logger log = LoggerFactory.getLogger(SolrSinkConnector.class);

    private Map<String, String> props;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting SolrSinkConnector v{}", version());
        this.props = new HashMap<>(props);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return SolrSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; i++) {
            configs.add(Collections.unmodifiableMap(new HashMap<>(props)));
        }
        return configs;
    }

    @Override
    public void stop() {
        log.info("Stopping SolrSinkConnector");
    }

    @Override
    public ConfigDef config() {
        return SolrSinkConfig.config();
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        Config base = super.validate(connectorConfigs);
        boolean hasUrl = !nullOrEmpty(connectorConfigs.get(SolrSinkConfig.SOLR_URL_CONFIG));
        boolean hasZk = !nullOrEmpty(connectorConfigs.get(SolrSinkConfig.SOLR_ZK_HOST_CONFIG));
        if (!hasUrl && !hasZk) {
            for (ConfigValue v : base.configValues()) {
                if (SolrSinkConfig.SOLR_URL_CONFIG.equals(v.name())
                        || SolrSinkConfig.SOLR_ZK_HOST_CONFIG.equals(v.name())) {
                    v.addErrorMessage("One of solr.url or solr.zk.host must be configured.");
                }
            }
        }
        return base;
    }

    private static boolean nullOrEmpty(String s) {
        return s == null || s.trim().isEmpty();
    }
}
