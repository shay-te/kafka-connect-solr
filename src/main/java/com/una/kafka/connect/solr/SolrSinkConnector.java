package com.una.kafka.connect.solr;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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
            configs.add(Map.copyOf(props));
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
        return Validator.validate(super.validate(connectorConfigs), connectorConfigs);
    }
}
