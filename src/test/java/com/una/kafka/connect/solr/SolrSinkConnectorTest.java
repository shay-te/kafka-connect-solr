package com.una.kafka.connect.solr;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class SolrSinkConnectorTest {

    @Test
    void taskConfigsArePropagated() {
        SolrSinkConnector connector = new SolrSinkConnector();
        Map<String, String> props = new HashMap<>();
        props.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://solr:8983/solr");
        props.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "users");
        connector.start(props);

        List<Map<String, String>> configs = connector.taskConfigs(3);
        assertThat(configs).hasSize(3);
        configs.forEach(c -> assertThat(c.get(SolrSinkConfig.SOLR_COLLECTION_CONFIG))
                .isEqualTo("users"));
    }

    @Test
    void validationFailsWhenNoConnectionConfigured() {
        SolrSinkConnector connector = new SolrSinkConnector();
        Map<String, String> props = new HashMap<>();
        props.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "users");
        Config result = connector.validate(props);
        boolean foundError = false;
        for (ConfigValue v : result.configValues()) {
            if (!v.errorMessages().isEmpty()) {
                foundError = true;
                break;
            }
        }
        assertThat(foundError).isTrue();
    }
}
