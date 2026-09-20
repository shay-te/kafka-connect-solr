package com.una.kafka.connect.solr;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkTask;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Workers commonly run connectors at WARN. With INFO off for {@link SolrSinkTask}, preCommit must
 * skip building the metrics line entirely. slf4j-simple fixes a logger's level when it is created,
 * so the task and its logging are loaded in an isolated plugin-style loader with the level set.
 */
class SolrSinkTaskQuietLoggingTest {

    private static final String LEVEL_PROPERTY =
            "org.slf4j.simpleLogger.log." + SolrSinkTask.class.getName();

    @Test
    void preCommitWithInfoDisabledSkipsTheMetricsLine() throws Exception {
        URL[] urls = {
                ChildFirstClassLoader.codeSourceOf(SolrSinkTask.class),
                ChildFirstClassLoader.codeSourceOf(LoggerFactory.class),
                ChildFirstClassLoader.codeSourceOf(Class.forName("org.slf4j.impl.StaticLoggerBinder"))
        };
        String previous = System.getProperty(LEVEL_PROPERTY);
        System.setProperty(LEVEL_PROPERTY, "warn");
        try (ChildFirstClassLoader loader = new ChildFirstClassLoader(urls,
                "com.una.kafka.connect.solr.", "org.slf4j.")) {
            Class<?> taskClass = Class.forName(SolrSinkTask.class.getName(), true, loader);
            assertThat(taskClass.getClassLoader()).isSameAs(loader);
            SinkTask task = (SinkTask) taskClass.getDeclaredConstructor().newInstance();

            Map<String, String> props = new HashMap<>();
            props.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://127.0.0.1:8983/solr");
            props.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "users");
            props.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "UNUSED");
            task.start(props);
            try {
                Map<TopicPartition, OffsetAndMetadata> offsets =
                        Collections.singletonMap(new TopicPartition("users", 0), new OffsetAndMetadata(7));
                assertThat(task.preCommit(offsets)).isEqualTo(offsets);
                Object logger = Class.forName("org.slf4j.LoggerFactory", true, loader)
                        .getMethod("getLogger", String.class).invoke(null, SolrSinkTask.class.getName());
                assertThat((Boolean) logger.getClass().getMethod("isInfoEnabled").invoke(logger)).isFalse();
            } finally {
                task.stop();
            }
        } finally {
            if (previous == null) System.clearProperty(LEVEL_PROPERTY);
            else System.setProperty(LEVEL_PROPERTY, previous);
        }
    }
}
