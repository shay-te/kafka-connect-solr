package com.una.kafka.connect.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.junit.jupiter.api.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Every metric the task lists is readable over JMX while it runs, under its own name, and gone after stop(). */
class SolrSinkTaskMetricsTest {

    private static final MBeanServer JMX = ManagementFactory.getPlatformMBeanServer();

    private static class TestTask extends SolrSinkTask {
        private final SolrWriter writer;
        TestTask(SolrWriter writer) { this.writer = writer; }
        @Override protected SolrClient createClient(SolrSinkConfig config) { return mock(SolrClient.class); }
        @Override protected SolrWriter createWriter(SolrClient c, SolrSinkConfig cfg, OffsetTracker tracker) { return writer; }
    }

    private static Map<String, String> props(String task) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "users");
        p.put("name", "users-sink");
        p.put(SolrSinkTaskMetrics.TASK_ID_CONFIG, task);
        return p;
    }

    private static ObjectName name(String task) throws Exception {
        return new ObjectName("com.una.kafka.connect.solr:type=sink-task-metrics,connector=users-sink,task=" + task);
    }

    private static SolrWriter writerWithCounters() {
        SolrWriter writer = mock(SolrWriter.class);
        when(writer.recordsWritten()).thenReturn(120L);
        when(writer.recordsFailed()).thenReturn(3L);
        when(writer.retries()).thenReturn(7L);
        when(writer.avgBatchLatencyMs()).thenReturn(12.5);
        when(writer.avgSolrCallLatencyMs()).thenReturn(4.25);
        when(writer.queueDepth()).thenReturn(42);
        return writer;
    }

    @Test
    void everyListedMetricIsReadableOverJmxWhileTheTaskRuns() throws Exception {
        TestTask task = new TestTask(writerWithCounters());
        task.start(props("0"));
        try {
            ObjectName name = name("0");
            assertThat(JMX.getAttribute(name, "RecordsWritten")).isEqualTo(120L);
            assertThat(JMX.getAttribute(name, "RecordsFailed")).isEqualTo(3L);
            assertThat(JMX.getAttribute(name, "Retries")).isEqualTo(7L);
            assertThat(JMX.getAttribute(name, "BatchLatencyAvgMs")).isEqualTo(12.5);
            assertThat(JMX.getAttribute(name, "SolrRequestLatencyAvgMs")).isEqualTo(4.25);
            assertThat(JMX.getAttribute(name, "QueueDepth")).isEqualTo(42);
            assertThat(JMX.getAttribute(name, "SolrVersion")).isEqualTo(SolrVersionDetector.UNKNOWN);
        } finally {
            task.stop();
        }
        assertThat(JMX.isRegistered(name("0"))).as("unregistered on stop").isFalse();
    }

    @Test
    void eachTaskOfAConnectorHasItsOwnMetrics() throws Exception {
        TestTask first = new TestTask(writerWithCounters());
        TestTask second = new TestTask(mock(SolrWriter.class));
        first.start(props("0"));
        second.start(props("1"));
        try {
            assertThat(JMX.getAttribute(name("0"), "RecordsWritten")).isEqualTo(120L);
            assertThat(JMX.getAttribute(name("1"), "RecordsWritten")).isEqualTo(0L);
        } finally {
            first.stop();
            second.stop();
        }
    }
}
