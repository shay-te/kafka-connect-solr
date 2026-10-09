package com.una.kafka.connect.solr;

import org.apache.kafka.common.utils.Sanitizer;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

/**
 * One task's counters over JMX, read live from its {@link SolrWriter}, as
 * {@code com.una.kafka.connect.solr:type=sink-task-metrics,connector=<name>,task=<id>}.
 */
public final class SolrSinkTaskMetrics implements SolrSinkTaskMetricsMBean {

    static final String DOMAIN = "com.una.kafka.connect.solr";
    /** Set by {@link SolrSinkConnector#taskConfigs} so each task registers under its own name. */
    static final String TASK_ID_CONFIG = "task.id";

    private static final Logger log = LoggerFactory.getLogger(SolrSinkTaskMetrics.class);

    private final SolrWriter writer;
    private final String solrVersion;
    private final ObjectName name;

    private SolrSinkTaskMetrics(SolrWriter writer, String solrVersion, ObjectName name) {
        this.writer = writer;
        this.solrVersion = solrVersion;
        this.name = name;
    }

    static SolrSinkTaskMetrics register(String connector, String task, SolrWriter writer, String solrVersion) {
        try {
            ObjectName name = new ObjectName(DOMAIN + ":type=sink-task-metrics,connector="
                    + Sanitizer.jmxSanitize(connector) + ",task=" + Sanitizer.jmxSanitize(task));
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            // A task restarted in the same worker reuses its name, and the earlier run may not have stopped cleanly.
            if (server.isRegistered(name)) {
                server.unregisterMBean(name);
            }
            SolrSinkTaskMetrics metrics = new SolrSinkTaskMetrics(writer, solrVersion, name);
            server.registerMBean(metrics, name);
            return metrics;
        } catch (JMException e) {
            throw new ConnectException("Could not register the task's JMX metrics", e);
        }
    }

    void unregister() {
        try {
            ManagementFactory.getPlatformMBeanServer().unregisterMBean(name);
        } catch (JMException e) {
            log.warn("Could not unregister the JMX metrics {}: {}", name, e.getMessage());
        }
    }

    @Override
    public long getRecordsWritten() {
        return writer.recordsWritten();
    }

    @Override
    public long getRecordsFailed() {
        return writer.recordsFailed();
    }

    @Override
    public long getRetries() {
        return writer.retries();
    }

    @Override
    public double getBatchLatencyAvgMs() {
        return writer.avgBatchLatencyMs();
    }

    @Override
    public double getSolrRequestLatencyAvgMs() {
        return writer.avgSolrCallLatencyMs();
    }

    @Override
    public int getQueueDepth() {
        return writer.queueDepth();
    }

    @Override
    public String getSolrVersion() {
        return solrVersion;
    }
}
