package com.una.kafka.connect.solr;

/** What each running {@link SolrSinkTask} exposes over JMX; see {@link SolrSinkTaskMetrics}. */
public interface SolrSinkTaskMetricsMBean {

    long getRecordsWritten();

    long getRecordsFailed();

    long getRetries();

    double getBatchLatencyAvgMs();

    double getSolrRequestLatencyAvgMs();

    int getQueueDepth();

    String getSolrVersion();
}
