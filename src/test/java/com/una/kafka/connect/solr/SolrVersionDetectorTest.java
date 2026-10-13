package com.una.kafka.connect.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SolrVersionDetectorTest {

    @Test
    void readsTheVersionOfARealSolr() throws Exception {
        EmbeddedSolrServer solr = EmbeddedSolrSupport.start();
        try {
            assertThat(SolrVersionDetector.detect(solr)).matches("9\\.\\d+\\.\\d+.*");
        } finally {
            EmbeddedSolrSupport.stop(solr);
        }
    }

    @Test
    void aLoginWithoutConfigReadOrAnUnreachableSolrReadsUnknown() throws Exception {
        for (Exception failure : new Exception[] {
                new BaseHttpSolrClient.RemoteSolrException("http://x/solr", 403, "forbidden", null),
                new SolrServerException("connection refused")}) {
            SolrClient client = mock(SolrClient.class);
            when(client.request(any(), isNull())).thenThrow(failure);
            assertThat(SolrVersionDetector.detect(client)).as("%s", failure).isEqualTo(SolrVersionDetector.UNKNOWN);
        }
    }
}
