package com.una.kafka.connect.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.SimpleOrderedMap;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SolrVersionDetectorTest {

    @Test
    void parsesLuceneSolrSpecVersion() throws Exception {
        SolrClient client = mock(SolrClient.class);
        NamedList<Object> response = new NamedList<>();
        SimpleOrderedMap<Object> lucene = new SimpleOrderedMap<>();
        lucene.add("solr-spec-version", "9.4.1");
        response.add("lucene", lucene);
        when(client.request(any())).thenReturn(response);

        assertThat(SolrVersionDetector.detectMajor(client)).isEqualTo(9);
    }

    @Test
    void defaultsTo9WhenMissing() throws Exception {
        SolrClient client = mock(SolrClient.class);
        NamedList<Object> response = new NamedList<>();
        when(client.request(any())).thenReturn(response);

        assertThat(SolrVersionDetector.detectMajor(client)).isEqualTo(9);
    }

    @Test
    void defaultsTo9OnException() throws Exception {
        SolrClient client = mock(SolrClient.class);
        when(client.request(any())).thenThrow(new RuntimeException("boom"));

        assertThat(SolrVersionDetector.detectMajor(client)).isEqualTo(9);
    }
}
