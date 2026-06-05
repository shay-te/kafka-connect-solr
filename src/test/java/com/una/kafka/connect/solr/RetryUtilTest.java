package com.una.kafka.connect.solr;

import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.SocketTimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RetryUtilTest {

    @Test
    void retriesAndSucceeds() {
        final int[] attempts = {0};
        Integer result = RetryUtil.retry(() -> {
            attempts[0]++;
            if (attempts[0] < 3) {
                throw new IOException("boom");
            }
            return 42;
        }, 5, 1L, "test");
        assertThat(result).isEqualTo(42);
        assertThat(attempts[0]).isEqualTo(3);
    }

    @Test
    void nonRetriableSurfacesImmediately() {
        assertThatThrownBy(() -> RetryUtil.retry(() -> {
            throw new BaseHttpSolrClient.RemoteSolrException(
                    "url", 400, "bad request", null);
        }, 3, 1L, "test")).isInstanceOf(RuntimeException.class);
    }

    @Test
    void classification() {
        assertThat(RetryUtil.isRetriable(new SocketTimeoutException())).isTrue();
        assertThat(RetryUtil.isRetriable(new BaseHttpSolrClient.RemoteSolrException(
                "url", 503, "down", null))).isTrue();
        assertThat(RetryUtil.isRetriable(new BaseHttpSolrClient.RemoteSolrException(
                "url", 400, "bad", null))).isFalse();
    }
}
