package com.una.kafka.connect.solr;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.concurrent.atomic.LongAdder;

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
    void exhaustionThrowsRetriable() {
        assertThatThrownBy(() -> RetryUtil.retry(() -> {
            throw new IOException("forever");
        }, 2, 1L, "test")).isInstanceOf(RetriableException.class);
    }

    @Test
    void nonRetriableSurfacesImmediately() {
        assertThatThrownBy(() -> RetryUtil.retry(() -> {
            throw new BaseHttpSolrClient.RemoteSolrException(
                    "url", 400, "bad request", null);
        }, 3, 1L, "test")).isInstanceOf(RuntimeException.class);
    }

    @Test
    void runtimeExceptionFromNonRetriablePropagatesAsIs() {
        IllegalStateException ise = new IllegalStateException("nope");
        assertThatThrownBy(() -> RetryUtil.retry(() -> { throw ise; }, 3, 1L, "test"))
                .isSameAs(ise);
    }

    @Test
    void checkedNonRetriableWrappedAsConnectException() {
        assertThatThrownBy(() -> RetryUtil.retry(() -> {
            throw new Exception("checked");
        }, 0, 1L, "test")).isInstanceOf(ConnectException.class);
    }

    @Test
    void longAdderOverloadCountsRetries() {
        LongAdder retries = new LongAdder();
        final int[] attempts = {0};
        RetryUtil.retry(() -> {
            attempts[0]++;
            if (attempts[0] < 3) {
                throw new IOException("boom");
            }
            return 42;
        }, 5, 1L, "test", retries);
        // Two retries happened (attempts 2 and 3 succeeded after 1 and 2 failures).
        assertThat(retries.sum()).isEqualTo(2L);
    }

    @Test
    void longAdderOverloadIsNullSafe() {
        // Passing a null counter must work exactly like the 4-arg overload.
        Integer result = RetryUtil.retry(() -> 1, 0, 1L, "test", null);
        assertThat(result).isEqualTo(1);
    }

    @Test
    void classification() {
        assertThat(RetryUtil.isRetriable(new SocketTimeoutException())).isTrue();
        assertThat(RetryUtil.isRetriable(new java.net.ConnectException())).isTrue();
        assertThat(RetryUtil.isRetriable(new IOException())).isTrue();
        assertThat(RetryUtil.isRetriable(new SolrServerException("e"))).isTrue();
        assertThat(RetryUtil.isRetriable(new BaseHttpSolrClient.RemoteSolrException(
                "url", 429, "rate", null))).isTrue();
        assertThat(RetryUtil.isRetriable(new BaseHttpSolrClient.RemoteSolrException(
                "url", 503, "down", null))).isTrue();
        assertThat(RetryUtil.isRetriable(new BaseHttpSolrClient.RemoteSolrException(
                "url", 400, "bad", null))).isFalse();
        assertThat(RetryUtil.isRetriable(null)).isFalse();
        assertThat(RetryUtil.isRetriable(new IllegalStateException())).isFalse();
        // Causes are walked.
        RuntimeException wrapped = new RuntimeException("outer", new IOException("inner"));
        assertThat(RetryUtil.isRetriable(wrapped)).isTrue();
    }
}
