package com.una.kafka.connect.solr;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.text.ParseException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Real control-flow through {@link RetryUtil} with genuine exception types — retriable
 * (IOException) vs not (checked ParseException, runtime IllegalState), exhaustion, the
 * retry counter, and interruption during back-off. No mocks: real Callables that throw.
 */
class RetryUtilRealDataTest {

    @Test
    void retriesTransientIoErrorsThenSucceeds() {
        AtomicInteger calls = new AtomicInteger();
        LongAdder retries = new LongAdder();
        String result = RetryUtil.retry(() -> {
            if (calls.incrementAndGet() < 3) throw new IOException("transient");
            return "ok";
        }, 5, 1L, () -> "op", retries);

        assertThat(result).isEqualTo("ok");
        assertThat(calls.get()).isEqualTo(3);
        assertThat(retries.sum()).isEqualTo(2); // two retries before the 3rd call succeeded
    }

    @Test
    void exhaustsRetriesAndThrowsRetriableWithLastCause() {
        AtomicInteger calls = new AtomicInteger();
        assertThatThrownBy(() -> RetryUtil.retry(() -> {
            calls.incrementAndGet();
            throw new IOException("still down");
        }, 2, 1L, "op"))
                .isInstanceOf(RetriableException.class)
                .hasRootCauseInstanceOf(IOException.class);
        assertThat(calls.get()).isEqualTo(3); // initial + 2 retries
    }

    @Test
    void nonRetriableRuntimeExceptionRethrownAsIs() {
        assertThatThrownBy(() -> RetryUtil.retry(() -> {
            throw new IllegalStateException("bad state");
        }, 3, 1L, "op"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("bad state");
    }

    @Test
    void nonRetriableCheckedExceptionWrappedInConnectException() {
        assertThatThrownBy(() -> RetryUtil.retry(() -> {
            throw new ParseException("cannot parse", 4);
        }, 3, 1L, "op"))
                .isInstanceOf(ConnectException.class)
                .hasRootCauseInstanceOf(ParseException.class);
    }

    @Test
    void nestedRetriableCauseIsUnwrapped() {
        // A RuntimeException whose CAUSE is an IOException is retriable via cause recursion.
        AtomicInteger calls = new AtomicInteger();
        String r = RetryUtil.retry(() -> {
            if (calls.incrementAndGet() < 2) throw new RuntimeException("wrap", new IOException("io"));
            return "recovered";
        }, 3, 1L, "op");
        assertThat(r).isEqualTo("recovered");
        assertThat(calls.get()).isEqualTo(2);
    }

    @Test
    void interruptionDuringBackoffThrowsRetriableAndPreservesInterruptFlag() {
        assertThatThrownBy(() -> RetryUtil.retry(() -> {
            Thread.currentThread().interrupt();     // so the next Thread.sleep throws immediately
            throw new IOException("transient");
        }, 3, 50L, "op"))
                .isInstanceOf(RetriableException.class);
        assertThat(Thread.interrupted()).isTrue(); // flag was re-set by RetryUtil; clear it here
    }
}
