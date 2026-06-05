package com.una.kafka.connect.solr;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.BaseHttpSolrClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.LongAdder;

public final class RetryUtil {

    private static final Logger log = LoggerFactory.getLogger(RetryUtil.class);
    private static final long MAX_BACKOFF_MS = 30_000L;

    private RetryUtil() {
    }

    public static <T> T retry(Callable<T> action, int maxRetries, long initialBackoffMs, String description) {
        return retry(action, maxRetries, initialBackoffMs, description, null);
    }

    /**
     * Same as {@link #retry(Callable, int, long, String)} but also increments
     * the supplied {@link LongAdder} once per retry attempt. Lets callers
     * expose a "total retries" metric without us tracking it ourselves.
     */
    public static <T> T retry(Callable<T> action, int maxRetries, long initialBackoffMs,
                              String description, LongAdder retriesCounter) {
        int attempt = 0;
        long backoff = initialBackoffMs;
        Exception last = null;
        while (attempt <= maxRetries) {
            try {
                return action.call();
            } catch (Exception e) {
                last = e;
                if (!isRetriable(e)) {
                    if (e instanceof RuntimeException) {
                        throw (RuntimeException) e;
                    }
                    throw new ConnectException(description + " failed", e);
                }
                if (attempt == maxRetries) {
                    break;
                }
                attempt++;
                if (retriesCounter != null) {
                    retriesCounter.increment();
                }
                log.warn("{} attempt {}/{} failed: {}. Retrying in {}ms",
                        description, attempt, maxRetries, e.getMessage(), backoff);
                try {
                    Thread.sleep(backoff);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RetriableException(ie);
                }
                backoff = Math.min(MAX_BACKOFF_MS, backoff * 2);
            }
        }
        throw new RetriableException(description + " exhausted retries", last);
    }

    public static boolean isRetriable(Throwable t) {
        if (t == null) {
            return false;
        }
        if (t instanceof java.net.SocketTimeoutException
                || t instanceof java.net.ConnectException
                || t instanceof java.io.IOException
                || t instanceof SolrServerException) {
            return true;
        }
        if (t instanceof BaseHttpSolrClient.RemoteSolrException) {
            int code = ((BaseHttpSolrClient.RemoteSolrException) t).code();
            return code == 429 || code >= 500;
        }
        return isRetriable(t.getCause());
    }
}
