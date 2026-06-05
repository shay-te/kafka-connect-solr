package com.una.kafka.connect.solr;

import org.apache.kafka.connect.errors.RetriableException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Batches documents per target collection and ships them with bounded
 * concurrency. The shape mirrors Confluent's BulkProcessor but uses a
 * real executor pool against an HTTP/2 client so N batches run truly
 * concurrently on one connection.
 */
public class SolrBulkProcessor implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SolrBulkProcessor.class);

    private final SolrClient client;
    private final SolrSinkConfig config;
    private final ExecutorService executor;
    private final BlockingQueue<Future<?>> inflight;

    private final Map<String, List<SolrInputDocument>> upsertBuffers = new HashMap<>();
    private final Map<String, List<String>> deleteBuffers = new HashMap<>();
    private long lastFlushNanos = System.nanoTime();

    private final AtomicLong recordsWritten = new AtomicLong();
    private final AtomicLong recordsFailed = new AtomicLong();
    private final AtomicLong totalRetries = new AtomicLong();
    private final AtomicInteger queueDepth = new AtomicInteger();

    public SolrBulkProcessor(SolrClient client, SolrSinkConfig config, SolrSchemaManager schemaManager) {
        this.client = client;
        this.config = config;
        this.inflight = new ArrayBlockingQueue<>(Math.max(1, config.maxInFlight()));
        this.executor = new ThreadPoolExecutor(
                config.maxInFlight(),
                config.maxInFlight(),
                30L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(config.maxInFlight() * 2),
                namedFactory("solr-bulk"),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public void upsert(String collection, SolrInputDocument doc) {
        upsertBuffers.computeIfAbsent(collection, k -> new ArrayList<>(config.batchSize())).add(doc);
        queueDepth.incrementAndGet();
        maybeFlush();
    }

    public void delete(String collection, String id) {
        deleteBuffers.computeIfAbsent(collection, k -> new ArrayList<>(config.batchSize())).add(id);
        queueDepth.incrementAndGet();
        maybeFlush();
    }

    private void maybeFlush() {
        boolean batchFull = false;
        for (List<SolrInputDocument> docs : upsertBuffers.values()) {
            if (docs.size() >= config.batchSize()) {
                batchFull = true;
                break;
            }
        }
        long age = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastFlushNanos);
        if (batchFull || age >= config.lingerMs() || queueDepth.get() >= config.maxBufferedRecords()) {
            flushAsync();
        }
    }

    public void flushAsync() {
        for (Map.Entry<String, List<SolrInputDocument>> e : new HashMap<>(upsertBuffers).entrySet()) {
            String collection = e.getKey();
            List<SolrInputDocument> docs = e.getValue();
            if (docs.isEmpty()) {
                continue;
            }
            upsertBuffers.put(collection, new ArrayList<>(config.batchSize()));
            submit(() -> sendUpsert(collection, docs));
        }
        for (Map.Entry<String, List<String>> e : new HashMap<>(deleteBuffers).entrySet()) {
            String collection = e.getKey();
            List<String> ids = e.getValue();
            if (ids.isEmpty()) {
                continue;
            }
            deleteBuffers.put(collection, new ArrayList<>(config.batchSize()));
            submit(() -> sendDelete(collection, ids));
        }
        lastFlushNanos = System.nanoTime();
    }

    public void flushSync() {
        flushAsync();
        Future<?> f;
        long deadline = System.currentTimeMillis() + config.flushTimeoutMs();
        while ((f = inflight.poll()) != null) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                throw new RetriableException("Solr flush timed out");
            }
            try {
                f.get(remaining, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                throw new RetriableException("Solr bulk request failed", e);
            }
        }
    }

    private void submit(Runnable task) {
        try {
            Future<?> f = executor.submit(task);
            inflight.put(f);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RetriableException(ie);
        }
    }

    private Void sendUpsert(String collection, List<SolrInputDocument> docs) {
        return RetryUtil.retry(() -> {
            UpdateRequest req = new UpdateRequest();
            req.add(docs);
            if (config.commitWithinMs() > 0) {
                req.setCommitWithin((int) config.commitWithinMs());
            }
            req.process(client, collection);
            recordsWritten.addAndGet(docs.size());
            queueDepth.addAndGet(-docs.size());
            return null;
        }, config.maxRetries(), config.retryBackoffMs(),
                "Solr upsert(" + collection + ", " + docs.size() + " docs)");
    }

    private Void sendDelete(String collection, List<String> ids) {
        return RetryUtil.retry(() -> {
            UpdateRequest req = new UpdateRequest();
            req.deleteById(ids);
            if (config.commitWithinMs() > 0) {
                req.setCommitWithin((int) config.commitWithinMs());
            }
            req.process(client, collection);
            recordsWritten.addAndGet(ids.size());
            queueDepth.addAndGet(-ids.size());
            return null;
        }, config.maxRetries(), config.retryBackoffMs(),
                "Solr delete(" + collection + ", " + ids.size() + " ids)");
    }

    public long recordsWritten() { return recordsWritten.get(); }
    public long recordsFailed() { return recordsFailed.get(); }
    public long retries() { return totalRetries.get(); }
    public int queueDepth() { return queueDepth.get(); }

    @Override
    public void close() {
        try {
            flushSync();
        } catch (Exception e) {
            log.warn("Failure during final flush: {}", e.getMessage());
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }

    private static ThreadFactory namedFactory(String prefix) {
        AtomicInteger counter = new AtomicInteger();
        return r -> {
            Thread t = new Thread(r, prefix + "-" + counter.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
    }
}
