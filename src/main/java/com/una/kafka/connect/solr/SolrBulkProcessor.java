package com.una.kafka.connect.solr;

import org.apache.kafka.connect.errors.RetriableException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateHttp2SolrClient;
import org.apache.solr.client.solrj.impl.NoOpResponseParser;
import org.apache.solr.client.solrj.ResponseParser;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
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
import java.util.concurrent.atomic.LongAdder;

public final class SolrBulkProcessor implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SolrBulkProcessor.class);
    private static final ResponseParser NO_OP_PARSER = new NoOpResponseParser("javabin");

    private final SolrClient client;
    private final ExecutorService executor;
    private final BlockingQueue<Future<?>> inflight;

    private final int batchSize;
    private final long lingerNanos;
    private final long bulkSizeBytes;
    private final int maxBufferedRecords;
    private final int maxRetries;
    private final long retryBackoffMs;
    private final long flushTimeoutMs;
    private final int commitWithinMs;
    private final boolean dryRun;

    private volatile long lastSuccessEpochMs = 0L;

    private final Map<String, UpsertBuffer> upsertBuffers = new HashMap<>();
    private final Map<String, DeleteBuffer> deleteBuffers = new HashMap<>();
    private long lastFlushNanos = System.nanoTime();

    private String lastUpsertCollection;
    private UpsertBuffer lastUpsertBuffer;
    private String lastDeleteCollection;
    private DeleteBuffer lastDeleteBuffer;

    private final LongAdder recordsWritten = new LongAdder();
    private final LongAdder recordsFailed = new LongAdder();
    private final LongAdder totalRetries = new LongAdder();
    private final LongAdder queueDepth = new LongAdder();

    private final LongAdder batchLatencyNanos = new LongAdder();
    private final LongAdder batchCount = new LongAdder();
    private final LongAdder solrCallLatencyNanos = new LongAdder();
    private final LongAdder solrCallCount = new LongAdder();

    public SolrBulkProcessor(SolrClient client, SolrSinkConfig config) {
        this.client = client;
        this.batchSize = config.batchSize();
        this.lingerNanos = TimeUnit.MILLISECONDS.toNanos(config.lingerMs());
        this.bulkSizeBytes = config.bulkSizeBytes();
        this.maxBufferedRecords = config.maxBufferedRecords();
        this.maxRetries = config.maxRetries();
        this.retryBackoffMs = config.retryBackoffMs();
        this.flushTimeoutMs = config.flushTimeoutMs();
        long cw = config.commitWithinMs();
        this.commitWithinMs = cw > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) cw;
        this.dryRun = config.dryRun();
        if (dryRun) {
            log.warn("DRY RUN MODE: docs will be logged but never sent to Solr");
        }

        int inFlight = Math.max(1, config.maxInFlight());
        this.inflight = new ArrayBlockingQueue<>(inFlight);
        this.executor = new ThreadPoolExecutor(
                inFlight, inFlight,
                30L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(inFlight * 2),
                namedFactory("solr-bulk"),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public void upsert(String collection, SolrInputDocument doc, OffsetState offsetState) {
        UpsertBuffer buf = upsertBufferFor(collection);
        buf.docs.add(doc);
        buf.states.add(offsetState);
        if (bulkSizeBytes > 0) buf.bytes.add(estimateBytes(doc));
        queueDepth.increment();
        maybeFlush(buf);
    }

    public void delete(String collection, String id, OffsetState offsetState) {
        DeleteBuffer buf = deleteBufferFor(collection);
        buf.ids.add(id);
        buf.states.add(offsetState);
        queueDepth.increment();
        maybeFlushDelete(buf);
    }

    private UpsertBuffer upsertBufferFor(String collection) {
        if (collection.equals(lastUpsertCollection)) {
            return lastUpsertBuffer;
        }
        UpsertBuffer buf = upsertBuffers.get(collection);
        if (buf == null) {
            buf = new UpsertBuffer(batchSize);
            upsertBuffers.put(collection, buf);
        }
        lastUpsertCollection = collection;
        lastUpsertBuffer = buf;
        return buf;
    }

    private DeleteBuffer deleteBufferFor(String collection) {
        if (collection.equals(lastDeleteCollection)) {
            return lastDeleteBuffer;
        }
        DeleteBuffer buf = deleteBuffers.get(collection);
        if (buf == null) {
            buf = new DeleteBuffer(batchSize);
            deleteBuffers.put(collection, buf);
        }
        lastDeleteCollection = collection;
        lastDeleteBuffer = buf;
        return buf;
    }

    private void maybeFlush(UpsertBuffer touched) {
        if (touched.docs.size() >= batchSize
                || (bulkSizeBytes > 0 && touched.bytes.sum() >= bulkSizeBytes)) {
            flushAsync();
            return;
        }
        checkGlobalThresholds();
    }

    private void maybeFlushDelete(DeleteBuffer touched) {
        if (touched.ids.size() >= batchSize) {
            flushAsync();
            return;
        }
        checkGlobalThresholds();
    }

    private void checkGlobalThresholds() {
        long age = System.nanoTime() - lastFlushNanos;
        if (age >= lingerNanos || queueDepth.sum() >= maxBufferedRecords) {
            flushAsync();
        }
    }

    public void flushAsync() {
        for (Map.Entry<String, UpsertBuffer> e : upsertBuffers.entrySet()) {
            UpsertBuffer buf = e.getValue();
            if (buf.docs.isEmpty()) continue;
            String collection = e.getKey();
            List<SolrInputDocument> docs = buf.docs;
            List<OffsetState> states = buf.states;
            buf.docs = new ArrayList<>(batchSize);
            buf.states = new ArrayList<>(batchSize);
            buf.bytes.reset();
            submit(() -> sendUpsert(collection, docs, states));
        }
        for (Map.Entry<String, DeleteBuffer> e : deleteBuffers.entrySet()) {
            DeleteBuffer buf = e.getValue();
            if (buf.ids.isEmpty()) continue;
            String collection = e.getKey();
            List<String> ids = buf.ids;
            List<OffsetState> states = buf.states;
            buf.ids = new ArrayList<>(batchSize);
            buf.states = new ArrayList<>(batchSize);
            submit(() -> sendDelete(collection, ids, states));
        }
        lastFlushNanos = System.nanoTime();
    }

    public void flushSync() {
        flushAsync();
        Future<?> f;
        long deadline = System.currentTimeMillis() + flushTimeoutMs;
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
        // CUHTTP2 queues docs internally; block on its drain so reported offsets are acked.
        if (client instanceof ConcurrentUpdateHttp2SolrClient) {
            try {
                ((ConcurrentUpdateHttp2SolrClient) client).blockUntilFinished();
            } catch (Exception e) {
                throw new RetriableException("Solr streaming flush failed", e);
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

    private Void sendUpsert(String collection, List<SolrInputDocument> docs, List<OffsetState> states) {
        long batchStart = System.nanoTime();
        try {
            int size = docs.size();
            if (dryRun) {
                log.info("DRY RUN: would upsert {} docs to collection={} (first id={})",
                        size, collection,
                        size == 0 ? "<empty>" : docs.get(0).getFieldValue("id"));
                recordsWritten.add(size);
                queueDepth.add(-size);
                ackAll(states);
                lastSuccessEpochMs = System.currentTimeMillis();
                return null;
            }
            try {
                return RetryUtil.retry(() -> {
                    UpdateRequest req = new UpdateRequest();
                    req.setResponseParser(NO_OP_PARSER);
                    req.add(docs);
                    if (commitWithinMs > 0) {
                        req.setCommitWithin(commitWithinMs);
                    }
                    long callStart = System.nanoTime();
                    req.process(client, collection);
                    solrCallLatencyNanos.add(System.nanoTime() - callStart);
                    solrCallCount.increment();
                    recordsWritten.add(size);
                    queueDepth.add(-size);
                    ackAll(states);
                    lastSuccessEpochMs = System.currentTimeMillis();
                    return null;
                }, maxRetries, retryBackoffMs,
                        () -> "Solr upsert(" + collection + ", " + size + " docs)",
                        totalRetries);
            } catch (RuntimeException e) {
                recordsFailed.add(size);
                throw e;
            }
        } finally {
            batchLatencyNanos.add(System.nanoTime() - batchStart);
            batchCount.increment();
        }
    }

    private Void sendDelete(String collection, List<String> ids, List<OffsetState> states) {
        long batchStart = System.nanoTime();
        try {
            int size = ids.size();
            if (dryRun) {
                log.info("DRY RUN: would delete {} ids from collection={} (first id={})",
                        size, collection, size == 0 ? "<empty>" : ids.get(0));
                recordsWritten.add(size);
                queueDepth.add(-size);
                ackAll(states);
                lastSuccessEpochMs = System.currentTimeMillis();
                return null;
            }
            try {
                return RetryUtil.retry(() -> {
                    UpdateRequest req = new UpdateRequest();
                    req.setResponseParser(NO_OP_PARSER);
                    req.deleteById(ids);
                    if (commitWithinMs > 0) {
                        req.setCommitWithin(commitWithinMs);
                    }
                    long callStart = System.nanoTime();
                    req.process(client, collection);
                    solrCallLatencyNanos.add(System.nanoTime() - callStart);
                    solrCallCount.increment();
                    recordsWritten.add(size);
                    queueDepth.add(-size);
                    ackAll(states);
                    lastSuccessEpochMs = System.currentTimeMillis();
                    return null;
                }, maxRetries, retryBackoffMs,
                        () -> "Solr delete(" + collection + ", " + size + " ids)",
                        totalRetries);
            } catch (RuntimeException e) {
                recordsFailed.add(size);
                throw e;
            }
        } finally {
            batchLatencyNanos.add(System.nanoTime() - batchStart);
            batchCount.increment();
        }
    }

    private static void ackAll(List<OffsetState> states) {
        for (int i = 0, n = states.size(); i < n; i++) {
            OffsetState s = states.get(i);
            if (s != null) s.markAcked();
        }
    }

    static long estimateBytes(SolrInputDocument doc) {
        long total = 16;
        for (SolrInputField f : doc.values()) {
            total += f.getName().length() * 2L;
            Object v = f.getValue();
            if (v == null) continue;
            if (v instanceof CharSequence) {
                total += ((CharSequence) v).length() * 2L;
            } else if (v instanceof Collection) {
                for (Object item : (Collection<?>) v) {
                    total += item instanceof CharSequence
                            ? ((CharSequence) item).length() * 2L : 16L;
                }
            } else if (v instanceof Number || v instanceof Boolean) {
                total += 16L;
            } else {
                total += 32L;
            }
        }
        return total;
    }

    public long recordsWritten() { return recordsWritten.sum(); }
    public long recordsFailed() { return recordsFailed.sum(); }
    public long retries() { return totalRetries.sum(); }
    public int queueDepth() { return (int) queueDepth.sum(); }

    public double avgBatchLatencyMs() {
        long count = batchCount.sum();
        return count == 0 ? 0.0 : (batchLatencyNanos.sum() / (double) count) / 1_000_000.0;
    }

    public double avgSolrCallLatencyMs() {
        long count = solrCallCount.sum();
        return count == 0 ? 0.0 : (solrCallLatencyNanos.sum() / (double) count) / 1_000_000.0;
    }

    public long batchCount() { return batchCount.sum(); }
    public long solrCallCount() { return solrCallCount.sum(); }
    public long lastSuccessEpochMs() { return lastSuccessEpochMs; }

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

    private static final class UpsertBuffer {
        List<SolrInputDocument> docs;
        List<OffsetState> states;
        final LongAdder bytes = new LongAdder();

        UpsertBuffer(int batchSize) {
            this.docs = new ArrayList<>(batchSize);
            this.states = new ArrayList<>(batchSize);
        }
    }

    private static final class DeleteBuffer {
        List<String> ids;
        List<OffsetState> states;

        DeleteBuffer(int batchSize) {
            this.ids = new ArrayList<>(batchSize);
            this.states = new ArrayList<>(batchSize);
        }
    }
}
