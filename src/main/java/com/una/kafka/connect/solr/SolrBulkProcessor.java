package com.una.kafka.connect.solr;

import org.apache.kafka.connect.errors.RetriableException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateHttp2SolrClient;
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

    private final Map<String, CollectionBuffer> upsertBuffers = new HashMap<>();
    private final Map<String, List<PendingDelete>> deleteBuffers = new HashMap<>();
    private long lastFlushNanos = System.nanoTime();

    private String lastUpsertCollection;
    private CollectionBuffer lastUpsertBuffer;
    private String lastDeleteCollection;
    private List<PendingDelete> lastDeleteBuffer;

    private final LongAdder recordsWritten = new LongAdder();
    private final LongAdder recordsFailed = new LongAdder();
    private final LongAdder totalRetries = new LongAdder();
    private final LongAdder queueDepth = new LongAdder();

    private final LongAdder batchLatencyNanos = new LongAdder();
    private final LongAdder batchCount = new LongAdder();
    private final LongAdder solrCallLatencyNanos = new LongAdder();
    private final LongAdder solrCallCount = new LongAdder();

    public SolrBulkProcessor(SolrClient client, SolrSinkConfig config, SolrSchemaManager schemaManager) {
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
        CollectionBuffer buf = upsertBufferFor(collection);
        buf.docs.add(new Pending(doc, offsetState));
        if (bulkSizeBytes > 0) buf.bytes.add(estimateBytes(doc));
        queueDepth.increment();
        maybeFlush(buf);
    }

    public void delete(String collection, String id, OffsetState offsetState) {
        List<PendingDelete> buf = deleteBufferFor(collection);
        buf.add(new PendingDelete(id, offsetState));
        queueDepth.increment();
        maybeFlushDelete(buf);
    }

    private CollectionBuffer upsertBufferFor(String collection) {
        if (collection.equals(lastUpsertCollection)) {
            return lastUpsertBuffer;
        }
        CollectionBuffer buf = upsertBuffers.get(collection);
        if (buf == null) {
            buf = new CollectionBuffer(batchSize);
            upsertBuffers.put(collection, buf);
        }
        lastUpsertCollection = collection;
        lastUpsertBuffer = buf;
        return buf;
    }

    private List<PendingDelete> deleteBufferFor(String collection) {
        if (collection.equals(lastDeleteCollection)) {
            return lastDeleteBuffer;
        }
        List<PendingDelete> buf = deleteBuffers.get(collection);
        if (buf == null) {
            buf = new ArrayList<>(batchSize);
            deleteBuffers.put(collection, buf);
        }
        lastDeleteCollection = collection;
        lastDeleteBuffer = buf;
        return buf;
    }

    private void maybeFlush(CollectionBuffer touched) {
        if (touched.docs.size() >= batchSize
                || (bulkSizeBytes > 0 && touched.bytes.sum() >= bulkSizeBytes)) {
            flushAsync();
            return;
        }
        checkGlobalThresholds();
    }

    private void maybeFlushDelete(List<PendingDelete> touched) {
        if (touched.size() >= batchSize) {
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
        for (Map.Entry<String, CollectionBuffer> e : upsertBuffers.entrySet()) {
            CollectionBuffer buf = e.getValue();
            if (buf.docs.isEmpty()) continue;
            String collection = e.getKey();
            List<Pending> snapshot = buf.takeAndReset(batchSize);
            submit(() -> sendUpsert(collection, snapshot));
        }
        for (Map.Entry<String, List<PendingDelete>> e : deleteBuffers.entrySet()) {
            List<PendingDelete> dels = e.getValue();
            if (dels.isEmpty()) continue;
            String collection = e.getKey();
            List<PendingDelete> snapshot = dels;
            e.setValue(new ArrayList<>(batchSize));
            submit(() -> sendDelete(collection, snapshot));
        }
        // lastDeleteBuffer would point at the in-flight snapshot after the swap above.
        lastDeleteCollection = null;
        lastDeleteBuffer = null;
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

    private Void sendUpsert(String collection, List<Pending> docs) {
        long batchStart = System.nanoTime();
        try {
            if (dryRun) {
                log.info("DRY RUN: would upsert {} docs to collection={} (first id={})",
                        docs.size(), collection,
                        docs.isEmpty() ? "<empty>" : docs.get(0).doc.getFieldValue("id"));
                recordsWritten.add(docs.size());
                queueDepth.add(-docs.size());
                for (Pending p : docs) {
                    if (p.offsetState != null) p.offsetState.markAcked();
                }
                lastSuccessEpochMs = System.currentTimeMillis();
                return null;
            }
            try {
                return RetryUtil.retry(() -> {
                    UpdateRequest req = new UpdateRequest();
                    List<SolrInputDocument> rawDocs = new ArrayList<>(docs.size());
                    for (Pending p : docs) rawDocs.add(p.doc);
                    req.add(rawDocs);
                    if (commitWithinMs > 0) {
                        req.setCommitWithin(commitWithinMs);
                    }
                    long callStart = System.nanoTime();
                    req.process(client, collection);
                    solrCallLatencyNanos.add(System.nanoTime() - callStart);
                    solrCallCount.increment();
                    recordsWritten.add(docs.size());
                    queueDepth.add(-docs.size());
                    for (Pending p : docs) {
                        if (p.offsetState != null) p.offsetState.markAcked();
                    }
                    lastSuccessEpochMs = System.currentTimeMillis();
                    return null;
                }, maxRetries, retryBackoffMs,
                        "Solr upsert(" + collection + ", " + docs.size() + " docs)",
                        totalRetries);
            } catch (RuntimeException e) {
                recordsFailed.add(docs.size());
                throw e;
            }
        } finally {
            batchLatencyNanos.add(System.nanoTime() - batchStart);
            batchCount.increment();
        }
    }

    private Void sendDelete(String collection, List<PendingDelete> dels) {
        long batchStart = System.nanoTime();
        try {
            if (dryRun) {
                log.info("DRY RUN: would delete {} ids from collection={} (first id={})",
                        dels.size(), collection,
                        dels.isEmpty() ? "<empty>" : dels.get(0).id);
                recordsWritten.add(dels.size());
                queueDepth.add(-dels.size());
                for (PendingDelete p : dels) {
                    if (p.offsetState != null) p.offsetState.markAcked();
                }
                lastSuccessEpochMs = System.currentTimeMillis();
                return null;
            }
            try {
                return RetryUtil.retry(() -> {
                    UpdateRequest req = new UpdateRequest();
                    List<String> ids = new ArrayList<>(dels.size());
                    for (PendingDelete p : dels) ids.add(p.id);
                    req.deleteById(ids);
                    if (commitWithinMs > 0) {
                        req.setCommitWithin(commitWithinMs);
                    }
                    long callStart = System.nanoTime();
                    req.process(client, collection);
                    solrCallLatencyNanos.add(System.nanoTime() - callStart);
                    solrCallCount.increment();
                    recordsWritten.add(dels.size());
                    queueDepth.add(-dels.size());
                    for (PendingDelete p : dels) {
                        if (p.offsetState != null) p.offsetState.markAcked();
                    }
                    lastSuccessEpochMs = System.currentTimeMillis();
                    return null;
                }, maxRetries, retryBackoffMs,
                        "Solr delete(" + collection + ", " + dels.size() + " ids)",
                        totalRetries);
            } catch (RuntimeException e) {
                recordsFailed.add(dels.size());
                throw e;
            }
        } finally {
            batchLatencyNanos.add(System.nanoTime() - batchStart);
            batchCount.increment();
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

    private static final class CollectionBuffer {
        private List<Pending> docs;
        private final LongAdder bytes = new LongAdder();

        CollectionBuffer(int batchSize) {
            this.docs = new ArrayList<>(batchSize);
        }

        List<Pending> takeAndReset(int batchSize) {
            List<Pending> taken = this.docs;
            this.docs = new ArrayList<>(batchSize);
            this.bytes.reset();
            return taken;
        }
    }

    private static final class Pending {
        final SolrInputDocument doc;
        final OffsetState offsetState;
        Pending(SolrInputDocument doc, OffsetState offsetState) {
            this.doc = doc;
            this.offsetState = offsetState;
        }
    }

    private static final class PendingDelete {
        final String id;
        final OffsetState offsetState;
        PendingDelete(String id, OffsetState offsetState) {
            this.id = id;
            this.offsetState = offsetState;
        }
    }
}
