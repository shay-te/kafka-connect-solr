package com.una.kafka.connect.solr;

import org.apache.kafka.connect.errors.RetriableException;
import org.apache.solr.client.solrj.SolrClient;
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
import java.util.concurrent.atomic.AtomicLong;

/**
 * Concurrent batching engine. Per-collection buffers; flush triggers when
 * <ul>
 *     <li>{@code batch.size} records buffered (record-count cap), OR</li>
 *     <li>{@code bulk.size.bytes} bytes accumulated (byte-size cap, ES connector parity), OR</li>
 *     <li>{@code linger.ms} elapsed since the last flush.</li>
 * </ul>
 *
 * <p>Each pending record carries an {@link OffsetState} so the configured
 * {@link OffsetTracker} can advance Kafka offsets as soon as the matching
 * Solr request acks.</p>
 */
public class SolrBulkProcessor implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SolrBulkProcessor.class);

    private final SolrClient client;
    private final SolrSinkConfig config;
    private final ExecutorService executor;
    private final BlockingQueue<Future<?>> inflight;

    private final Map<String, List<Pending>> upsertBuffers = new HashMap<>();
    private final Map<String, Long> upsertBytes = new HashMap<>();
    private final Map<String, List<PendingDelete>> deleteBuffers = new HashMap<>();
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

    public void upsert(String collection, SolrInputDocument doc, OffsetState offsetState) {
        long bytes = estimateBytes(doc);
        upsertBuffers.computeIfAbsent(collection, k -> new ArrayList<>(config.batchSize()))
                .add(new Pending(doc, offsetState, bytes));
        upsertBytes.merge(collection, bytes, Long::sum);
        queueDepth.incrementAndGet();
        maybeFlush();
    }

    public void delete(String collection, String id, OffsetState offsetState) {
        deleteBuffers.computeIfAbsent(collection, k -> new ArrayList<>(config.batchSize()))
                .add(new PendingDelete(id, offsetState));
        queueDepth.incrementAndGet();
        maybeFlush();
    }

    private void maybeFlush() {
        boolean batchFull = false;
        for (Map.Entry<String, List<Pending>> e : upsertBuffers.entrySet()) {
            if (e.getValue().size() >= config.batchSize()) {
                batchFull = true;
                break;
            }
            long bytes = upsertBytes.getOrDefault(e.getKey(), 0L);
            if (config.bulkSizeBytes() > 0 && bytes >= config.bulkSizeBytes()) {
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
        for (Map.Entry<String, List<Pending>> e : new HashMap<>(upsertBuffers).entrySet()) {
            String collection = e.getKey();
            List<Pending> docs = e.getValue();
            if (docs.isEmpty()) {
                continue;
            }
            upsertBuffers.put(collection, new ArrayList<>(config.batchSize()));
            upsertBytes.put(collection, 0L);
            submit(() -> sendUpsert(collection, docs));
        }
        for (Map.Entry<String, List<PendingDelete>> e : new HashMap<>(deleteBuffers).entrySet()) {
            String collection = e.getKey();
            List<PendingDelete> dels = e.getValue();
            if (dels.isEmpty()) {
                continue;
            }
            deleteBuffers.put(collection, new ArrayList<>(config.batchSize()));
            submit(() -> sendDelete(collection, dels));
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

    private Void sendUpsert(String collection, List<Pending> docs) {
        return RetryUtil.retry(() -> {
            UpdateRequest req = new UpdateRequest();
            List<SolrInputDocument> rawDocs = new ArrayList<>(docs.size());
            for (Pending p : docs) rawDocs.add(p.doc);
            req.add(rawDocs);
            if (config.commitWithinMs() > 0) {
                req.setCommitWithin((int) config.commitWithinMs());
            }
            req.process(client, collection);
            recordsWritten.addAndGet(docs.size());
            queueDepth.addAndGet(-docs.size());
            for (Pending p : docs) {
                if (p.offsetState != null) p.offsetState.markAcked();
            }
            return null;
        }, config.maxRetries(), config.retryBackoffMs(),
                "Solr upsert(" + collection + ", " + docs.size() + " docs)");
    }

    private Void sendDelete(String collection, List<PendingDelete> dels) {
        return RetryUtil.retry(() -> {
            UpdateRequest req = new UpdateRequest();
            List<String> ids = new ArrayList<>(dels.size());
            for (PendingDelete p : dels) ids.add(p.id);
            req.deleteById(ids);
            if (config.commitWithinMs() > 0) {
                req.setCommitWithin((int) config.commitWithinMs());
            }
            req.process(client, collection);
            recordsWritten.addAndGet(dels.size());
            queueDepth.addAndGet(-dels.size());
            for (PendingDelete p : dels) {
                if (p.offsetState != null) p.offsetState.markAcked();
            }
            return null;
        }, config.maxRetries(), config.retryBackoffMs(),
                "Solr delete(" + collection + ", " + dels.size() + " ids)");
    }

    /**
     * Rough on-wire size estimate. Used only as a flush trigger so a
     * cheap approximation is fine.
     */
    static long estimateBytes(SolrInputDocument doc) {
        long total = 16; // doc envelope overhead
        for (SolrInputField f : doc.values()) {
            total += f.getName().length() * 2L; // UTF-8 worst case
            Object v = f.getValue();
            if (v == null) continue;
            if (v instanceof CharSequence) {
                total += ((CharSequence) v).length() * 2L;
            } else if (v instanceof Collection) {
                for (Object item : (Collection<?>) v) {
                    total += item == null ? 0 : String.valueOf(item).length() * 2L;
                }
            } else if (v instanceof Map) {
                total += String.valueOf(v).length() * 2L;
            } else {
                total += 16; // primitive
            }
        }
        return total;
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

    private static final class Pending {
        final SolrInputDocument doc;
        final OffsetState offsetState;
        final long bytes;
        Pending(SolrInputDocument doc, OffsetState offsetState, long bytes) {
            this.doc = doc;
            this.offsetState = offsetState;
            this.bytes = bytes;
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
