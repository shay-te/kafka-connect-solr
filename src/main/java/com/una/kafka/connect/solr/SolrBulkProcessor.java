package com.una.kafka.connect.solr;

import com.una.kafka.connect.solr.SolrSinkConfig.BehaviorOnMalformed;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateHttp2SolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;

/**
 * Buffers upserts and deletes per collection and flushes them to Solr in batches.
 *
 * <p><b>Apply-order guarantee:</b> each collection keeps ONE ordered buffer of operations in
 * record-insertion (Kafka offset) order. Because a single SolrJ {@link UpdateRequest} does not
 * guarantee relative order between {@code add} docs and {@code deleteById} ids, a flush splits
 * the ordered buffer into consecutive same-type <em>runs</em> ([add,add,del,add] &rarr;
 * add-batch, delete-batch, add-batch) and one flush task per collection sends those runs
 * <em>sequentially</em> in that order. So [delete@N, create@N+1] for the same key is always
 * applied as delete-then-create <em>within one flush</em>.
 *
 * <p><b>Scope of that guarantee:</b> it holds ACROSS flushes only when
 * {@code max.in.flight.requests=1} (the default), because two successive flushes of the same
 * collection are separate tasks and would otherwise run concurrently. With
 * {@code max.in.flight.requests>1} you MUST also set {@code kafka.offset.version.field} and a
 * Solr DocBasedVersionConstraints processor on it, or {@code ordering.lanes.enabled=true} —
 * {@link Validator} rejects the combination otherwise. Different collections may always flush
 * concurrently.
 *
 * <p><b>Lanes</b> ({@link OrderingLanes}): each collection's buffer is split into one buffer per
 * lane, an op goes to the lane of its document id, and a lane's flushes run one at a time on its own
 * thread. The guarantee above then holds per document across flushes at any in-flight level. Off
 * (the default), there is exactly one buffer per collection on the shared pool.
 */
public final class SolrBulkProcessor implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(SolrBulkProcessor.class);

    private final SolrClient client;
    private final ExecutorService executor;
    // Per-document lanes when ordering.lanes.enabled and max.in.flight.requests > 1. Null otherwise,
    // and then every flush runs on `executor` exactly as it did before lanes existed.
    private final OrderingLanes lanes;
    // Back-pressure: acquired before each submit, released by the worker when its
    // task completes. Caps concurrent in-flight requests at maxInFlight.
    private final Semaphore inflightSlots;
    // Tracks futures for flushSync to wait on. Unbounded - the Semaphore bounds in-flight,
    // not this queue. Workers do NOT remove their own futures from here; flushSync drains.
    private final Queue<Future<?>> inflight = new ConcurrentLinkedQueue<>();
    // First failure reaped off an already-completed future (see reapCompletedFutures). Without
    // this, a worker that failed BEFORE flushSync ran would be silently dropped by the reap and
    // flushSync would report success — in sync mode that commits offsets past the lost batch.
    private final AtomicReference<Throwable> reapedFailure = new AtomicReference<>();

    private final int batchSize;
    private final long lingerNanos;
    private final long bulkSizeBytes;
    private final int maxBufferedRecords;
    private final int maxRetries;
    private final long retryBackoffMs;
    private final long flushTimeoutMs;
    private final int commitWithinMs;
    private final boolean dryRun;
    // Governs FLUSH-TIME poison docs too: a doc that converts fine but that Solr rejects
    // non-retriably (400 schema conflict etc.) is isolated instead of killing the task
    // when this is WARN or IGNORE. FAIL keeps the historical throw-on-rejection behavior.
    private final BehaviorOnMalformed behaviorOnMalformed;

    private volatile long lastSuccessEpochMs = 0L;

    // ONE ordered buffer per collection lane (one lane unless lanes are on): upserts and deletes
    // share it in insertion order.
    private final Map<String, OpBuffer[]> buffers = new HashMap<>();
    private long lastFlushNanos = System.nanoTime();

    private String lastCollection;
    private OpBuffer[] lastBuffers;

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
        this.behaviorOnMalformed = config.behaviorOnMalformed();
        this.dryRun = config.dryRun();
        if (dryRun) {
            log.warn("DRY RUN MODE: docs will be logged but never sent to Solr");
        }

        int inFlight = Math.max(1, config.maxInFlight());
        this.lanes = config.orderingLanesEnabled() && inFlight > 1 ? new OrderingLanes(inFlight) : null;
        // With lanes a submitted flush can wait behind its own lane's running one while holding a
        // permit; twice the lanes keeps one busy lane from starving the others. Concurrency is still
        // capped at inFlight: each lane has one thread.
        this.inflightSlots = new Semaphore(lanes == null ? inFlight : inFlight * 2);
        this.executor = new ThreadPoolExecutor(
                inFlight, inFlight,
                30L, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(inFlight * 2),
                namedFactory("solr-bulk"),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public void upsert(String collection, SolrInputDocument doc, OffsetState offsetState) {
        upsert(collection, doc, -1L, offsetState);
    }

    /**
     * Same as {@link #upsert(String, SolrInputDocument, OffsetState)} but accepts a pre-computed
     * byte estimate from the converter — when present and {@code bulk.size.bytes > 0} we skip the
     * second-pass {@link #estimateBytes} walk of the doc.
     *
     * @param preComputedBytes a non-negative byte estimate or {@code -1} to compute on demand.
     */
    public void upsert(String collection, SolrInputDocument doc, long preComputedBytes, OffsetState offsetState) {
        OpBuffer buf = bufferFor(collection, doc.getFieldValue("id"));
        Run run = buf.runFor(false);
        run.docs.add(doc);
        run.states.add(offsetState);
        buf.size++;
        if (bulkSizeBytes > 0) {
            buf.bytes += preComputedBytes >= 0 ? preComputedBytes : estimateBytes(doc);
        }
        queueDepth.increment();
        maybeFlush(collection, buf);
    }

    public void delete(String collection, String id, OffsetState offsetState) {
        OpBuffer buf = bufferFor(collection, id);
        Run run = buf.runFor(true);
        run.ids.add(id);
        run.states.add(offsetState);
        buf.size++;
        queueDepth.increment();
        maybeFlush(collection, buf);
    }

    /** The buffer for this collection and, when lanes are on, the lane of document {@code id}. */
    private OpBuffer bufferFor(String collection, Object id) {
        OpBuffer[] laneBuffers;
        if (collection.equals(lastCollection)) {
            laneBuffers = lastBuffers;
        } else {
            laneBuffers = buffers.get(collection);
            if (laneBuffers == null) {
                laneBuffers = new OpBuffer[lanes == null ? 1 : lanes.count()];
                for (int i = 0; i < laneBuffers.length; i++) {
                    laneBuffers[i] = new OpBuffer(i);
                }
                buffers.put(collection, laneBuffers);
            }
            lastCollection = collection;
            lastBuffers = laneBuffers;
        }
        return lanes == null ? laneBuffers[0] : laneBuffers[lanes.laneFor(id)];
    }

    private void maybeFlush(String collection, OpBuffer touched) {
        // Per-buffer threshold: flush ONLY this collection's buffer. Avoids dragging
        // every other collection's small batch out as a tiny Solr request when one
        // collection is hot. Global thresholds (linger / maxBufferedRecords) below
        // still fire flushAsync() for everyone. Thresholds apply to the COMBINED
        // ordered buffer (upserts + deletes together).
        if (touched.size >= batchSize
                || (bulkSizeBytes > 0 && touched.bytes >= bulkSizeBytes)) {
            flushOne(collection, touched);
            return;
        }
        checkGlobalThresholds();
    }

    private void checkGlobalThresholds() {
        // queueDepth.sum() is a cheap LongAdder read; nanoTime is ~5× more expensive.
        // At high throughput maxBufferedRecords trips long before lingerNanos, so checking
        // it first lets us short-circuit out without the nanoTime call.
        if (queueDepth.sum() >= maxBufferedRecords) {
            flushBuffers();
            return;
        }
        if (System.nanoTime() - lastFlushNanos >= lingerNanos) {
            flushBuffers();
        }
    }

    /**
     * The preCommit flush: sends every buffer, then surfaces a failed batch. Only preCommit may
     * surface it — thrown from put(), Connect retries the same batch without rewinding and the
     * failure is gone, so the next preCommit would commit past the lost records.
     */
    public void flushAsync() {
        flushBuffers();
        throwIfReaped();
    }

    /** Sends every buffer. A failure is only recorded here, never thrown or cleared. */
    private void flushBuffers() {
        // Drop finished futures BEFORE adding new ones; in async mode nothing else drains them.
        reapCompletedFutures();
        for (Map.Entry<String, OpBuffer[]> e : buffers.entrySet()) {
            for (OpBuffer buf : e.getValue()) {
                flushOne(e.getKey(), buf);
            }
        }
        lastFlushNanos = System.nanoTime();
    }

    /** True while any collection buffer holds records not yet handed to a worker. */
    public boolean hasBuffered() {
        for (OpBuffer[] laneBuffers : buffers.values()) {
            for (OpBuffer buf : laneBuffers) {
                if (buf.size > 0) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Enforce {@code linger.ms} when no record arrives (called from an idle {@code put()}); without
     * it a partial batch waited for the next offset flush, 60 s by default. Never throws a failed
     * batch — see {@link #flushAsync()}.
     */
    public void flushIfLingerElapsed() {
        if (hasBuffered() && System.nanoTime() - lastFlushNanos >= lingerNanos) {
            flushBuffers();
        }
    }

    private void throwIfReaped() {
        Throwable reaped = reapedFailure.getAndSet(null);
        if (reaped != null) {
            throw asFlushError(reaped);
        }
    }

    /**
     * Remove already-completed futures from the inflight queue. Counters and offset acking were
     * already handled by the worker, but a FAILURE must not vanish with the future: the first
     * one is kept in {@link #reapedFailure} so the next {@link #flushSync()} still surfaces it
     * (otherwise a fast-failing batch would be reaped before flushSync waits on it and sync-mode
     * preCommit would commit offsets past the lost records).
     */
    private void reapCompletedFutures() {
        for (java.util.Iterator<Future<?>> it = inflight.iterator(); it.hasNext(); ) {
            Future<?> f = it.next();
            if (!f.isDone()) {
                continue;
            }
            try {
                f.get();
            } catch (java.util.concurrent.ExecutionException ee) {
                reapedFailure.compareAndSet(null, ee.getCause() != null ? ee.getCause() : ee);
            } catch (java.util.concurrent.CancellationException ce) {
                reapedFailure.compareAndSet(null, ce);
            } catch (InterruptedException ie) {
                // isDone() means get() cannot block; purely defensive.
                Thread.currentThread().interrupt();
            }
            it.remove();
        }
    }

    /** Test hook: current inflight-future queue depth (must stay bounded in async mode). */
    int inflightQueueSize() {
        return inflight.size();
    }

    /** Test hook: lanes in use — 1 means the original shared-pool path. */
    int laneCount() {
        return lanes == null ? 1 : lanes.count();
    }

    /**
     * Targeted flush of a single collection's ordered op buffer. The buffer's consecutive
     * same-type runs are sent SEQUENTIALLY by one flush task, preserving Kafka-offset apply
     * order between upserts and deletes for this collection. Does NOT update
     * {@code lastFlushNanos} — the linger.ms clock is global and must keep ticking
     * so cold buffers eventually get drained by {@link #checkGlobalThresholds()}.
     */
    private void flushOne(String collection, OpBuffer buf) {
        if (buf.size == 0) return;
        List<Run> runs = buf.runs;
        int size = buf.size;
        long bytes = buf.bytes;
        buf.runs = new ArrayList<>();
        buf.size = 0;
        buf.bytes = 0L;
        try {
            submit(() -> sendRuns(collection, runs), buf.lane);
        } catch (RuntimeException e) {
            // Put them back: dropped here, preCommit would commit past records that were never sent.
            buf.runs = runs;
            buf.size = size;
            buf.bytes = bytes;
            throw e;
        }
    }

    /**
     * Sends this collection's runs in insertion order. A failed run aborts the remaining
     * runs — sending them anyway would apply them out of order relative to the failed run's
     * records when Connect re-delivers; their offsets are simply never acked, so at-least-once
     * re-delivery covers them.
     */
    private void sendRuns(String collection, List<Run> runs) {
        for (int i = 0, n = runs.size(); i < n; i++) {
            Run run = runs.get(i);
            if (run.delete) {
                sendDelete(collection, run.ids, run.states);
            } else {
                sendUpsert(collection, run.docs, run.states);
            }
        }
    }

    public void flushSync() {
        // flushAsync already reaps and rethrows a worker that failed before this flush ran, so the
        // failure is never silently committed over. Early exit leaves remaining futures in
        // flight — same at-least-once story as a mid-drain failure below.
        flushAsync();
        Future<?> f;
        // Monotonic: a wall-clock jump (NTP) made a healthy flush time out and rewind.
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(flushTimeoutMs);
        while ((f = inflight.poll()) != null) {
            long remaining = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
            if (remaining <= 0) {
                throw new RetriableException("Solr flush timed out");
            }
            try {
                f.get(remaining, TimeUnit.MILLISECONDS);
            } catch (java.util.concurrent.TimeoutException e) {
                // A hung Solr is transient, exactly like a deadline already spent: retry, never fail the task.
                throw new RetriableException("Solr flush timed out", e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RetriableException("Solr flush interrupted", e);
            } catch (Exception e) {
                // Early exit on first failure leaves remaining futures in flight.
                // Their permits release as workers finish; at-least-once is preserved
                // because Connect re-delivers on the next preCommit retry.
                Throwable cause = (e instanceof java.util.concurrent.ExecutionException && e.getCause() != null)
                        ? e.getCause() : e;
                throw asFlushError(cause);
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

    private static RuntimeException asFlushError(Throwable cause) {
        if (cause instanceof RetriableException || RetryUtil.isRetriable(cause)) {
            return new RetriableException("Solr bulk request failed", cause);
        }
        // Permanent rejection (e.g. Solr 400 — bad document / schema-type conflict).
        // Retrying can NEVER succeed, so do not wrap it as retriable: that would loop the
        // whole pipeline forever on one poison-pill record. Surface a non-retriable error
        // so Kafka Connect fails fast / routes it via errors.tolerance + DLQ.
        return new ConnectException(
                "Solr rejected a document (non-retriable): " + cause.getMessage(), cause);
    }

    private void submit(Runnable task, int lane) {
        try {
            inflightSlots.acquire();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new RetriableException(ie);
        }
        Runnable wrapped = () -> {
            try {
                task.run();
            } finally {
                inflightSlots.release();
            }
        };
        try {
            inflight.add((lanes == null ? executor : lanes.executor(lane)).submit(wrapped));
        } catch (RuntimeException e) {
            // submit failed (e.g. RejectedExecutionException on shutdown) - the
            // wrapped runnable will never run, so release the permit ourselves.
            inflightSlots.release();
            throw e;
        }
    }

    // A run is only ever created to hold the op being added (see OpBuffer.runFor), so it is never empty.
    private Void sendUpsert(String collection, List<SolrInputDocument> docs, List<OffsetState> states) {
        return sendRun(collection, "upsert", docs.size(), "docs", docs.get(0).getFieldValue("id"), states,
                req -> req.add(docs), e -> isolatePoisonRun(collection, docs, null, states, e));
    }

    private Void sendDelete(String collection, List<String> ids, List<OffsetState> states) {
        return sendRun(collection, "delete", ids.size(), "ids", ids.get(0), states,
                req -> req.deleteById(ids), e -> isolatePoisonRun(collection, null, ids, states, e));
    }

    /**
     * One run to Solr: logged only in dry-run, otherwise sent with the retry policy. A non-retriable
     * rejection under WARN/IGNORE means at least one op in the run is poison, so it is isolated op by
     * op instead of freezing all indexing on it.
     */
    private Void sendRun(String collection, String operation, int size, String unit, Object firstId,
                         List<OffsetState> states, Consumer<UpdateRequest> fill,
                         Consumer<RuntimeException> isolatePoison) {
        long batchStart = System.nanoTime();
        try {
            if (dryRun) {
                log.info("DRY RUN: would {} {} {} in collection={} (first id={})",
                        operation, size, unit, collection, firstId);
                markWritten(size, states);
                return null;
            }
            try {
                return RetryUtil.retry(() -> {
                    process(collection, fill);
                    markWritten(size, states);
                    return null;
                }, maxRetries, retryBackoffMs,
                        () -> "Solr " + operation + "(" + collection + ", " + size + " " + unit + ")",
                        totalRetries);
            } catch (RuntimeException e) {
                if (e instanceof RetriableException || behaviorOnMalformed == BehaviorOnMalformed.FAIL) {
                    failed(size);
                    throw e;
                }
                isolatePoison.accept(e);
                return null;
            }
        } finally {
            batchLatencyNanos.add(System.nanoTime() - batchStart);
            batchCount.increment();
        }
    }

    /** One timed, counted Solr update call; `fill` adds the documents or the deletes. */
    private void process(String collection, Consumer<UpdateRequest> fill) throws SolrServerException, IOException {
        UpdateRequest req = new UpdateRequest();
        fill.accept(req);
        if (commitWithinMs > 0) {
            req.setCommitWithin(commitWithinMs);
        }
        long callStart = System.nanoTime();
        req.process(client, collection);
        solrCallLatencyNanos.add(System.nanoTime() - callStart);
        solrCallCount.increment();
    }

    private void markWritten(int size, List<OffsetState> states) {
        recordsWritten.add(size);
        queueDepth.add(-size);
        ackAll(states);
        lastSuccessEpochMs = System.currentTimeMillis();
    }

    /**
     * Fallback for a run Solr rejected NON-retriably while behavior.on.malformed.documents is
     * WARN or IGNORE: re-send the run one op at a time (docs one-by-one, deletes id-by-id) so
     * only the op(s) Solr actually rejects are dropped — logged per the policy, counted in
     * {@code recordsFailed} and ACKED so the pipeline moves past them — while every healthy op
     * still lands, in the original order. A retriable failure mid-isolation counts the
     * unprocessed remainder failed and propagates; those offsets stay un-acked so Connect
     * redelivers them (at-least-once).
     *
     * <p>Exactly one of {@code docs} / {@code ids} is non-null (upsert run vs delete run).
     */
    private void isolatePoisonRun(String collection, List<SolrInputDocument> docs,
                                  List<String> ids, List<OffsetState> states,
                                  RuntimeException rejection) {
        boolean delete = docs == null;
        int size = delete ? ids.size() : docs.size();
        String runKind = delete ? "delete run" : "upsert run";
        if (behaviorOnMalformed == BehaviorOnMalformed.WARN) {
            log.warn("Solr rejected a {} of {} ops (collection={}): {} — re-sending op-by-op "
                    + "to isolate the poison document(s)", runKind, size, collection, rejection.getMessage());
        } else {
            log.debug("Solr rejected a {} of {} ops (collection={}): {} — isolating op-by-op",
                    runKind, size, collection, rejection.getMessage());
        }
        for (int i = 0; i < size; i++) {
            try {
                sendOne(collection, delete ? null : docs.get(i), delete ? ids.get(i) : null, states.get(i));
            } catch (RetriableException re) {
                failed(size - i);
                throw re;
            }
        }
    }

    /**
     * Count ops as failed AND drop them from the buffered-record gauge. queueDepth is incremented
     * once per buffered op and only ever decremented on a SUCCESSFUL send, so without this a single
     * exhausted-retry batch leaves the gauge permanently above {@code max.buffered.records} —
     * {@link #checkGlobalThresholds()} then fires a flush for EVERY subsequent record, collapsing
     * batching into one Solr request per document for the rest of the task's life.
     */
    private void failed(int count) {
        recordsFailed.add(count);
        queueDepth.add(-count);
    }

    /**
     * Sends a single op with the standard retry policy. A non-retriable rejection here pins the
     * poison op: it is dropped per the malformed policy (WARN logs it, IGNORE stays silent),
     * counted failed and its offset acked so one bad document cannot freeze all Solr indexing.
     */
    private void sendOne(String collection, SolrInputDocument doc, String deleteId, OffsetState state) {
        try {
            RetryUtil.retry(() -> {
                process(collection, req -> {
                    if (doc != null) {
                        req.add(doc);
                    } else {
                        req.deleteById(deleteId);
                    }
                });
                return null;
            }, maxRetries, retryBackoffMs,
                    () -> "Solr isolated " + (doc == null ? "delete" : "upsert") + "(" + collection + ", 1 op)",
                    totalRetries);
            recordsWritten.increment();
            queueDepth.decrement();
            if (state != null) state.markAcked();
            lastSuccessEpochMs = System.currentTimeMillis();
        } catch (RetriableException re) {
            throw re;
        } catch (RuntimeException poison) {
            if (behaviorOnMalformed == BehaviorOnMalformed.WARN) {
                log.warn("Dropping {} rejected by Solr (collection={}): {}",
                        doc == null ? "delete of id=" + deleteId : "document id=" + doc.getFieldValue("id"),
                        collection, poison.getMessage());
            }
            recordsFailed.increment();
            queueDepth.decrement();
            if (state != null) state.markAcked();
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
        if (lanes != null) {
            lanes.close();
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

    /**
     * Per-collection ordered operation buffer. Ops are stored as consecutive same-type
     * {@link Run}s in insertion order; a new run starts whenever the op type flips
     * (upsert&harr;delete). All counters are single-task-thread access (Kafka Connect
     * contract) — plain fields, no atomics needed.
     */
    private static final class OpBuffer {
        /** The lane this buffer flushes on; always 0 without lanes. */
        final int lane;
        List<Run> runs = new ArrayList<>();
        /** Total buffered ops (upserts + deletes) — batch.size applies to this. */
        int size;
        /** Estimated buffered upsert bytes — bulk.size.bytes applies to this. */
        long bytes;

        OpBuffer(int lane) {
            this.lane = lane;
        }

        /** The run to append the next op of the given type to, extending or starting one. */
        Run runFor(boolean delete) {
            int n = runs.size();
            if (n > 0) {
                Run last = runs.get(n - 1);
                if (last.delete == delete) {
                    return last;
                }
            }
            Run run = new Run(delete);
            runs.add(run);
            return run;
        }
    }

    /** A maximal sequence of consecutive same-type ops; sent to Solr as one UpdateRequest. */
    private static final class Run {
        final boolean delete;
        final List<SolrInputDocument> docs;
        final List<String> ids;
        final List<OffsetState> states = new ArrayList<>();

        Run(boolean delete) {
            this.delete = delete;
            this.docs = delete ? null : new ArrayList<>();
            this.ids = delete ? new ArrayList<>() : null;
        }
    }
}
