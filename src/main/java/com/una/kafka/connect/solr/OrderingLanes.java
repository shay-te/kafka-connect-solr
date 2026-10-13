package com.una.kafka.connect.solr;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Per-document ordering lanes: {@code max.in.flight.requests > 1} WITHOUT the Solr-side version
 * guard ({@code kafka.offset.version.field} + DocBasedVersionConstraints).
 *
 * <p>Every operation goes to lane {@code hash(id) % lanes}. Each lane owns ONE thread, so its
 * requests run one at a time in submission (Kafka offset) order; different lanes run in parallel.
 * Two writes for the same id always share a lane, so an older one can never land after a newer
 * one — the only ordering in-flight concurrency could otherwise break.
 *
 * <p>Self-contained on purpose. {@link SolrBulkProcessor} only asks {@link #laneFor} and
 * {@link #executor}; with {@code ordering.lanes.enabled=false} (the default) this class is never
 * built and the processor runs its original shared pool. Going back to plain in-flight is flipping
 * that setting — or deleting this class and those two call sites.
 *
 * <p>Retries and failures behave as at {@code max.in.flight.requests=1}: a lane's later requests
 * still run after a failed one, and the failed records' offsets stay un-acked, so Connect replays
 * from the earliest of them in Kafka order and each id converges on its newest version.
 */
final class OrderingLanes implements AutoCloseable {

    private static final long CLOSE_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(10);

    private final ExecutorService[] executors;

    OrderingLanes(int lanes) {
        if (lanes < 2) {
            throw new IllegalArgumentException("ordering lanes need at least 2 lanes, got " + lanes);
        }
        executors = new ExecutorService[lanes];
        for (int i = 0; i < lanes; i++) {
            // One thread for the lane's whole life: tasks arrive through submit(), whose FutureTask
            // catches what they throw, so the thread is never replaced.
            String name = "solr-lane-" + i;
            executors[i] = Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, name);
                t.setDaemon(true);
                return t;
            });
        }
    }

    int count() {
        return executors.length;
    }

    /**
     * The lane for a document id. {@code String.valueOf} so an upsert whose id is a number and a
     * delete of the same id as a string ("12345") always meet in one lane.
     */
    int laneFor(Object id) {
        return id == null ? 0 : Math.floorMod(String.valueOf(id).hashCode(), executors.length);
    }

    ExecutorService executor(int lane) {
        return executors[lane];
    }

    @Override
    public void close() {
        close(CLOSE_TIMEOUT_NANOS);
    }

    /** Drain every lane within ONE shared deadline, then stop whatever is still running. */
    void close(long timeoutNanos) {
        for (ExecutorService e : executors) {
            e.shutdown();
        }
        long deadline = System.nanoTime() + timeoutNanos;
        try {
            for (ExecutorService e : executors) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0 || !e.awaitTermination(remaining, TimeUnit.NANOSECONDS)) {
                    e.shutdownNow();
                }
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            for (ExecutorService e : executors) {
                e.shutdownNow();
            }
        }
    }
}
