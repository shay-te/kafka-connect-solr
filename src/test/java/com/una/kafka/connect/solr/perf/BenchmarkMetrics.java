package com.una.kafka.connect.solr.perf;

import com.sun.management.ThreadMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Per-phase CPU / allocation / GC delta capture for benchmarking.
 *
 * <p>Usage:
 * <pre>{@code
 * BenchmarkMetrics.Snapshot before = BenchmarkMetrics.capture();
 * doWork();
 * BenchmarkMetrics.Delta delta = BenchmarkMetrics.capture().minus(before);
 * System.out.println(delta.toTable("phase-name", wallClockMs));
 * }</pre>
 *
 * <p>Why this exists: wall-clock alone hides where the time goes. CPU vs wall tells
 * us whether we're waiting on I/O or burning cores. Allocated-bytes tells us GC pressure.
 * GC pause time tells us whether stop-the-world cost is dominating. Together they answer
 * "is the bottleneck CPU, GC, or network?" without needing a profiler.
 */
final class BenchmarkMetrics {

    private static final ThreadMXBean THREADS =
            (ThreadMXBean) ManagementFactory.getThreadMXBean();
    private static final List<GarbageCollectorMXBean> GCS =
            ManagementFactory.getGarbageCollectorMXBeans();

    static { THREADS.setThreadAllocatedMemoryEnabled(true); }

    private BenchmarkMetrics() {}

    static Snapshot capture() {
        long cpuNanos = 0L;
        long allocatedBytes = 0L;
        long[] ids = THREADS.getAllThreadIds();
        for (long id : ids) {
            long cpu = THREADS.getThreadCpuTime(id);
            if (cpu > 0) cpuNanos += cpu;
            long alloc = THREADS.getThreadAllocatedBytes(id);
            if (alloc > 0) allocatedBytes += alloc;
        }
        long gcCount = 0L;
        long gcMillis = 0L;
        for (GarbageCollectorMXBean gc : GCS) {
            long c = gc.getCollectionCount();
            long t = gc.getCollectionTime();
            if (c > 0) gcCount += c;
            if (t > 0) gcMillis += t;
        }
        return new Snapshot(cpuNanos, allocatedBytes, gcCount, gcMillis);
    }

    static final class Snapshot {
        final long cpuNanos;
        final long allocatedBytes;
        final long gcCount;
        final long gcMillis;

        Snapshot(long cpuNanos, long allocatedBytes, long gcCount, long gcMillis) {
            this.cpuNanos = cpuNanos;
            this.allocatedBytes = allocatedBytes;
            this.gcCount = gcCount;
            this.gcMillis = gcMillis;
        }

        Delta minus(Snapshot baseline) {
            return new Delta(
                    cpuNanos - baseline.cpuNanos,
                    allocatedBytes - baseline.allocatedBytes,
                    gcCount - baseline.gcCount,
                    gcMillis - baseline.gcMillis);
        }
    }

    static final class Delta {
        final long cpuNanos;
        final long allocatedBytes;
        final long gcCount;
        final long gcMillis;

        Delta(long cpuNanos, long allocatedBytes, long gcCount, long gcMillis) {
            this.cpuNanos = cpuNanos;
            this.allocatedBytes = allocatedBytes;
            this.gcCount = gcCount;
            this.gcMillis = gcMillis;
        }

        String toTable(String phase, long wallMillis) {
            long cpuMs = TimeUnit.NANOSECONDS.toMillis(cpuNanos);
            double cpuPerWall = wallMillis == 0 ? 0.0 : (double) cpuMs / wallMillis;
            double allocMb = allocatedBytes / 1024.0 / 1024.0;
            double allocPerSec = wallMillis == 0 ? 0.0 : allocMb * 1000.0 / wallMillis;
            return String.format(
                    "  %-13s wall=%6d ms | cpu=%6d ms (%.2fx wall) | "
                            + "alloc=%8.1f MB (%.1f MB/s) | gc=%d (%d ms)",
                    phase, wallMillis, cpuMs, cpuPerWall,
                    allocMb, allocPerSec, gcCount, gcMillis);
        }
    }
}
