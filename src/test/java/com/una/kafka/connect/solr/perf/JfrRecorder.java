package com.una.kafka.connect.solr.perf;

import jdk.jfr.Configuration;
import jdk.jfr.Recording;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;

/**
 * Per-phase JFR (Java Flight Recorder) capture.
 *
 * <p>Writes one {@code .jfr} file per phase to {@code target/perf-jfr/}. Open with
 * JDK Mission Control or convert to a flamegraph via async-profiler's {@code jfrconv}.
 *
 * <p>Why JFR over async-profiler here: JFR is built into the JDK, needs no native agent,
 * and survives JVM restarts cleanly. Async-profiler is more accurate for CPU sampling
 * but has to be attached externally — see {@code docs/benchmark.md} for that flow.
 *
 * <p>Failure mode: if the JFR config cannot be loaded (e.g. headless build of OpenJDK
 * without JFR), {@link #start(String)} returns a no-op handle so the perf test still runs.
 */
final class JfrRecorder implements AutoCloseable {

    private static final Path OUTPUT_DIR = Paths.get("target", "perf-jfr");

    private final Recording recording;
    private final Path destination;

    private JfrRecorder(Recording recording, Path destination) {
        this.recording = recording;
        this.destination = destination;
    }

    static JfrRecorder start(String phase) {
        try {
            Files.createDirectories(OUTPUT_DIR);
            Configuration config = Configuration.getConfiguration("profile");
            Recording recording = new Recording(config);
            recording.setName("perf-" + phase);
            recording.setMaxAge(Duration.ofHours(1));
            recording.setToDisk(true);
            Path destination = OUTPUT_DIR.resolve(phase + ".jfr");
            recording.setDestination(destination);
            recording.start();
            return new JfrRecorder(recording, destination);
        } catch (Throwable t) {
            System.err.println("[JFR] disabled for phase '" + phase + "': " + t.getMessage());
            return new JfrRecorder(null, null);
        }
    }

    @Override
    public void close() {
        if (recording == null) return;
        try {
            recording.stop();
            recording.close();
            System.out.printf("[JFR] wrote %s%n", destination);
        } catch (Throwable t) {
            System.err.println("[JFR] stop failed: " + t.getMessage());
        }
    }
}
