package com.una.kafka.connect.solr;

import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.stream.Stream;

/**
 * Boots a real, in-process Solr ({@link EmbeddedSolrServer}) from the {@code embedded-solr}
 * configset on the test classpath — a genuine Solr core, no Docker, no HTTP. Used to exercise
 * the real write / flush / delete paths of {@link SolrBulkProcessor} and {@link SolrWriter}.
 */
final class EmbeddedSolrSupport {

    private EmbeddedSolrSupport() {}

    static final String CORE = "users";

    /** Copy the classpath configset to a fresh temp Solr home and load a real core. */
    static EmbeddedSolrServer start() throws Exception {
        return start("embedded-solr");
    }

    /** Same, from a named test configset directory on the classpath (e.g. embedded-solr-versioned). */
    static EmbeddedSolrServer start(String configsetName) throws Exception {
        Path resourceHome = Paths.get(
                EmbeddedSolrSupport.class.getResource("/" + configsetName + "/solr.xml").toURI()).getParent();
        Path solrHome = Files.createTempDirectory("embedded-solr-home");
        copyTree(resourceHome, solrHome);
        CoreContainer container = CoreContainer.createAndLoad(solrHome);
        return new EmbeddedSolrServer(container, CORE);
    }

    /** Close the server and its container, then delete the temp home. */
    static void stop(EmbeddedSolrServer server) {
        if (server == null) return;
        CoreContainer container = server.getCoreContainer();
        Path home = container.getSolrHome() == null ? null : Paths.get(container.getSolrHome());
        try {
            server.close();
        } catch (IOException ignored) {
            // best effort
        }
        container.shutdown();
        deleteTree(home);
    }

    private static void copyTree(Path src, Path dst) throws IOException {
        try (Stream<Path> paths = Files.walk(src)) {
            paths.forEach(p -> {
                try {
                    Path target = dst.resolve(src.relativize(p).toString());
                    if (Files.isDirectory(p)) {
                        Files.createDirectories(target);
                    } else {
                        Files.createDirectories(target.getParent());
                        Files.copy(p, target);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    private static void deleteTree(Path root) {
        if (root == null || !Files.exists(root)) return;
        try (Stream<Path> paths = Files.walk(root)) {
            paths.sorted(Comparator.reverseOrder()).forEach(p -> {
                try {
                    Files.delete(p);
                } catch (IOException ignored) {
                    // best effort
                }
            });
        } catch (IOException ignored) {
            // best effort
        }
    }
}
