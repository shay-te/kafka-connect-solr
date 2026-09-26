package com.una.kafka.connect.solr;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link ExternalResourceManager} against a real standalone Solr node (in-process): the Collections
 * API answers "not running in SolrCloud mode", so a missing core is auto-created through CoreAdmin
 * from a configset on the node, then written to by a real {@link SolrWriter}.
 */
class ExternalResourceManagerStandaloneEmbeddedTest {

    private static final Schema CUSTOMER = SchemaBuilder.struct().name("customer")
            .field("id", Schema.STRING_SCHEMA)
            .field("name", Schema.STRING_SCHEMA)
            .field("segment", Schema.OPTIONAL_STRING_SCHEMA)
            .build();

    private EmbeddedSolrServer solr;

    @BeforeEach
    void setUp() throws Exception {
        solr = EmbeddedSolrSupport.start();
        // Standalone Solr creates cores from <solr home>/configsets/<name>/conf.
        Path home = Paths.get(solr.getCoreContainer().getSolrHome());
        copyTree(home.resolve(EmbeddedSolrSupport.CORE).resolve("conf"),
                home.resolve("configsets").resolve("_default").resolve("conf"));
    }

    @AfterEach
    void tearDown() {
        EmbeddedSolrSupport.stop(solr);
    }

    private static void copyTree(Path src, Path dst) throws Exception {
        try (Stream<Path> paths = Files.walk(src)) {
            for (Path p : (Iterable<Path>) paths::iterator) {
                Path target = dst.resolve(src.relativize(p).toString());
                if (Files.isDirectory(p)) Files.createDirectories(target);
                else Files.copy(p, target);
            }
        }
    }

    private SolrSinkConfig config(String... kv) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://embedded/solr");
        p.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "AUTO");
        p.put(SolrSinkConfig.SCHEMA_AUTO_CREATE_CONFIG, "true");
        p.put(SolrSinkConfig.BATCH_SIZE_CONFIG, "10");
        for (int i = 0; i < kv.length; i += 2) p.put(kv[i], kv[i + 1]);
        return new SolrSinkConfig(p);
    }

    private boolean coreExists(String name) throws Exception {
        return !CoreAdminRequest.getStatus(name, solr).getCoreStatus(name).asMap(0).isEmpty();
    }

    @Test
    void missingCoreIsCreatedFromTheConfigsetAndReceivesTheTopicsRecords() throws Exception {
        assertThat(coreExists("customers")).isFalse();
        SolrWriter writer = new SolrWriter(new EmbeddedSolrServer(solr.getCoreContainer(), null), config());
        try {
            Struct c = new Struct(CUSTOMER).put("id", "c-1").put("name", "Maya").put("segment", "smb");
            writer.write(new SinkRecord("customers", 0, Schema.STRING_SCHEMA, "c-1", CUSTOMER, c, 1));
            // A second topic -> second missing core: the standalone answer is cached, no second
            // Collections API call.
            Struct l = new Struct(CUSTOMER).put("id", "l-1").put("name", "Ido").put("segment", "ent");
            writer.write(new SinkRecord("leads", 0, Schema.STRING_SCHEMA, "l-1", CUSTOMER, l, 1));
            writer.flush();
        } finally {
            writer.close();
        }
        assertThat(coreExists("customers")).isTrue();
        assertThat(coreExists("leads")).isTrue();
        solr.commit("customers");
        assertThat(solr.query("customers", new SolrQuery("segment:smb")).getResults().getNumFound()).isEqualTo(1);
    }

    @Test
    void creatingFromAConfigsetTheNodeDoesNotHaveFailsTheTask() {
        ExternalResourceManager mgr = new ExternalResourceManager(solr,
                config(SolrSinkConfig.AUTO_CREATE_CONFIGSET_CONFIG, "no_such_configset"));
        assertThatThrownBy(() -> mgr.ensure("orders"))
                .isInstanceOf(ConnectException.class)
                .hasMessageContaining("Failed to auto-create collection 'orders'");
        assertThat(mgr.isKnown("orders")).isFalse();
    }

    private static boolean blockedOnTheManager(Thread t) {
        StackTraceElement[] stack = t.getStackTrace();
        return t.getState() == Thread.State.BLOCKED && stack.length > 0
                && stack[0].getClassName().equals(ExternalResourceManager.class.getName())
                && stack[0].getMethodName().equals("ensureSlow");
    }

    @Test
    void aSecondTaskThreadWaitingOnTheSameCoreFindsItAlreadyEnsured() throws Exception {
        ExternalResourceManager mgr = new ExternalResourceManager(solr, config());
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread second = new Thread(() -> {
            try {
                mgr.ensure("invoices");
            } catch (Throwable t) {
                failure.set(t);
            }
        }, "second-ensure");
        // Hold the manager's lock so the second thread queues behind the first ensure.
        synchronized (mgr) {
            second.start();
            while (!blockedOnTheManager(second)) {
                Thread.onSpinWait();
            }
            mgr.ensure("invoices"); // reentrant: creates the core while the other thread waits
        }
        second.join();
        assertThat(failure.get()).isNull();
        assertThat(mgr.isKnown("invoices")).isTrue();
        assertThat(coreExists("invoices")).isTrue();
    }
}
