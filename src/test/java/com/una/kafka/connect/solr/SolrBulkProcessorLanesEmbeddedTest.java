package com.una.kafka.connect.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.NamedList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Ordering lanes: {@code max.in.flight.requests > 1} keeps per-document order WITHOUT the Solr
 * version guard. The core here has NO DocBasedVersionConstraints — nothing in Solr rescues a
 * reordered write, so the final document shows exactly the order the connector applied.
 *
 * <p>Overtaking is forced, not hoped for: a real in-process Solr sits behind a client that holds
 * back the requests a test picks, so a later flush of the same id is ready first. With lanes off
 * that later flush overtakes and the stale write wins — proof the test can see the failure lanes
 * exist to prevent.
 */
class SolrBulkProcessorLanesEmbeddedTest {

    private static final String CORE = EmbeddedSolrSupport.CORE;

    private EmbeddedSolrServer solr;

    @BeforeEach
    void setUp() throws Exception {
        solr = EmbeddedSolrSupport.start();
    }

    @AfterEach
    void tearDown() {
        EmbeddedSolrSupport.stop(solr);
    }

    /** Every op flushes on its own (batch.size=1), so each write is a separate in-flight request. */
    private static SolrSinkConfig config(int inFlight, boolean lanes) {
        Map<String, String> p = new LinkedHashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://embedded/solr");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, CORE);
        p.put(SolrSinkConfig.EXTERNAL_RESOURCE_USAGE_CONFIG, "UNUSED");
        p.put(SolrSinkConfig.SCHEMA_IGNORE_CONFIG, "true");
        p.put(SolrSinkConfig.BATCH_SIZE_CONFIG, "1");
        p.put(SolrSinkConfig.LINGER_MS_CONFIG, "60000");
        p.put(SolrSinkConfig.MAX_RETRIES_CONFIG, "0");
        p.put(SolrSinkConfig.MAX_IN_FLIGHT_REQUESTS_CONFIG, String.valueOf(inFlight));
        p.put(SolrSinkConfig.ORDERING_LANES_ENABLED_CONFIG, String.valueOf(lanes));
        return new SolrSinkConfig(p);
    }

    private static SolrInputDocument doc(String id, String name) {
        SolrInputDocument d = new SolrInputDocument();
        d.addField("id", id);
        d.addField("name", name);
        return d;
    }

    private SolrDocumentList find(String id) throws Exception {
        solr.commit(CORE);
        return solr.query(CORE, new SolrQuery("id:" + id)).getResults();
    }

    private static boolean upsertNamed(SolrRequest<?> request, String name) {
        return request instanceof UpdateRequest && ((UpdateRequest) request).getDocuments() != null
                && name.equals(((UpdateRequest) request).getDocuments().get(0).getFieldValue("name"));
    }

    private static boolean isDelete(SolrRequest<?> request) {
        return request instanceof UpdateRequest && ((UpdateRequest) request).getDeleteById() != null;
    }

    @Test
    void withoutLanesALaterWriteOvertakesAndTheStaleVersionWins() throws Exception {
        // The hazard itself, at in-flight 2 with the version guard absent.
        HoldingClient client = new HoldingClient(solr, r -> upsertNamed(r, "v1"), 300);
        try (SolrBulkProcessor bulk = new SolrBulkProcessor(client, config(2, false))) {
            assertThat(bulk.laneCount()).isEqualTo(1);
            bulk.upsert(CORE, doc("x", "v1"), null);
            bulk.upsert(CORE, doc("x", "v2"), null);
            bulk.flushSync();
        }
        assertThat(find("x").get(0).getFirstValue("name"))
                .as("v2 was sent second but landed first; v1 then overwrote it").isEqualTo("v1");
    }

    @Test
    void withLanesTheSameIdWaitsForItsOwnLaneAndTheNewestVersionWins() throws Exception {
        HoldingClient client = new HoldingClient(solr, r -> upsertNamed(r, "v1"), 300);
        try (SolrBulkProcessor bulk = new SolrBulkProcessor(client, config(2, true))) {
            assertThat(bulk.laneCount()).isEqualTo(2);
            bulk.upsert(CORE, doc("x", "v1"), null);
            bulk.upsert(CORE, doc("x", "v2"), null);
            bulk.flushSync();
        }
        assertThat(find("x").get(0).getFirstValue("name")).isEqualTo("v2");
    }

    @Test
    void withLanesADeleteHeldBackStillLandsBeforeTheRecreateOfThatId() throws Exception {
        solr.add(CORE, doc("u", "original"));
        HoldingClient client = new HoldingClient(solr, SolrBulkProcessorLanesEmbeddedTest::isDelete, 300);
        try (SolrBulkProcessor bulk = new SolrBulkProcessor(client, config(4, true))) {
            bulk.delete(CORE, "u", null);
            bulk.upsert(CORE, doc("u", "recreated"), null);
            bulk.flushSync();
        }
        SolrDocumentList found = find("u");
        assertThat(found).as("delete then re-create must leave the document alive").hasSize(1);
        assertThat(found.get(0).getFirstValue("name")).isEqualTo("recreated");
    }

    @Test
    void withLanesDifferentIdsStillRunInParallel() throws Exception {
        // Lanes must not collapse to one serial stream: requests for ids on different lanes overlap.
        HoldingClient client = new HoldingClient(solr, r -> true, 150);
        try (SolrBulkProcessor bulk = new SolrBulkProcessor(client, config(4, true))) {
            for (int i = 0; i < 16; i++) {
                bulk.upsert(CORE, doc("p" + i, "n" + i), null);
            }
            bulk.flushSync();
        }
        assertThat(client.maxConcurrent.get()).as("peak concurrent Solr requests").isGreaterThan(1)
                .isLessThanOrEqualTo(4);
        solr.commit(CORE);
        assertThat(solr.query(CORE, new SolrQuery("id:p*")).getResults().getNumFound()).isEqualTo(16);
    }

    @Test
    void lanesOnAtInFlightOneAreNotBuiltAndTheirThreadsEndOnClose() throws Exception {
        try (SolrBulkProcessor serial = new SolrBulkProcessor(solr, config(1, true))) {
            assertThat(serial.laneCount()).as("in-flight 1 is already ordered: no lanes").isEqualTo(1);
        }
        SolrBulkProcessor laned = new SolrBulkProcessor(solr, config(3, true));
        laned.upsert(CORE, doc("t", "t"), null);
        laned.flushSync();
        laned.close();
        assertThat(Thread.getAllStackTraces().keySet())
                .noneMatch(t -> t.isAlive() && t.getName().startsWith("solr-lane-"));
    }

    @Test
    void anUpsertWithANumericIdAndADeleteOfItsStringFormShareOneLane() {
        try (OrderingLanes lanes = new OrderingLanes(5)) {
            for (long id = 0; id < 200; id++) {
                assertThat(lanes.laneFor(id)).isEqualTo(lanes.laneFor(String.valueOf(id)));
            }
            assertThat(lanes.laneFor(null)).isZero();
        }
    }

    @Test
    void fewerThanTwoLanesIsRejectedBecauseOneLaneIsJustTheSharedPoolPath() {
        assertThatThrownBy(() -> new OrderingLanes(1)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void closeStopsALaneWhoseRequestOutlivesTheTimeout() throws Exception {
        OrderingLanes lanes = new OrderingLanes(2);
        CountDownLatch stopped = stuckRequestOn(lanes.executor(0));
        lanes.close(TimeUnit.MILLISECONDS.toNanos(50));
        assertThat(stopped.await(5, TimeUnit.SECONDS)).as("the stuck request was interrupted").isTrue();
        assertThat(lanes.executor(1).isShutdown()).as("the idle lane is stopped too").isTrue();
    }

    @Test
    void anInterruptedCloseStillStopsEveryLaneAndKeepsTheInterrupt() throws Exception {
        OrderingLanes lanes = new OrderingLanes(2);
        CountDownLatch stopped = stuckRequestOn(lanes.executor(0));
        Thread.currentThread().interrupt();
        try {
            lanes.close(TimeUnit.SECONDS.toNanos(10));
            assertThat(Thread.currentThread().isInterrupted()).as("interrupt restored for the caller").isTrue();
        } finally {
            Thread.interrupted();
        }
        assertThat(stopped.await(5, TimeUnit.SECONDS)).isTrue();
        assertThat(lanes.executor(1).isShutdown()).isTrue();
    }

    /** A request that blocks until interrupted; the latch opens when it is. */
    private static CountDownLatch stuckRequestOn(java.util.concurrent.ExecutorService lane) throws Exception {
        CountDownLatch running = new CountDownLatch(1);
        CountDownLatch stopped = new CountDownLatch(1);
        lane.submit(() -> {
            running.countDown();
            try {
                Thread.sleep(60_000);
            } catch (InterruptedException ie) {
                stopped.countDown();
            }
        });
        assertThat(running.await(5, TimeUnit.SECONDS)).isTrue();
        return stopped;
    }

    /** Delegates to a real Solr; holds back the requests {@code hold} matches and counts overlap. */
    private static final class HoldingClient extends SolrClient {
        private final SolrClient delegate;
        private final Predicate<SolrRequest<?>> hold;
        private final long holdMs;
        private final AtomicInteger concurrent = new AtomicInteger();
        final AtomicInteger maxConcurrent = new AtomicInteger();

        HoldingClient(SolrClient delegate, Predicate<SolrRequest<?>> hold, long holdMs) {
            this.delegate = delegate;
            this.hold = hold;
            this.holdMs = holdMs;
        }

        @Override
        public NamedList<Object> request(SolrRequest<?> request, String collection)
                throws SolrServerException, IOException {
            maxConcurrent.accumulateAndGet(concurrent.incrementAndGet(), Math::max);
            try {
                if (hold.test(request)) {
                    Thread.sleep(holdMs);
                }
                return delegate.request(request, collection);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IOException(ie);
            } finally {
                concurrent.decrementAndGet();
            }
        }

        @Override
        public void close() {
        }
    }
}
