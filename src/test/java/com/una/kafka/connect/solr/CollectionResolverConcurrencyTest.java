package com.una.kafka.connect.solr;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link CollectionResolver} under concurrent first use and with an explicitly null strategy.
 */
class CollectionResolverConcurrencyTest {

    private static SolrSinkConfig config(String strategy, String collection) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://solr:8983/solr");
        p.put(SolrSinkConfig.COLLECTION_NAMING_STRATEGY_CONFIG, strategy);
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, collection);
        return new SolrSinkConfig(p);
    }

    @Test
    void explicitlyNullStrategyFallsBackToTopicNaming() {
        CollectionResolver r = new CollectionResolver(config(null, ""));
        assertThat(r.resolve("crm.public.contacts")).isEqualTo("crm.public.contacts");
    }

    private static boolean inCompute(Thread t) {
        for (StackTraceElement e : t.getStackTrace()) {
            if (e.getClassName().equals(CollectionResolver.class.getName()) && e.getMethodName().equals("compute")) {
                return true;
            }
        }
        return false;
    }

    @Test
    void twoThreadsResolvingTheSameNewTopicAgreeOnOneResult() throws Exception {
        // A pathological (catastrophically backtracking) pattern keeps each first resolve busy for
        // a while, so both threads can be observed inside compute() at the same moment.
        String topic = "a".repeat(26);
        for (int attempt = 0; attempt < 20; attempt++) {
            CollectionResolver resolver = new CollectionResolver(config("TOPIC_REGEX", "(a+)+b=>never"));
            AtomicReference<String> first = new AtomicReference<>();
            AtomicReference<String> second = new AtomicReference<>();
            CountDownLatch go = new CountDownLatch(1);
            Thread t1 = new Thread(() -> { await(go); first.set(resolver.resolve(topic)); }, "resolver-1");
            Thread t2 = new Thread(() -> { await(go); second.set(resolver.resolve(topic)); }, "resolver-2");
            t1.start();
            t2.start();
            go.countDown();
            boolean overlapped = false;
            while (t1.isAlive() && t2.isAlive() && !overlapped) {
                overlapped = inCompute(t1) && inCompute(t2);
            }
            t1.join();
            t2.join();
            assertThat(first.get()).isEqualTo(topic);
            assertThat(second.get()).isEqualTo(topic);
            if (overlapped) {
                // Both computed; one lost the putIfAbsent and returned the winner's value.
                assertThat(resolver.resolve(topic)).isEqualTo(topic);
                return;
            }
        }
        throw new AssertionError("never observed both threads inside compute() at once");
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
