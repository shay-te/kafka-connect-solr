package com.una.kafka.connect.solr;

import org.apache.solr.client.solrj.impl.ConcurrentUpdateHttp2SolrClient;

import java.util.concurrent.atomic.AtomicReference;

/**
 * The streaming client, keeping what Solr refused. ConcurrentUpdateHttp2SolrClient sends from its own runner
 * threads and only logs a refused batch in handleError; {@link SolrBulkProcessor#flushSync()} takes the failure
 * after the drain and fails the flush, so no offset is committed past it.
 */
final class StreamingSolrClient extends ConcurrentUpdateHttp2SolrClient {

    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    StreamingSolrClient(ConcurrentUpdateHttp2SolrClient.Builder builder) {
        super(builder);
    }

    @Override
    public void handleError(Throwable ex) {
        super.handleError(ex);
        failure.compareAndSet(null, ex);
    }

    /** The first failure since the last call: a flush owns everything streamed before it. */
    Throwable takeFailure() {
        return failure.getAndSet(null);
    }
}
