package com.una.kafka.connect.solr;

import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.DeleteUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;

import java.io.IOException;

/**
 * Server-side fault injection for the {@code embedded-solr-faults} configset: runs inside the real
 * Solr update chain so the connector sees exactly what a failing Solr node returns.
 * <ul>
 *   <li>a document whose {@code fault} field is {@code io}, or a delete of an id starting
 *       {@code io-}: the index write fails with an {@link IOException} (a transient disk/network
 *       failure — retriable);</li>
 *   <li>a delete of an id starting {@code reject-}: Solr refuses it with HTTP 400 (non-retriable).</li>
 * </ul>
 * Everything else passes through untouched.
 */
public class FaultInjectingUpdateProcessorFactory extends UpdateRequestProcessorFactory {

    @Override
    public UpdateRequestProcessor getInstance(SolrQueryRequest req, SolrQueryResponse rsp,
                                              UpdateRequestProcessor next) {
        return new UpdateRequestProcessor(next) {
            @Override
            public void processAdd(AddUpdateCommand cmd) throws IOException {
                SolrInputDocument doc = cmd.getSolrInputDocument();
                if ("io".equals(doc.getFieldValue("fault"))) {
                    throw new IOException("simulated index write failure for id=" + doc.getFieldValue("id"));
                }
                super.processAdd(cmd);
            }

            @Override
            public void processDelete(DeleteUpdateCommand cmd) throws IOException {
                String id = cmd.getId();
                if (id != null && id.startsWith("io-")) {
                    throw new IOException("simulated index write failure deleting id=" + id);
                }
                if (id != null && id.startsWith("reject-")) {
                    throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "delete refused for id=" + id);
                }
                super.processDelete(cmd);
            }
        };
    }
}
