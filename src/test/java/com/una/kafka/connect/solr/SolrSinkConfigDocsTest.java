package com.una.kafka.connect.solr;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;

/** The configuration reference is generated from the ConfigDef, never written by hand. */
class SolrSinkConfigDocsTest {

    private static final Path REFERENCE = Paths.get("docs", "configuration.rst");
    private static final Path GENERATED = Paths.get("target", "configuration.rst");

    @Test
    void mainPrintsTheReferenceGeneratedFromTheConfigDef() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream original = System.out;
        System.setOut(new PrintStream(out, true, "UTF-8"));
        try {
            SolrSinkConfig.main(new String[0]);
        } finally {
            System.setOut(original);
        }
        assertThat(out.toString("UTF-8")).isEqualTo(SolrSinkConfig.config().toEnrichedRst());
    }

    @Test
    void theCommittedReferenceMatchesTheConfigDef() throws Exception {
        String generated = SolrSinkConfig.config().toEnrichedRst();
        Files.write(GENERATED, generated.getBytes(StandardCharsets.UTF_8));
        String committed = new String(Files.readAllBytes(REFERENCE), StandardCharsets.UTF_8).replace("\r\n", "\n");
        assertThat(committed).as("docs/configuration.rst is stale: copy target/configuration.rst over it")
                .isEqualTo(generated);
    }
}
