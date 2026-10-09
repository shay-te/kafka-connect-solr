package com.una.kafka.connect.solr;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The image's plugin directory is the assembly's lib/ (the connector jar plus SolrJ and every other
 * runtime dependency). Copying target/*.jar installed only the thin connector jar, and the connector
 * died with NoClassDefFoundError on the SolrJ classes at start.
 */
class DockerfilePluginTest {

    private static String read(String path) throws Exception {
        return new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8);
    }

    @Test
    void theImageInstallsTheAssemblyLibNotTheThinJar() throws Exception {
        String dockerfile = read("Dockerfile");

        assertThat(dockerfile).doesNotContain("target/*.jar");
        assertThat(dockerfile).contains("jar xf /src/target/*-package.zip");
        assertThat(dockerfile).contains("mv /assembly/*/lib /plugin-lib");
        assertThat(dockerfile).contains(
                "COPY --from=build /plugin-lib/ /usr/share/confluent-hub-components/una-kafka-connect-solr/lib/");
    }

    @Test
    void theAssemblyLibHoldsTheConnectorAndItsRuntimeDependencies() throws Exception {
        String assembly = read("src/assembly/package.xml");

        assertThat(assembly).contains("<outputDirectory>lib</outputDirectory>");
        assertThat(assembly).contains("<useProjectArtifact>true</useProjectArtifact>");
        assertThat(assembly).contains("<scope>runtime</scope>");
    }
}
