package com.una.kafka.connect.solr;

import org.junit.jupiter.api.Test;

import java.net.URL;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link Version} when the plugin is packaged without its version resource, or with a corrupt
 * one: the connector must still report a version instead of failing to load. The real class is
 * re-initialised in an isolated loader that serves (or withholds) the resource.
 */
class VersionResourceFallbackTest {

    private static final String RESOURCE = "kafka-connect-solr-version.properties";

    private static String versionSeenBy(ChildFirstClassLoader loader) throws Exception {
        try (loader) {
            Class<?> version = Class.forName(Version.class.getName(), true, loader);
            assertThat(version.getClassLoader()).isSameAs(loader);
            return (String) version.getMethod("getVersion").invoke(null);
        }
    }

    private static ChildFirstClassLoader isolatedVersion() {
        return new ChildFirstClassLoader(new URL[]{ChildFirstClassLoader.codeSourceOf(Version.class)},
                Version.class.getName());
    }

    @Test
    void theBuiltResourceIsReadWhenPresent() {
        assertThat(Version.getVersion()).isNotEqualTo("0.0.0").isNotBlank();
    }

    @Test
    void missingResourceFallsBackToTheDefault() throws Exception {
        assertThat(versionSeenBy(isolatedVersion().withResource(RESOURCE, null))).isEqualTo("0.0.0");
    }

    @Test
    void unreadableResourceFallsBackToTheDefault() throws Exception {
        // java.util.Properties rejects a malformed unicode escape with IllegalArgumentException.
        byte[] corrupt = "version=\\uZZZZ\n".getBytes(StandardCharsets.ISO_8859_1);
        assertThat(versionSeenBy(isolatedVersion().withResource(RESOURCE, corrupt))).isEqualTo("0.0.0");
    }
}
