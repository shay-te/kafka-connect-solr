package com.una.kafka.connect.solr;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class VersionTest {

    @Test
    void returnsNonEmpty() {
        assertThat(Version.getVersion()).isNotNull();
        assertThat(Version.getVersion()).isNotEmpty();
    }

    @Test
    void stableAcrossCalls() {
        assertThat(Version.getVersion()).isEqualTo(Version.getVersion());
    }
}
