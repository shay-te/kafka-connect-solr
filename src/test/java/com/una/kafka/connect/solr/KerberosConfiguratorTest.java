package com.una.kafka.connect.solr;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class KerberosConfiguratorTest {

    private SolrSinkConfig cfg(Map<String, String> overrides) {
        Map<String, String> p = new HashMap<>();
        p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
        p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "c");
        p.putAll(overrides);
        return new SolrSinkConfig(p);
    }

    @Test
    void disabledIsNoop() {
        // No principal/keytab set - install() returns without touching JAAS.
        KerberosConfigurator.install(cfg(new HashMap<>()));
    }

    @Test
    void missingKeytabFails() {
        Map<String, String> o = new HashMap<>();
        o.put(SolrSinkConfig.KERBEROS_PRINCIPAL_CONFIG, "user@REALM");
        o.put(SolrSinkConfig.KERBEROS_KEYTAB_PATH_CONFIG, "/no/such/keytab");
        assertThatThrownBy(() -> KerberosConfigurator.install(cfg(o)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("keytab not found");
    }

    @Test
    void installsWithRealKeytabFile() throws Exception {
        // We only need a *file that exists* - JAAS validation happens at
        // first authentication attempt, not at install time.
        Path keytab = Files.createTempFile("test-keytab", ".kt");
        try {
            Map<String, String> o = new HashMap<>();
            o.put(SolrSinkConfig.KERBEROS_PRINCIPAL_CONFIG, "user@REALM");
            o.put(SolrSinkConfig.KERBEROS_KEYTAB_PATH_CONFIG, keytab.toAbsolutePath().toString());
            KerberosConfigurator.install(cfg(o));
            // Side effect: solr.kerberos.jaas.appname system property is set.
            assertThat(System.getProperty("solr.kerberos.jaas.appname")).isEqualTo("SolrJClient");
        } finally {
            Files.deleteIfExists(keytab);
            System.clearProperty("solr.kerberos.jaas.appname");
        }
    }
}
