package com.una.kafka.connect.solr;

import org.junit.jupiter.api.Test;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Drives the InMemoryJaasConfig inner class — covers getAppConfigurationEntry's
 * match path (matching SolrJClient name) and miss path (unknown entry name).
 */
class KerberosInMemoryJaasConfigTest {

    @Test
    void jaasEntryPopulatedForSolrJClient() throws Exception {
        Path keytab = Files.createTempFile("kt", ".kt");
        Configuration previous = Configuration.getConfiguration();
        try {
            Map<String, String> p = new HashMap<>();
            p.put(SolrSinkConfig.SOLR_URL_CONFIG, "http://x");
            p.put(SolrSinkConfig.SOLR_COLLECTION_CONFIG, "c");
            p.put(SolrSinkConfig.KERBEROS_PRINCIPAL_CONFIG, "user@REALM");
            p.put(SolrSinkConfig.KERBEROS_KEYTAB_PATH_CONFIG, keytab.toAbsolutePath().toString());
            KerberosConfigurator.install(new SolrSinkConfig(p));

            AppConfigurationEntry[] entries =
                    Configuration.getConfiguration().getAppConfigurationEntry("SolrJClient");
            assertThat(entries).hasSize(1);
            Map<String, ?> opts = entries[0].getOptions();
            assertThat(opts.get("useKeyTab")).isEqualTo("true");
            assertThat(opts.get("storeKey")).isEqualTo("true");
            assertThat(opts.get("doNotPrompt")).isEqualTo("true");
            assertThat(opts.get("principal")).isEqualTo("user@REALM");
            assertThat(opts.get("keyTab")).isEqualTo(keytab.toAbsolutePath().toString());
            assertThat(opts.get("refreshKrb5Config")).isEqualTo("true");
            assertThat(opts.get("renewTGT")).isEqualTo("true");

            // Unknown entry name returns null.
            assertThat(Configuration.getConfiguration().getAppConfigurationEntry("Unknown"))
                    .isNull();
        } finally {
            Configuration.setConfiguration(previous);
            Files.deleteIfExists(keytab);
            System.clearProperty("solr.kerberos.jaas.appname");
        }
    }
}
