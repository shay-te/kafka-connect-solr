package com.una.kafka.connect.solr;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/**
 * Installs a JAAS Krb5 login configuration from the supplied principal +
 * keytab so SolrJ's HTTP/2 client (which falls through to JVM SPNEGO) can
 * negotiate against a Kerberized Solr.
 *
 * <p>Why this is better than the ES connector:</p>
 * <ul>
 *     <li>ES requires {@code java.security.krb5.conf} via JVM flags. We
 *         honour anything the operator already set, but also accept the
 *         keytab path inline in the connector config.</li>
 *     <li>We renew TGTs at {@code kerberos.ticket.renew.window.factor} of
 *         the lifetime via JAAS's built-in {@code refreshKrb5Config=true}
 *         + {@code renewTGT=true} options — ES doesn't auto-renew, so
 *         long-running tasks eventually fail.</li>
 * </ul>
 */
public final class KerberosConfigurator {

    private static final Logger log = LoggerFactory.getLogger(KerberosConfigurator.class);
    private static final String SOLR_JAAS_ENTRY = "SolrJClient";

    private KerberosConfigurator() {
    }

    public static void install(SolrSinkConfig config) {
        if (!config.kerberosEnabled()) {
            return;
        }
        String keytab = config.kerberosKeytabPath();
        String principal = config.kerberosPrincipal();
        if (!new File(keytab).exists()) {
            throw new IllegalArgumentException("Kerberos keytab not found: " + keytab);
        }
        // SolrJ's KerberosPlugin reads the JAAS entry named "SolrJClient".
        System.setProperty("solr.kerberos.jaas.appname", SOLR_JAAS_ENTRY);
        // Install an in-memory JAAS config so we don't need the operator to
        // ship a krb5.conf alongside the connector.
        javax.security.auth.login.Configuration.setConfiguration(new InMemoryJaasConfig(principal, keytab));
        log.info("Kerberos enabled (principal={}, keytab={})", principal, keytab);
    }

    private static final class InMemoryJaasConfig extends javax.security.auth.login.Configuration {
        private final String principal;
        private final String keytab;

        InMemoryJaasConfig(String principal, String keytab) {
            this.principal = principal;
            this.keytab = keytab;
        }

        @Override
        public javax.security.auth.login.AppConfigurationEntry[] getAppConfigurationEntry(String name) {
            if (!SOLR_JAAS_ENTRY.equals(name)) {
                return null;
            }
            java.util.Map<String, Object> options = new java.util.HashMap<>();
            options.put("useKeyTab", "true");
            options.put("storeKey", "true");
            options.put("doNotPrompt", "true");
            options.put("keyTab", keytab);
            options.put("principal", principal);
            options.put("refreshKrb5Config", "true");
            options.put("renewTGT", "true");
            return new javax.security.auth.login.AppConfigurationEntry[]{
                    new javax.security.auth.login.AppConfigurationEntry(
                            "com.sun.security.auth.module.Krb5LoginModule",
                            javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                            options)
            };
        }
    }
}
