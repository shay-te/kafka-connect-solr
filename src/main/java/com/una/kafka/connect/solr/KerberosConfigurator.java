package com.una.kafka.connect.solr;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

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
        System.setProperty("solr.kerberos.jaas.appname", SOLR_JAAS_ENTRY);
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
