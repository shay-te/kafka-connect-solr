package com.una.kafka.connect.solr;

import java.io.InputStream;
import java.util.Properties;

public final class Version {

    private static final String VERSION;

    static {
        String version = "0.0.0";
        try (InputStream in = Version.class.getResourceAsStream("/kafka-connect-solr-version.properties")) {
            if (in != null) {
                Properties props = new Properties();
                props.load(in);
                version = props.getProperty("version", version);
            }
        } catch (Exception ignored) {
            // fall back to the default
        }
        VERSION = version;
    }

    private Version() {
    }

    public static String getVersion() {
        return VERSION;
    }
}
