package com.una.kafka.connect.solr;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Real PKCS12 key material generated at test time with the running JDK's {@code keytool}: a
 * self-signed server/client key pair and a truststore holding its certificate. Written under
 * {@code target/} so nothing leaves the build directory.
 */
final class TestKeystores {

    static final String PASSWORD = "changeit-test";
    static final String TYPE = "PKCS12";

    final Path keystore;
    final Path truststore;

    private TestKeystores(Path keystore, Path truststore) {
        this.keystore = keystore;
        this.truststore = truststore;
    }

    /** Key pair valid for {@code localhost}, {@code 127.0.0.1} and any extra host names. */
    static TestKeystores generate(String... extraHosts) throws Exception {
        Path dir = Files.createDirectories(Paths.get("target", "test-keystores"));
        dir = Files.createTempDirectory(dir, "ks");
        Path keystore = dir.resolve("solr-ssl.keystore.p12");
        Path cert = dir.resolve("solr-ssl.cer");
        Path truststore = dir.resolve("solr-ssl.truststore.p12");

        StringBuilder san = new StringBuilder("SAN=DNS:localhost,IP:127.0.0.1");
        for (String host : extraHosts) {
            if (host == null || host.isEmpty() || host.equals("localhost") || host.equals("127.0.0.1")) continue;
            san.append(host.matches("[0-9.]+") ? ",IP:" : ",DNS:").append(host);
        }
        keytool("-genkeypair", "-alias", "solr", "-keyalg", "RSA", "-keysize", "2048",
                "-validity", "2", "-dname", "CN=localhost, OU=kafka-connect-solr tests",
                "-ext", san.toString(),
                "-keystore", keystore.toString(), "-storetype", TYPE,
                "-storepass", PASSWORD, "-keypass", PASSWORD);
        keytool("-exportcert", "-alias", "solr", "-keystore", keystore.toString(),
                "-storetype", TYPE, "-storepass", PASSWORD, "-file", cert.toString());
        keytool("-importcert", "-noprompt", "-alias", "solr", "-file", cert.toString(),
                "-keystore", truststore.toString(), "-storetype", TYPE, "-storepass", PASSWORD);
        return new TestKeystores(keystore.toAbsolutePath(), truststore.toAbsolutePath());
    }

    private static void keytool(String... args) throws Exception {
        List<String> cmd = new ArrayList<>();
        cmd.add(Paths.get(System.getProperty("java.home"), "bin", "keytool").toString());
        cmd.addAll(Arrays.asList(args));
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        byte[] out = p.getInputStream().readAllBytes();
        if (!p.waitFor(60, TimeUnit.SECONDS) || p.exitValue() != 0) {
            throw new IOException("keytool " + args[0] + " failed: " + new String(out, StandardCharsets.UTF_8));
        }
    }
}
