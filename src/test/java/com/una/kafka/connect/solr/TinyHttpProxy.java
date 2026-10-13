package com.una.kafka.connect.solr;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A minimal real HTTP/1.1 forward proxy on loopback. It requires Basic proxy credentials (answers
 * 407 until they are sent), then forwards the request to the upstream registered for the target
 * host and relays the response. One request per connection; enough for HttpURLConnection flows.
 */
final class TinyHttpProxy implements AutoCloseable {

    private final ServerSocket server;
    private final String expectedAuthorization;
    private final Map<String, InetSocketAddress> upstreams = new ConcurrentHashMap<>();
    private final AtomicInteger challenged = new AtomicInteger();
    private final AtomicInteger forwarded = new AtomicInteger();

    /** `user` null: an open proxy, which is the only kind SolrJ's client can use (it sends no credentials). */
    TinyHttpProxy(String user, String password) throws IOException {
        this.expectedAuthorization = user == null ? null : "Basic " + Base64.getEncoder()
                .encodeToString((user + ":" + password).getBytes(StandardCharsets.UTF_8));
        server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        Thread acceptor = new Thread(this::acceptLoop, "tiny-proxy-accept");
        acceptor.setDaemon(true);
        acceptor.start();
    }

    int port() {
        return server.getLocalPort();
    }

    /** Requests for {@code host} are forwarded to {@code upstream}. */
    void route(String host, InetSocketAddress upstream) {
        upstreams.put(host.toLowerCase(java.util.Locale.ROOT), upstream);
    }

    int challengedRequests() {
        return challenged.get();
    }

    int forwardedRequests() {
        return forwarded.get();
    }

    private void acceptLoop() {
        while (!server.isClosed()) {
            try {
                Socket client = server.accept();
                Thread t = new Thread(() -> handle(client), "tiny-proxy-conn");
                t.setDaemon(true);
                t.start();
            } catch (IOException closed) {
                return;
            }
        }
    }

    private void handle(Socket client) {
        try (Socket c = client; InputStream in = c.getInputStream(); OutputStream out = c.getOutputStream()) {
            String requestLine = readLine(in);
            if (requestLine == null || requestLine.isEmpty()) return;
            Map<String, String> headers = new LinkedHashMap<>();
            for (String line = readLine(in); line != null && !line.isEmpty(); line = readLine(in)) {
                int colon = line.indexOf(':');
                if (colon > 0) headers.put(line.substring(0, colon).trim().toLowerCase(java.util.Locale.ROOT),
                        line.substring(colon + 1).trim());
            }
            if (expectedAuthorization != null && !expectedAuthorization.equals(headers.get("proxy-authorization"))) {
                challenged.incrementAndGet();
                respond(out, "407 Proxy Authentication Required",
                        "Proxy-Authenticate: Basic realm=\"tiny-proxy\"\r\n", "");
                return;
            }
            String[] parts = requestLine.split(" ");
            URI target = URI.create(parts[1]);
            InetSocketAddress upstream = upstreams.get(target.getHost().toLowerCase(java.util.Locale.ROOT));
            if (upstream == null) {
                respond(out, "502 Bad Gateway", "", "no route to " + target.getHost());
                return;
            }
            byte[] body = new byte[0];
            if (headers.containsKey("content-length")) {
                body = in.readNBytes(Integer.parseInt(headers.get("content-length")));
            }
            forwarded.incrementAndGet();
            forward(parts[0], target, headers, body, in, upstream, out);
        } catch (IOException ignored) {
            // client went away
        }
    }

    private static void forward(String method, URI target, Map<String, String> headers, byte[] body,
                                InputStream rest, InetSocketAddress upstream, OutputStream out) throws IOException {
        try (Socket up = new Socket(upstream.getAddress(), upstream.getPort())) {
            StringBuilder req = new StringBuilder();
            String path = target.getRawPath() + (target.getRawQuery() == null ? "" : "?" + target.getRawQuery());
            req.append(method).append(' ').append(path).append(" HTTP/1.1\r\n");
            for (Map.Entry<String, String> h : headers.entrySet()) {
                if (h.getKey().startsWith("proxy-") || h.getKey().equals("connection")) continue;
                req.append(h.getKey()).append(": ").append(h.getValue()).append("\r\n");
            }
            req.append("connection: close\r\n\r\n");
            OutputStream upOut = up.getOutputStream();
            upOut.write(req.toString().getBytes(StandardCharsets.ISO_8859_1));
            upOut.write(body);
            upOut.flush();
            // A chunked body (SolrJ's update requests) has no content-length: relay whatever follows.
            Thread pump = new Thread(() -> {
                try {
                    rest.transferTo(upOut);
                    upOut.flush();
                } catch (IOException ignored) {
                    // the client closed; the response below is what matters
                }
            }, "tiny-proxy-body");
            pump.setDaemon(true);
            pump.start();
            up.getInputStream().transferTo(out); // upstream closes after one response
            out.flush();
        }
    }

    private static void respond(OutputStream out, String status, String extraHeaders, String body) throws IOException {
        byte[] b = body.getBytes(StandardCharsets.UTF_8);
        String head = "HTTP/1.1 " + status + "\r\n" + extraHeaders
                + "Content-Length: " + b.length + "\r\nConnection: close\r\n\r\n";
        out.write(head.getBytes(StandardCharsets.ISO_8859_1));
        out.write(b);
        out.flush();
    }

    private static String readLine(InputStream in) throws IOException {
        ByteArrayOutputStream line = new ByteArrayOutputStream();
        int b;
        while ((b = in.read()) != -1) {
            if (b == '\n') break;
            if (b != '\r') line.write(b);
        }
        return b == -1 && line.size() == 0 ? null : line.toString(StandardCharsets.ISO_8859_1);
    }

    @Override
    public void close() throws IOException {
        server.close();
    }
}
