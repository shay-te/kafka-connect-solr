package com.una.kafka.connect.solr;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * A hung Solr: a real TCP endpoint that accepts connections, drains whatever the client sends and
 * never answers. Drives the flush-timeout and stalled-stream paths deterministically — the request
 * cannot complete until the client gives up or this server is closed.
 */
final class BlackholeSolrServer implements AutoCloseable {

    private final ServerSocket server;
    private final List<Socket> accepted = new CopyOnWriteArrayList<>();
    private final Thread acceptor;

    BlackholeSolrServer() throws IOException {
        server = new ServerSocket(0, 50, InetAddress.getLoopbackAddress());
        acceptor = new Thread(this::acceptLoop, "blackhole-solr-accept");
        acceptor.setDaemon(true);
        acceptor.start();
    }

    /** Base URL in the shape the connector expects for solr.url. */
    String baseUrl() {
        return "http://127.0.0.1:" + server.getLocalPort() + "/solr";
    }

    private void acceptLoop() {
        while (!server.isClosed()) {
            try {
                Socket socket = server.accept();
                accepted.add(socket);
                Thread drain = new Thread(() -> drain(socket), "blackhole-solr-drain");
                drain.setDaemon(true);
                drain.start();
            } catch (IOException closed) {
                return;
            }
        }
    }

    // Keep reading so the client's writes never block on a full socket buffer.
    private static void drain(Socket socket) {
        byte[] buf = new byte[8192];
        try (InputStream in = socket.getInputStream()) {
            while (in.read(buf) >= 0) {
                // discard
            }
        } catch (IOException ignored) {
            // socket closed
        }
    }

    @Override
    public void close() throws IOException {
        server.close();
        for (Socket s : accepted) {
            try {
                s.close();
            } catch (IOException ignored) {
                // best effort
            }
        }
    }
}
