package aerocache.net;

import aerocache.core.CacheEngine;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * AeroCache TCP server — accepts client connections and dispatches commands
 * to the local {@link CacheEngine} using the CRLF text wire protocol.
 *
 * <h3>Concurrency model — Java 21 Virtual Threads</h3>
 * The server uses {@link Executors#newVirtualThreadPerTaskExecutor()} to spawn
 * one lightweight virtual thread per connected client. Virtual threads are
 * cheap enough (stack ~KB vs ~MB for platform threads) that we can handle
 * thousands of simultaneous connections without NIO selectors or thread-pool
 * tuning. The JVM schedules them on its carrier thread pool automatically.
 *
 * <pre>
 *   ServerSocket.accept()       ← blocking, runs in its own virtual thread
 *         │
 *         ▼  for each accepted Socket
 *   virtualThreadPool.submit(ClientSession)
 *         │
 *         ▼  inside ClientSession (virtual thread)
 *   while (readLine() != null)
 *       CommandParser.parse()  →  CommandHandler.handle()  →  write response
 * </pre>
 *
 * <h3>TCP settings</h3>
 * <ul>
 *   <li>{@code TCP_NODELAY = true} — disables Nagle's algorithm so small
 *       command frames are sent immediately, reducing per-operation latency.</li>
 *   <li>{@code SO_REUSEADDR = true} — allows the server to re-bind the port
 *       instantly after a restart without waiting for TIME_WAIT to expire.</li>
 *   <li>No {@code SO_TIMEOUT} on client sockets — connections are kept open
 *       as long as the client stays connected (persistent sessions).</li>
 * </ul>
 *
 * <h3>Graceful shutdown</h3>
 * {@link #stop()} closes the {@code ServerSocket} (which unblocks
 * {@code accept()}), sets {@code running = false}, then shuts the executor
 * down and waits up to 5 seconds for in-flight sessions to complete.
 */
public class TCPServer {

    private static final Logger LOG = Logger.getLogger(TCPServer.class.getName());

    /** CRLF terminator appended to every response frame. */
    private static final String CRLF = "\r\n";

    // -----------------------------------------------------------------------
    // Configuration
    // -----------------------------------------------------------------------

    private final int port;

    // -----------------------------------------------------------------------
    // Shared, stateless handler — safe to use from all virtual threads
    // -----------------------------------------------------------------------
    private final CommandHandler handler;

    // -----------------------------------------------------------------------
    // Runtime state
    // -----------------------------------------------------------------------

    private ServerSocket serverSocket;
    private ExecutorService virtualThreadPool;

    /**
     * Controls the accept loop and all client session loops.
     * Declared {@code volatile} so the write in {@link #stop()} is visible
     * to every virtual thread reading it on the next iteration.
     */
    private volatile boolean running = false;

    // -----------------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------------

    /**
     * @param port   TCP port to listen on (e.g. 6001)
     * @param engine the local cache engine for this node
     */
    public TCPServer(int port, CacheEngine<String, String> engine) {
        if (port < 1 || port > 65535)
            throw new IllegalArgumentException("Invalid port: " + port);
        this.port    = port;
        this.handler = new CommandHandler(engine); // stateless — one instance for all sessions
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    /**
     * Bind the server socket and start accepting connections.
     *
     * <p>This method returns immediately after starting the accept loop in a
     * background virtual thread — it does NOT block the caller.
     *
     * @throws IOException if the port cannot be bound
     */
    public void start() throws IOException {
        serverSocket = new ServerSocket();
        serverSocket.setReuseAddress(true); // survive quick restarts
        serverSocket.bind(new InetSocketAddress(port));

        // Java 21: unlimited virtual threads, each mapped to a carrier thread by the JVM scheduler.
        virtualThreadPool = Executors.newVirtualThreadPerTaskExecutor();
        running = true;

        // The accept loop itself runs in a virtual thread so start() is non-blocking.
        virtualThreadPool.submit(this::acceptLoop);

        LOG.info("[TCPServer] Listening on port " + port + " with virtual-thread-per-connection model.");
    }

    /**
     * Gracefully shut down the server.
     *
     * <ol>
     *   <li>Set {@code running = false} so all session loops exit at next iteration.</li>
     *   <li>Close the {@code ServerSocket} to unblock the accept loop immediately.</li>
     *   <li>Initiate executor shutdown and wait up to 5 s for in-flight sessions.</li>
     * </ol>
     */
    public void stop() {
        running = false;

        // Close server socket to unblock the blocking accept() call.
        try {
            if (serverSocket != null && !serverSocket.isClosed())
                serverSocket.close();
        } catch (IOException e) {
            LOG.warning("[TCPServer] Error closing server socket: " + e.getMessage());
        }

        if (virtualThreadPool != null) {
            virtualThreadPool.shutdown();
            try {
                if (!virtualThreadPool.awaitTermination(5, TimeUnit.SECONDS)) {
                    virtualThreadPool.shutdownNow();
                    LOG.warning("[TCPServer] Forced shutdown after 5 s timeout.");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                virtualThreadPool.shutdownNow();
            }
        }

        LOG.info("[TCPServer] Stopped.");
    }

    public boolean isRunning() { return running; }
    public int     getPort()   { return port;    }

    // -----------------------------------------------------------------------
    // Accept loop — runs in a single virtual thread
    // -----------------------------------------------------------------------

    /**
     * Blocking accept loop: waits for incoming connections and spawns a new
     * virtual thread for each one.
     *
     * <p>If {@code accept()} throws an {@link IOException} and the server is
     * still supposed to be running, the exception is logged and the loop
     * continues — transient OS errors (e.g. EMFILE) should not kill the server.
     * When {@link #stop()} closes the server socket the next {@code accept()}
     * call will throw, {@code running} will be {@code false}, and the loop exits.
     */
    private void acceptLoop() {
        LOG.fine("[TCPServer] Accept loop started.");
        while (running) {
            try {
                Socket client = serverSocket.accept();

                // Low-latency tuning: send response frames immediately, don't buffer.
                client.setTcpNoDelay(true);
                // No read timeout — connections are long-lived (telnet / client SDK).
                client.setSoTimeout(0);

                // Spawn a virtual thread for each connection. Cost: ~2 KB stack.
                virtualThreadPool.submit(new ClientSession(client));

            } catch (IOException e) {
                if (running) {
                    // Log and continue — this might be a transient OS error.
                    LOG.warning("[TCPServer] Accept error (will retry): " + e.getMessage());
                }
                // If !running, the server socket was closed by stop() — exit quietly.
            }
        }
        LOG.fine("[TCPServer] Accept loop exited.");
    }

    // -----------------------------------------------------------------------
    // ClientSession — one instance per connected client, run in virtual thread
    // -----------------------------------------------------------------------

    /**
     * Handles the full lifecycle of a single client connection.
     *
     * <p>The session reads commands line-by-line ({@code readLine()} strips CRLF),
     * passes each line to the shared {@link CommandHandler}, and writes the
     * response back with an appended {@code \r\n}.
     *
     * <p>The session ends when:
     * <ul>
     *   <li>The client closes its end of the connection ({@code readLine()} → null).</li>
     *   <li>An {@link IOException} is raised (broken pipe, reset, etc.).</li>
     *   <li>{@code running} becomes {@code false} (server stopping).</li>
     * </ul>
     */
    private final class ClientSession implements Runnable {

        private final Socket socket;

        ClientSession(Socket socket) {
            this.socket = socket;
        }

        @Override
        public void run() {
            String remote = socket.getRemoteSocketAddress().toString();
            LOG.fine("[TCPServer] Session opened: " + remote);

            try (
                // Use a BufferedReader so readLine() handles both \n and \r\n.
                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(socket.getInputStream()));
                // autoFlush=false: we call flush() manually after each response
                // to avoid a flush on every println(), which would cause a
                // separate write() syscall per character in some JVM implementations.
                PrintWriter writer = new PrintWriter(
                        new BufferedWriter(new OutputStreamWriter(socket.getOutputStream())),
                        /*autoFlush=*/ false)
            ) {
                String line;
                while (running && (line = reader.readLine()) != null) {

                    // Skip blank lines (e.g. telnet sends \r\n on its own sometimes).
                    if (line.isBlank()) continue;

                    // Parse the command (strips leading/trailing whitespace internally).
                    CommandParser.ParsedCommand cmd = CommandParser.parse(line);

                    // Dispatch to the CacheEngine and get a protocol response.
                    String response = handler.handle(cmd);

                    // Write response + CRLF then flush in one syscall.
                    writer.print(response);
                    writer.print(CRLF);
                    writer.flush();
                }

            } catch (IOException e) {
                if (running) {
                    // Broken pipe / connection reset — normal for abruptly closed clients.
                    LOG.fine("[TCPServer] Session error (" + remote + "): " + e.getMessage());
                }
            } finally {
                // Always close the socket to free the OS file descriptor.
                try { socket.close(); } catch (IOException ignored) {}
                LOG.fine("[TCPServer] Session closed: " + remote);
            }
        }
    }
}
