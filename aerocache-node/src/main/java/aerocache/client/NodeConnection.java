package aerocache.client;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

/**
 * A persistent, thread-safe TCP connection to a single AeroCache master node.
 *
 * <h3>Role in the Smart Client</h3>
 * {@link AeroCacheClient} maintains exactly one {@code NodeConnection} per
 * physical master node. Rather than opening a new OS socket for every GET or
 * SET request (which would cost a full TCP 3-way handshake each time),
 * {@code NodeConnection} keeps the socket alive indefinitely and reuses it.
 * On first use or after a broken-pipe event the socket is re-established
 * transparently.
 *
 * <h3>Concurrency model</h3>
 * A single fair {@link ReentrantLock} serialises access to the socket so that
 * multiple application threads sharing one {@code AeroCacheClient} instance
 * do not interleave their request/response pairs.  The lock covers the
 * entire send + readLine cycle atomically.
 *
 * <h3>Connection lifecycle</h3>
 * <pre>
 *   First send()   → socket is null → connect()
 *   Subsequent sends → reuse existing socket
 *   Broken pipe / reset → IOException thrown → socket nulled by caller
 *                         → next send() reconnects transparently
 *   close()        → socket closed, NodeConnection permanently unusable
 * </pre>
 *
 * <h3>TCP tuning</h3>
 * <ul>
 *   <li>{@code TCP_NODELAY = true} — disables Nagle's algorithm; each command
 *       frame is transmitted immediately without waiting to fill a segment.</li>
 *   <li>{@code SO_KEEPALIVE = true} — OS-level keepalive detects dead
 *       connections (e.g. node container killed) faster than a read timeout.</li>
 *   <li>Connect timeout: {@link #CONNECT_TIMEOUT_MS} = 2 000 ms.</li>
 * </ul>
 */
public class NodeConnection implements Closeable {

    private static final Logger LOG = Logger.getLogger(NodeConnection.class.getName());

    /** Maximum time to wait for the TCP handshake to complete. */
    private static final int CONNECT_TIMEOUT_MS = 2_000;

    // -----------------------------------------------------------------------
    // Identity
    // -----------------------------------------------------------------------

    private final String host;
    private final int    port;

    /** Canonical "host:port" string used in log messages and exception details. */
    public final String nodeId;

    // -----------------------------------------------------------------------
    // Socket and I/O streams
    // -----------------------------------------------------------------------

    private Socket        socket;
    private BufferedReader reader;
    private PrintWriter   writer;

    // -----------------------------------------------------------------------
    // Concurrency
    // -----------------------------------------------------------------------

    /**
     * Fair lock: threads acquire in arrival order to prevent starvation when
     * many application threads race to send commands through the same connection.
     */
    private final ReentrantLock lock = new ReentrantLock(/*fair=*/ true);

    /** Set to {@code true} by {@link #close()} — prevents further reconnect attempts. */
    private volatile boolean closed = false;

    // -----------------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------------

    /**
     * @param host hostname or IP of the master cache node (e.g. "aerocache-node-1")
     * @param port TCP port the node's CacheServer listens on (e.g. 6001)
     */
    public NodeConnection(String host, int port) {
        this.host   = host;
        this.port   = port;
        this.nodeId = host + ":" + port;
    }

    // -----------------------------------------------------------------------
    // Public send API
    // -----------------------------------------------------------------------

    /**
     * Send a command to the node and return its single-line response.
     *
     * <p>The method appends {@code \r\n} to {@code command} before writing —
     * callers must NOT include the CRLF terminator themselves.
     *
     * <p>If the socket is not open, {@link #connect()} is called first.
     * If the send fails (broken pipe, connection reset), the socket is
     * invalidated and the exception is rethrown so {@link AeroCacheClient}
     * can implement its retry logic.
     *
     * @param command command line without CRLF (e.g. {@code "GET session:user123"})
     * @return server response without CRLF (e.g. {@code "$Alice"} or {@code "$-1"})
     * @throws IOException if the connection cannot be opened or the send fails
     */
    public String send(String command) throws IOException {
        lock.lock();
        try {
            // Lazy connection: open socket on first use or after invalidation.
            if (!isConnected()) {
                connect();
            }

            try {
                // Write command + CRLF in a single flush to minimise syscalls.
                writer.print(command + "\r\n");
                writer.flush();

                String response = reader.readLine();
                if (response == null) {
                    // Server closed connection.
                    invalidate();
                    throw new IOException("Server at " + nodeId + " closed the connection.");
                }
                return response;

            } catch (IOException e) {
                // Broken pipe, reset, etc. — null the socket so the next
                // call or retry attempt triggers a fresh connect().
                invalidate();
                throw e; // propagate to AeroCacheClient for retry decision
            }

        } finally {
            lock.unlock();
        }
    }

    /** {@code true} if the underlying socket is currently open and connected. */
    public boolean isConnected() {
        return socket != null && socket.isConnected() && !socket.isClosed();
    }

    /** Host this connection points to. */
    public String getHost() { return host; }

    /** Port this connection points to. */
    public int getPort()    { return port; }

    /**
     * Permanently close this connection.
     * After calling {@code close()}, any subsequent {@link #send} call will
     * throw {@link IOException} immediately.
     */
    @Override
    public void close() {
        closed = true;
        lock.lock();
        try {
            invalidate();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public String toString() {
        return "NodeConnection{" + nodeId + ", connected=" + isConnected() + "}";
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /**
     * Open a TCP socket to the node. Called under {@link #lock}.
     *
     * @throws IOException if the connection attempt fails
     */
    private void connect() throws IOException {
        if (closed) {
            throw new IOException("NodeConnection to " + nodeId + " is permanently closed.");
        }

        LOG.info("[NodeConnection] Connecting to " + nodeId + "...");

        Socket s = new Socket();
        s.setTcpNoDelay(true);   // no Nagle buffering — send frames immediately
        s.setKeepAlive(true);    // OS detects dead connections via TCP keepalive
        s.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MS);

        reader = new BufferedReader(new InputStreamReader(s.getInputStream()));
        writer = new PrintWriter(
                new BufferedWriter(new OutputStreamWriter(s.getOutputStream())),
                /*autoFlush=*/ false);
        socket = s;

        LOG.info("[NodeConnection] Connected to " + nodeId);
    }

    /**
     * Silently close all socket resources and null the references.
     * Called under {@link #lock}.
     */
private void invalidate() {
        try { if (reader != null) reader.close(); } catch (IOException ignored) {}
        // PrintWriter.close() does NOT throw IOException in Java 21+
        try { if (writer != null) writer.close(); } catch (Exception ignored) {}
        try { if (socket != null) socket.close(); } catch (IOException ignored) {}
        reader = null;
        writer = null;
        socket = null;
    }
}
