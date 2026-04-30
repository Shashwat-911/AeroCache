package aerocache.net;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

/**
 * Reusable, persistent TCP client for AeroCache inter-node communication.
 *
 * <h3>Design goal — persistent connections</h3>
 * The {@link aerocache.replication.HeartbeatService} sends a PING every 2 seconds.
 * The {@link aerocache.replication.ReplicationManager} sends a REPLICATE command
 * for every SET/DEL on the master. Opening a new TCP socket for each of these
 * would mean hundreds of TCP handshakes per minute per node pair — completely
 * avoidable overhead.
 *
 * <p>This class maintains a <em>single persistent {@link Socket}</em> to a target
 * host:port. It is created on the first {@link #sendCommand} call and kept alive
 * indefinitely. If the socket breaks (broken pipe, connection reset, remote
 * process restart) it is transparently reconnected with exponential backoff on
 * the next call.
 *
 * <h3>Concurrency — single-socket serialisation</h3>
 * A {@link ReentrantLock} ensures that only one thread uses the socket at a time.
 * The lock covers the send + receive pair atomically, so responses are never
 * interleaved between callers. Multiple threads (e.g. replication + heartbeat)
 * that share the same {@code TCPClient} instance will queue up rather than
 * corrupt each other's reads.
 *
 * <h3>Reconnect strategy</h3>
 * On connection failure the client retries up to {@link #maxReconnectAttempts}
 * times with exponential backoff starting at {@code 200 ms}, capped at
 * {@code 5 000 ms}. The backoff sleeps release the virtual thread's carrier,
 * so they do not block a platform thread.
 *
 * <h3>TCP tuning</h3>
 * <ul>
 *   <li>{@code TCP_NODELAY = true} — disable Nagle's algorithm for immediate frame delivery.</li>
 *   <li>{@code SO_KEEPALIVE = true} — OS-level keepalive detects half-open connections.</li>
 *   <li>Connect timeout configured via constructor (default 3 000 ms).</li>
 * </ul>
 */
public class TCPClient implements Closeable {

    private static final Logger LOG = Logger.getLogger(TCPClient.class.getName());

    /** Initial reconnect backoff in milliseconds. Doubles on each failed attempt. */
    private static final long INITIAL_BACKOFF_MS = 200L;

    /** Maximum reconnect backoff — prevents runaway waiting. */
    private static final long MAX_BACKOFF_MS = 5_000L;

    // -----------------------------------------------------------------------
    // Configuration (immutable after construction)
    // -----------------------------------------------------------------------

    private final String host;
    private final int    port;
    private final int    connectTimeoutMs;
    private final int    maxReconnectAttempts;

    // -----------------------------------------------------------------------
    // Persistent socket and its I/O streams
    // -----------------------------------------------------------------------

    private Socket       socket;
    private BufferedReader reader;
    private PrintWriter    writer;

    // -----------------------------------------------------------------------
    // Concurrency — exclusive lock over the socket for the send+receive pair
    // -----------------------------------------------------------------------

    /**
     * Fair lock: threads acquire the socket in arrival order.
     * Fairness prevents starvation of the heartbeat thread when the
     * replication queue fires many writes in rapid succession.
     */
    private final ReentrantLock lock = new ReentrantLock(/*fair=*/ true);

    // -----------------------------------------------------------------------
    // Lifecycle flag
    // -----------------------------------------------------------------------

    /**
     * Set to {@code true} by {@link #close()}. Once closed, all subsequent
     * {@link #sendCommand} calls immediately throw {@link IOException} instead
     * of attempting an infinite reconnect loop.
     */
    private volatile boolean closed = false;

    // -----------------------------------------------------------------------
    // Constructors
    // -----------------------------------------------------------------------

    /**
     * Create a client with sensible defaults (3 s connect timeout, 5 retries).
     *
     * @param host target hostname or IP (e.g. "aerocache-node-2")
     * @param port target TCP port (e.g. 6002)
     */
    public TCPClient(String host, int port) {
        this(host, port, 3_000, 5);
    }

    /**
     * @param host                 target hostname or IP
     * @param port                 target TCP port
     * @param connectTimeoutMs     socket connect timeout in milliseconds
     * @param maxReconnectAttempts how many times to retry before throwing
     */
    public TCPClient(String host, int port, int connectTimeoutMs, int maxReconnectAttempts) {
        this.host                 = host;
        this.port                 = port;
        this.connectTimeoutMs     = connectTimeoutMs;
        this.maxReconnectAttempts = maxReconnectAttempts;
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /**
     * Send a command and return the server's single-line response.
     *
     * <p>The method appends {@code \r\n} to {@code command} before sending —
     * callers must NOT include a CRLF terminator themselves.
     *
     * <p>If the underlying socket is not connected (first call, or broken pipe)
     * this method calls {@link #ensureConnected()} which may block for up to
     * {@code maxReconnectAttempts * MAX_BACKOFF_MS} milliseconds.
     *
     * @param command command line WITHOUT trailing \r\n
     * @return server response WITHOUT trailing \r\n
     * @throws IOException if the connection cannot be established after all retries,
     *                     or if the server closes the connection mid-request
     */
    public String sendCommand(String command) throws IOException {
        lock.lock();
        try {
            ensureConnected();

            try {
                // Write command + CRLF, flush immediately (writer is NOT auto-flushed).
                writer.print(command + "\r\n");
                writer.flush();

                // Block until the server writes a complete response line.
                String response = reader.readLine();
                if (response == null) {
                    // Null means the server closed the connection.
                    invalidateSocket();
                    throw new IOException("Server at " + host + ":" + port + " closed the connection.");
                }
                return response;

            } catch (IOException e) {
                // Mark the socket as broken so the next call reconnects.
                invalidateSocket();
                throw new IOException(
                        "Send failed to " + host + ":" + port + " — " + e.getMessage(), e);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Fire-and-forget variant: sends a command without waiting for a response.
     *
     * <p>Used by {@link aerocache.replication.ReplicationManager} for asynchronous
     * slave replication where the master does not need to block on the slave's ACK.
     * If the send fails the socket is invalidated and an error is logged —
     * the caller is not interrupted.
     *
     * @param command command line WITHOUT trailing \r\n
     */
    public void sendCommandAsync(String command) {
        lock.lock();
        try {
            ensureConnected();
            writer.print(command + "\r\n");
            writer.flush();
        } catch (IOException e) {
            LOG.warning("[TCPClient] Async send failed to " + host + ":" + port
                    + " — " + e.getMessage());
            invalidateSocket();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Send a PING and return {@code true} if {@code +PONG} is received.
     *
     * <p>Called by {@link aerocache.replication.HeartbeatService} every 2 seconds
     * over the persistent connection. A {@code false} return (timeout, broken pipe,
     * unexpected response) increments the missed-heartbeat counter.
     *
     * @return {@code true} iff the remote node responded with {@code +PONG}
     */
    public boolean ping() {
        try {
            String response = sendCommand("PING");
            return "+PONG".equals(response);
        } catch (IOException e) {
            LOG.fine("[TCPClient] PING failed to " + host + ":" + port + " — " + e.getMessage());
            return false;
        }
    }

    /**
     * @return {@code true} if the socket is currently open and connected
     */
    public boolean isConnected() {
        return socket != null && !socket.isClosed() && socket.isConnected();
    }

    /**
     * Permanently close this client. After this call, {@link #sendCommand}
     * will throw {@link IOException} immediately without attempting reconnects.
     */
    @Override
    public void close() {
        closed = true;
        lock.lock();
        try {
            invalidateSocket();
        } finally {
            lock.unlock();
        }
        LOG.info("[TCPClient] Closed connection to " + host + ":" + port);
    }

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    public String getHost() { return host; }
    public int    getPort() { return port; }

    @Override
    public String toString() {
        return "TCPClient{" + host + ":" + port + ", connected=" + isConnected() + "}";
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /**
     * Ensure the socket is open and ready. If not, attempt to connect with
     * exponential backoff. Called while holding {@link #lock}.
     *
     * @throws IOException if all reconnect attempts fail
     */
    private void ensureConnected() throws IOException {
        if (isConnected()) return;

        if (closed) {
            throw new IOException("TCPClient to " + host + ":" + port + " is permanently closed.");
        }

        long backoffMs  = INITIAL_BACKOFF_MS;
        IOException lastException = null;

        for (int attempt = 1; attempt <= maxReconnectAttempts; attempt++) {
            LOG.info("[TCPClient] Connecting to " + host + ":" + port
                    + " (attempt " + attempt + "/" + maxReconnectAttempts + ")");
            try {
                Socket s = new Socket();

                // Disable Nagle — small command frames must be sent without delay.
                s.setTcpNoDelay(true);

                // OS-level keepalive — detects dead connections without application pings.
                s.setKeepAlive(true);

                // Respect the configured connect timeout (avoids hanging forever on LAN).
                s.connect(new InetSocketAddress(host, port), connectTimeoutMs);

                // Wrap the streams.
                reader = new BufferedReader(new InputStreamReader(s.getInputStream()));
                // BufferedWriter + manual flush → one write() syscall per command.
                writer = new PrintWriter(
                        new BufferedWriter(new OutputStreamWriter(s.getOutputStream())),
                        /*autoFlush=*/ false);

                socket = s;
                LOG.info("[TCPClient] Connected to " + host + ":" + port);
                return; // ← success

            } catch (IOException e) {
                lastException = e;
                LOG.warning("[TCPClient] Attempt " + attempt + " failed: " + e.getMessage());

                if (attempt < maxReconnectAttempts) {
                    try {
                        Thread.sleep(backoffMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Interrupted during reconnect backoff", ie);
                    }
                    // Exponential backoff, capped at MAX_BACKOFF_MS.
                    backoffMs = Math.min(backoffMs * 2, MAX_BACKOFF_MS);
                }
            }
        }

        throw new IOException(
                "Failed to connect to " + host + ":" + port
                        + " after " + maxReconnectAttempts + " attempts",
                lastException);
    }

    /**
     * Silently close all socket resources and null the references so
     * {@link #isConnected()} returns {@code false} and the next call
     * to {@link #ensureConnected()} triggers a fresh reconnect.
     *
     * Must be called while holding {@link #lock}.
     */
    private void invalidateSocket() {
        try { if (reader != null) reader.close(); } catch (IOException ignored) {}
        try { if (writer != null) writer.close(); } catch (Exception ignored) {}
        try { if (socket != null) socket.close(); } catch (IOException ignored) {}
        reader = null;
        writer = null;
        socket = null;
    }
}
