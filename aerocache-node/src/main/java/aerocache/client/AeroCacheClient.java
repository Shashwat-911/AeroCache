package aerocache.client;

import aerocache.ring.CacheNode;
import aerocache.ring.ConsistentHashRing;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Smart routing client for the AeroCache distributed cache cluster.
 *
 * <h3>What makes it "smart"</h3>
 * A naive client would send every request to a single gateway node and let
 * that node proxy the request to the correct shard.  This introduces:
 * <ul>
 *   <li>A single point of failure.</li>
 *   <li>An extra network hop for every operation.</li>
 *   <li>A bottleneck as cluster throughput scales.</li>
 * </ul>
 * {@code AeroCacheClient} eliminates all three by embedding the exact same
 * consistent-hashing algorithm ({@link ConsistentHashRing} — MD5 + 150
 * virtual nodes per physical node) that the server cluster uses internally.
 * The client resolves <em>locally</em> which master owns any given key and
 * opens a direct TCP connection to that master.  Zero proxying, zero extra hop.
 *
 * <h3>Connection pooling</h3>
 * One {@link NodeConnection} is created per physical master node and kept
 * open for the lifetime of the client.  Each {@code NodeConnection} wraps a
 * persistent socket with a {@code ReentrantLock} so multiple calling threads
 * can share a single {@code AeroCacheClient} instance safely.
 *
 * <h3>Fault tolerance — single retry</h3>
 * If the first send to a node fails (broken pipe, connection reset, node
 * restarted):
 * <ol>
 *   <li>The {@code NodeConnection} automatically invalidates the dead socket.</li>
 *   <li>The client logs a warning and retries the same command once
 *       ({@code NodeConnection.send()} will re-connect on the second call).</li>
 *   <li>If the retry also fails, the client gives up and throws
 *       {@link AeroCacheConnectionException} — a checked exception that forces
 *       the application to handle the failure explicitly.</li>
 * </ol>
 *
 * <h3>Wire protocol</h3>
 * Uses the same CRLF text protocol as the server:
 * <pre>
 *   GET &lt;key&gt;\r\n   → "$&lt;value&gt;" (hit) | "$-1" (miss)
 *   SET &lt;key&gt; &lt;value&gt;\r\n → "+OK"
 *   DEL &lt;key&gt;\r\n   → "+OK" (existed) | "$-1" (not found)
 * </pre>
 *
 * <h3>Usage example</h3>
 * <pre>
 *   List&lt;String&gt; nodes = List.of("aerocache-node-1:6001", "aerocache-node-3:6003");
 *   try (AeroCacheClient client = new AeroCacheClient(nodes)) {
 *       client.set("user:42", "{\"name\":\"Alice\"}");
 *       String json = client.get("user:42");   // → "{\"name\":\"Alice\"}"
 *       client.delete("user:42");
 *   }
 * </pre>
 *
 * <h3>Thread safety</h3>
 * The ring lookup ({@link ConsistentHashRing#getNodeForKey}) is protected by
 * the ring-level read lock inside {@link aerocache.ring.HashRingManager}, but
 * here we use {@link ConsistentHashRing} directly.  The ring is immutable after
 * construction (no dynamic node add/remove on the client), so concurrent reads
 * without a lock are safe — {@code TreeMap} reads without structural modification
 * do not require synchronisation.
 */
public class AeroCacheClient implements Closeable {

    private static final Logger LOG = Logger.getLogger(AeroCacheClient.class.getName());

    // -----------------------------------------------------------------------
    // Wire-protocol response prefixes (must match CommandHandler constants)
    // -----------------------------------------------------------------------

    private static final String RESP_OK      = "+OK";
    private static final String RESP_NULL    = "$-1";
    private static final String RESP_BULK    = "$";    // prefix of a hit value
    private static final String RESP_ERR     = "-ERR";

    // -----------------------------------------------------------------------
    // Routing ring and connection pool
    // -----------------------------------------------------------------------

    /**
     * Client-side consistent hash ring — populated once in the constructor
     * from the seed node list.  Immutable after construction; concurrent reads
     * are safe without a lock.
     */
    private final ConsistentHashRing ring;

    /**
     * Connection pool: nodeId ("host:port") → persistent {@link NodeConnection}.
     * Populated once during construction; the map itself is never mutated after
     * that so concurrent reads need no synchronisation.
     */
    private final Map<String, NodeConnection> connections;

    // -----------------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------------

    /**
     * Build a smart client pre-configured with the cluster's master nodes.
     *
     * <p>The client immediately adds all seed nodes to its local hash ring and
     * creates a {@link NodeConnection} for each.  Sockets are opened lazily on
     * the first actual request.
     *
     * @param seedNodes list of master node addresses in {@code "host:port"} format.
     *                  Example: {@code List.of("aerocache-node-1:6001", "aerocache-node-3:6003")}
     * @throws IllegalArgumentException if {@code seedNodes} is null or empty, or
     *                                  if any entry is not in {@code "host:port"} format
     */
    public AeroCacheClient(List<String> seedNodes) {
        if (seedNodes == null || seedNodes.isEmpty()) {
            throw new IllegalArgumentException(
                    "AeroCacheClient requires at least one seed node.");
        }

        // Use the default 150 virtual nodes — same as the server cluster.
        this.ring        = new ConsistentHashRing();
        this.connections = new LinkedHashMap<>();

        for (String seedNode : seedNodes) {
            // ── Parse "host:port" ──────────────────────────────────────────
            String[] parts = seedNode.split(":", 2);
            if (parts.length != 2) {
                throw new IllegalArgumentException(
                        "Invalid seed node format (expected host:port): '" + seedNode + "'");
            }
            String host;
            int    port;
            try {
                host = parts[0].trim();
                port = Integer.parseInt(parts[1].trim());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "Invalid port in seed node '" + seedNode + "'", e);
            }

            // ── Register on the client-side ring ──────────────────────────
            // Role is MASTER — the client only routes to masters.
            CacheNode node = new CacheNode(host, port, CacheNode.Role.MASTER);
            ring.addNode(node);

            // ── Create a persistent connection (socket opened lazily) ──────
            NodeConnection conn = new NodeConnection(host, port);
            connections.put(node.getNodeId(), conn);

            LOG.info("[AeroCacheClient] Registered seed node: " + node.getNodeId());
        }

        LOG.info("[AeroCacheClient] Ready. Seed nodes=" + seedNodes.size()
                + ", ring v-nodes=" + ring.virtualNodeCount());
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /**
     * Retrieve the value associated with {@code key}.
     *
     * <p>The key is hashed locally to determine the responsible master node.
     * The request goes directly to that node — zero proxying.
     *
     * @param key the cache key
     * @return the cached value string, or {@code null} on a cache miss
     * @throws AeroCacheConnectionException if the target node is unreachable
     *                                      after one reconnect attempt
     */
    public String get(String key) throws AeroCacheConnectionException {
        String response = sendWithRetry(key, "GET " + key);

        if (RESP_NULL.equals(response)) {
            return null; // cache miss
        }
        if (response.startsWith(RESP_BULK)) {
            // Strip the leading '$' prefix to get the raw value.
            return response.substring(1);
        }
        if (response.startsWith(RESP_ERR)) {
            LOG.warning("[AeroCacheClient] Server error on GET " + key + ": " + response);
            return null;
        }
        // Unexpected response — log and return null defensively.
        LOG.warning("[AeroCacheClient] Unexpected GET response for key=" + key
                + ": '" + response + "'");
        return null;
    }

    /**
     * Insert or update a key-value pair in the cache.
     *
     * <p>The command is routed directly to the master node responsible for
     * {@code key}.  That master's {@link aerocache.replication.ReplicationManager}
     * will asynchronously propagate the write to its slave.
     *
     * @param key   the cache key (must not contain CRLF)
     * @param value the value to store (may contain spaces; must not contain CRLF)
     * @throws AeroCacheConnectionException if the target node is unreachable
     */
    public void set(String key, String value) throws AeroCacheConnectionException {
        String response = sendWithRetry(key, "SET " + key + " " + value);

        if (!RESP_OK.equals(response)) {
            LOG.warning("[AeroCacheClient] Unexpected SET response for key=" + key
                    + ": '" + response + "'");
        }
    }

    /**
     * Delete a key from the cache.
     *
     * @param key the cache key to remove
     * @throws AeroCacheConnectionException if the target node is unreachable
     */
    public void delete(String key) throws AeroCacheConnectionException {
        // $-1 is returned if the key didn't exist — not an error from the client's POV.
        sendWithRetry(key, "DEL " + key);
    }

    /**
     * Ping all registered nodes and return the IDs of healthy ones.
     * Useful for diagnostics and smoke tests.
     *
     * @return unmodifiable list of "host:port" strings that responded with PONG
     */
    public List<String> pingAll() {
        List<String> alive = new ArrayList<>();
        for (Map.Entry<String, NodeConnection> entry : connections.entrySet()) {
            try {
                String resp = entry.getValue().send("PING");
                if ("+PONG".equals(resp)) alive.add(entry.getKey());
            } catch (IOException e) {
                LOG.warning("[AeroCacheClient] PING failed for " + entry.getKey()
                        + ": " + e.getMessage());
            }
        }
        return Collections.unmodifiableList(alive);
    }

    /**
     * Close all persistent connections to cache nodes.
     *
     * <p>Call this in a try-with-resources block or in application shutdown
     * to release OS file descriptors promptly.
     */
    @Override
    public void close() {
        for (NodeConnection conn : connections.values()) {
            conn.close();
        }
        LOG.info("[AeroCacheClient] All connections closed.");
    }

    // -----------------------------------------------------------------------
    // Routing and retry logic
    // -----------------------------------------------------------------------

    /**
     * Hash {@code key} → node → send {@code command} → return response string.
     * On first-attempt failure, retries once (the invalidated socket reconnects
     * transparently inside {@link NodeConnection#send}).  On second failure,
     * throws {@link AeroCacheConnectionException}.
     *
     * @param key     the cache key (used only for ring routing, not the command)
     * @param command full protocol command line (e.g. {@code "GET session:user42"})
     * @return the raw server response string (without CRLF)
     * @throws AeroCacheConnectionException after two consecutive failures
     */
    private String sendWithRetry(String key, String command) throws AeroCacheConnectionException {
        // ── Step 1: Route locally ─────────────────────────────────────────
        CacheNode targetNode = ring.getNodeForKey(key);
        if (targetNode == null) {
            throw new AeroCacheConnectionException("ring",
                    "No nodes available in the hash ring. "
                            + "Was AeroCacheClient constructed with at least one seed node?");
        }

        NodeConnection conn = connections.get(targetNode.getNodeId());
        if (conn == null) {
            // Should never happen — connections is populated from the same set as the ring.
            throw new AeroCacheConnectionException(targetNode.getNodeId(),
                    "No NodeConnection found for this node (internal error).");
        }

        // ── Step 2: First attempt ─────────────────────────────────────────
        try {
            return conn.send(command);
        } catch (IOException firstEx) {
            LOG.warning("[AeroCacheClient] First attempt to " + targetNode.getNodeId()
                    + " failed (" + firstEx.getMessage() + "). Retrying once...");
        }

        // ── Step 3: Single retry ──────────────────────────────────────────
        // NodeConnection.send() calls connect() automatically because the
        // prior failure nulled the socket via invalidate().
        try {
            return conn.send(command);
        } catch (IOException retryEx) {
            // Both attempts failed — declare the node dead and surface the error.
            LOG.severe("[AeroCacheClient] Retry also failed for "
                    + targetNode.getNodeId() + ": " + retryEx.getMessage());
            throw new AeroCacheConnectionException(
                    targetNode.getNodeId(),
                    "Both the initial attempt and the single retry failed. "
                            + "Node is assumed dead. Cause: " + retryEx.getMessage(),
                    retryEx);
        }
    }
}
