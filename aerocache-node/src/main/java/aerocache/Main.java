package aerocache;

import aerocache.core.AIEvictionPolicy;
import aerocache.core.CacheEngine;
import aerocache.net.TCPClient;
import aerocache.net.TCPServer;
import aerocache.replication.HeartbeatService;
import aerocache.replication.PromotionHandler;
import aerocache.replication.ReplicationManager;
import aerocache.replication.SplitBrainGuard;
import aerocache.ring.CacheNode;
import aerocache.ring.HashRingManager;

import java.io.IOException;
import java.util.logging.Logger;

/**
 * AeroCache node entry point.
 *
 * <h3>Configuration via environment variables</h3>
 * All parameters are read from environment variables so Docker Compose can
 * configure each container independently without rebuilding the image.
 *
 * <pre>
 * Variable          Default    Description
 * ─────────────────────────────────────────────────────────────────────
 * CACHE_PORT        6001       TCP port this node listens on
 * CACHE_ROLE        MASTER     MASTER or SLAVE
 * CACHE_CAPACITY    1000       Maximum number of entries in the cache
 * NODE_HOST         localhost  This node's hostname (used as ring ID)
 *
 * PEER_HOST         (none)     Hostname of the paired peer node
 * PEER_PORT         (none)     Port of the paired peer node
 *
 * AI_HOST           localhost  Hostname of the Python AI microservice
 * AI_PORT           8000       Port of the Python AI microservice
 * AI_ENABLED        true       Set to "false" to force pure-LRU mode
 * </pre>
 *
 * <h3>Startup sequence for a MASTER node</h3>
 * <ol>
 *   <li>Create {@link CacheEngine} with {@link AIEvictionPolicy} (or LRU if AI disabled).</li>
 *   <li>Register self and slave in {@link HashRingManager}.</li>
 *   <li>Start {@link TCPServer} — begins accepting client connections immediately.</li>
 *   <li>Start {@link ReplicationManager} — begins draining the replication queue to the slave.</li>
 *   <li>Register JVM shutdown hook for graceful teardown.</li>
 * </ol>
 *
 * <h3>Startup sequence for a SLAVE node</h3>
 * <ol>
 *   <li>Create {@link CacheEngine} with LRU (slave doesn't need AI eviction — it
 *       only applies writes forwarded by the master).</li>
 *   <li>Register self and master in {@link HashRingManager}.</li>
 *   <li>Start {@link TCPServer} — accepts REPLICATE commands from the master
 *       and PING commands from the master's HeartbeatService.</li>
 *   <li>Start {@link HeartbeatService} — monitors the master via periodic PING.</li>
 *   <li>Register JVM shutdown hook.</li>
 * </ol>
 *
 * <h3>Shutdown hook</h3>
 * {@link Runtime#addShutdownHook} registers a virtual thread that runs when the
 * JVM receives SIGTERM (e.g. {@code docker compose down}). The hook:
 * <ol>
 *   <li>Stops {@link TCPServer} — rejects new connections, waits for in-flight sessions.</li>
 *   <li>Stops {@link ReplicationManager} — gives the drainer thread up to 3 s to flush
 *       pending replication commands before the JVM exits.</li>
 *   <li>Stops {@link HeartbeatService} — cancels the scheduler cleanly.</li>
 *   <li>Shuts down {@link CacheEngine} — stops the TTL background cleaner.</li>
 * </ol>
 */
public class Main {

    private static final Logger LOG = Logger.getLogger(Main.class.getName());

    // -----------------------------------------------------------------------
    // Environment variable names
    // -----------------------------------------------------------------------
    private static final String ENV_PORT        = "CACHE_PORT";
    private static final String ENV_ROLE        = "CACHE_ROLE";
    private static final String ENV_CAPACITY    = "CACHE_CAPACITY";
    private static final String ENV_NODE_HOST   = "NODE_HOST";
    private static final String ENV_PEER_HOST   = "PEER_HOST";
    private static final String ENV_PEER_PORT   = "PEER_PORT";
    private static final String ENV_AI_HOST     = "AI_HOST";
    private static final String ENV_AI_PORT     = "AI_PORT";
    private static final String ENV_AI_ENABLED  = "AI_ENABLED";

    // -----------------------------------------------------------------------
    // Defaults
    // -----------------------------------------------------------------------
    private static final int    DEFAULT_PORT     = 6001;
    private static final String DEFAULT_ROLE     = "MASTER";
    private static final int    DEFAULT_CAPACITY = 1000;
    private static final String DEFAULT_HOST     = "localhost";
    private static final String DEFAULT_AI_HOST  = "localhost";
    private static final int    DEFAULT_AI_PORT  = 8000;

    // -----------------------------------------------------------------------
    // Main
    // -----------------------------------------------------------------------

    public static void main(String[] args) throws IOException {

        // ── 1. Read configuration ───────────────────────────────────────────
        int    port      = intEnv(ENV_PORT,     DEFAULT_PORT);
        String roleStr   = strEnv(ENV_ROLE,     DEFAULT_ROLE).toUpperCase();
        int    capacity  = intEnv(ENV_CAPACITY, DEFAULT_CAPACITY);
        String nodeHost  = strEnv(ENV_NODE_HOST, DEFAULT_HOST);
        String aiHost    = strEnv(ENV_AI_HOST,   DEFAULT_AI_HOST);
        int    aiPort    = intEnv(ENV_AI_PORT,   DEFAULT_AI_PORT);
        boolean aiEnabled = Boolean.parseBoolean(strEnv(ENV_AI_ENABLED, "true"));

        CacheNode.Role role;
        try {
            role = CacheNode.Role.valueOf(roleStr);
        } catch (IllegalArgumentException e) {
            System.err.println("[Main] Invalid CACHE_ROLE='" + roleStr + "'. Must be MASTER or SLAVE.");
            System.exit(1);
            return;
        }

        // ── 2. Print startup banner ─────────────────────────────────────────
        printBanner(nodeHost, port, role, capacity, aiHost, aiPort, aiEnabled);

        // ── 3. Build the local CacheNode ────────────────────────────────────
        CacheNode self = new CacheNode(nodeHost, port, role);

        // ── 4. Build the CacheEngine with the appropriate eviction policy ───
        CacheEngine<String, String> engine = new CacheEngine<>(capacity);

        if (role == CacheNode.Role.MASTER && aiEnabled) {
            engine.setEvictionPolicy(new AIEvictionPolicy<>(aiHost, aiPort));
            LOG.info("[Main] Eviction policy: AI-driven (fallback: LRU). AI=" + aiHost + ":" + aiPort);
        } else {
            // Slaves only apply replicated writes — LRU is the right policy.
            // Also used when AI is explicitly disabled via ENV.
            LOG.info("[Main] Eviction policy: standard LRU.");
        }

        // ── 5. Build the HashRingManager ────────────────────────────────────
        HashRingManager ringManager = new HashRingManager();
        ringManager.addNode(self);

        // ── 6. Start the TCP server ─────────────────────────────────────────
        TCPServer server = new TCPServer(port, engine);
        server.start();
        LOG.info("[Main] TCPServer started on port " + port);

        // ── 7. Role-specific subsystems ─────────────────────────────────────
        ReplicationManager replicationManager = null;
        HeartbeatService   heartbeatService   = null;

        String peerHost = strEnv(ENV_PEER_HOST, null);
        int    peerPort = intEnv(ENV_PEER_PORT, -1);
        boolean hasPeer = peerHost != null && peerPort > 0;

        if (hasPeer) {
            CacheNode peer = new CacheNode(peerHost, peerPort,
                    role == CacheNode.Role.MASTER ? CacheNode.Role.SLAVE : CacheNode.Role.MASTER);
            ringManager.addNode(peer);

            if (role == CacheNode.Role.MASTER) {
                // MASTER → start replication pipeline to the slave.
                ringManager.registerReplicationPair(self, peer);
                TCPClient slaveClient = new TCPClient(peerHost, peerPort);
                replicationManager = new ReplicationManager(slaveClient);
                replicationManager.start();
                LOG.info("[Main] ReplicationManager started → slave at " + peerHost + ":" + peerPort);

            } else {
                // SLAVE → start heartbeat monitor watching the master.
                TCPClient masterClient     = new TCPClient(peerHost, peerPort);
                SplitBrainGuard guard      = new SplitBrainGuard();
                PromotionHandler promoter  = new PromotionHandler();
                heartbeatService = new HeartbeatService(
                        masterClient, self, peer, ringManager, promoter, guard);
                heartbeatService.start();
                LOG.info("[Main] HeartbeatService started → monitoring master at "
                        + peerHost + ":" + peerPort);
            }

        } else {
            LOG.warning("[Main] No PEER_HOST/PEER_PORT configured — running as a standalone node "
                    + "(no replication, no heartbeat).");
        }

        // ── 8. Register JVM shutdown hook ───────────────────────────────────
        final ReplicationManager finalReplMgr    = replicationManager;
        final HeartbeatService   finalHeartbeat  = heartbeatService;

        Runtime.getRuntime().addShutdownHook(
            Thread.ofVirtual().name("aerocache-shutdown-hook").unstarted(() -> {
                LOG.info("[Main] Shutdown hook triggered — beginning graceful teardown.");

                // 8a. Stop accepting new connections first.
                server.stop();

                // 8b. Flush the replication queue — give the drainer thread
                //     up to 3 seconds to send pending commands before the
                //     JVM exits. Commands still in the queue after 3 s are
                //     dropped and counted in the log.
                if (finalReplMgr != null) {
                    int pending = finalReplMgr.getPendingCount();
                    if (pending > 0) {
                        LOG.info("[Main] Flushing " + pending + " pending replication command(s)...");
                    }
                    finalReplMgr.stop();
                }

                // 8c. Stop the heartbeat scheduler cleanly.
                if (finalHeartbeat != null) {
                    finalHeartbeat.stop();
                }

                // 8d. Stop the CacheEngine background TTL cleaner.
                engine.shutdown();

                LOG.info("[Main] Graceful shutdown complete. Goodbye.");
            })
        );

        LOG.info("[Main] AeroCache node " + self.getNodeId()
                + " [" + role + "] is UP and ready to serve requests.");

        // ── 9. Block the main thread ─────────────────────────────────────────
        // TCPServer runs its accept loop in a background virtual thread.
        // We park the main thread here so the JVM stays alive. The shutdown
        // hook above will terminate everything cleanly on SIGTERM.
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /** Read a String environment variable, returning {@code defaultValue} if absent. */
    private static String strEnv(String name, String defaultValue) {
        String val = System.getenv(name);
        return (val != null && !val.isBlank()) ? val.trim() : defaultValue;
    }

    /** Read an integer environment variable, returning {@code defaultValue} if absent or invalid. */
    private static int intEnv(String name, int defaultValue) {
        String val = System.getenv(name);
        if (val == null || val.isBlank()) return defaultValue;
        try {
            return Integer.parseInt(val.trim());
        } catch (NumberFormatException e) {
            LOG.warning("[Main] Invalid integer for " + name + "='" + val
                    + "' — using default " + defaultValue);
            return defaultValue;
        }
    }

    /** Print an ASCII startup banner to stderr (visible in docker compose logs). */
    private static void printBanner(String host, int port, CacheNode.Role role,
                                    int capacity, String aiHost, int aiPort, boolean aiEnabled) {
        String bar = "─".repeat(55);
        System.err.println(bar);
        System.err.println("  AeroCache Node  —  Distributed In-Memory Cache");
        System.err.println(bar);
        System.err.println("  Node     : " + host + ":" + port);
        System.err.println("  Role     : " + role);
        System.err.println("  Capacity : " + capacity + " entries");
        System.err.println("  AI svc   : " + (aiEnabled ? aiHost + ":" + aiPort : "DISABLED (pure LRU)"));
        System.err.println(bar);
    }
}
