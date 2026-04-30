package aerocache.ring;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

/**
 * Thread-safe facade over {@link ConsistentHashRing}.
 *
 * <h3>Why a separate manager?</h3>
 * {@link ConsistentHashRing} intentionally has no internal locking — it is a
 * pure data structure. {@code HashRingManager} wraps it with a
 * {@link ReentrantReadWriteLock} so that:
 * <ul>
 *   <li>Many threads can call {@link #getNodeForKey} concurrently under the
 *       <em>read</em> lock (high-throughput routing).</li>
 *   <li>Topology changes ({@link #addNode}, {@link #removeNode},
 *       {@link #markNodeHealthy}, {@link #markNodeUnhealthy}) are serialised
 *       under the exclusive <em>write</em> lock.</li>
 * </ul>
 *
 * <h3>Physical node registry</h3>
 * The manager also maintains a secondary {@code Map<String, CacheNode>} keyed by
 * node ID ({@code "host:port"}) so callers can look up a node by address without
 * having to walk the ring.
 *
 * <h3>Replication topology</h3>
 * The manager is aware of master–slave pairs: {@link #getSlaveFor(CacheNode)}
 * and {@link #getMasterFor(CacheNode)} support the replication layer's promotion
 * and heartbeat logic.
 *
 * <h3>Failure handling</h3>
 * When a node is marked unhealthy via {@link #markNodeUnhealthy}, its virtual
 * nodes remain on the ring but {@link ConsistentHashRing#getNodeForKey} will
 * skip them (because {@link CacheNode#isHealthy()} returns {@code false}).
 * This avoids an expensive ring re-layout on every transient failure, and the
 * node snaps back in as soon as {@link #markNodeHealthy} is called.
 */
public class HashRingManager {

    private static final Logger LOG = Logger.getLogger(HashRingManager.class.getName());

    // -----------------------------------------------------------------------
    // The ring (not thread-safe by itself — protected by rwLock below)
    // -----------------------------------------------------------------------
    private final ConsistentHashRing ring;

    // -----------------------------------------------------------------------
    // Concurrency
    // -----------------------------------------------------------------------
    private final ReentrantReadWriteLock rwLock    = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock  readLock  = rwLock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = rwLock.writeLock();

    // -----------------------------------------------------------------------
    // Physical node registry — nodeId ("host:port") → CacheNode
    // -----------------------------------------------------------------------
    private final Map<String, CacheNode> physicalNodes = new LinkedHashMap<>();

    // -----------------------------------------------------------------------
    // Master → Slave mapping (for the replication layer)
    // -----------------------------------------------------------------------
    private final Map<String, CacheNode> masterToSlave = new LinkedHashMap<>();
    private final Map<String, CacheNode> slaveToMaster = new LinkedHashMap<>();

    // -----------------------------------------------------------------------
    // Constructors
    // -----------------------------------------------------------------------

    /** Create a manager with the default 150 virtual nodes per physical node. */
    public HashRingManager() {
        this.ring = new ConsistentHashRing();
    }

    /** Create a manager with a custom virtual-node count (useful for testing). */
    public HashRingManager(int virtualNodes) {
        this.ring = new ConsistentHashRing(virtualNodes);
    }

    // -----------------------------------------------------------------------
    // Topology mutation — write-lock required
    // -----------------------------------------------------------------------

    /**
     * Add a physical node to the ring and place its virtual nodes.
     *
     * <p>If a node with the same host:port already exists it is replaced
     * (idempotent — safe to call on cluster restart).
     *
     * @param node the node to add
     */
    public void addNode(CacheNode node) {
        writeLock.lock();
        try {
            String id = node.getNodeId();
            // Remove stale entry if present (handles node restart scenario).
            if (physicalNodes.containsKey(id)) {
                ring.removeNode(physicalNodes.get(id));
                LOG.info("[HashRingManager] Replacing existing node " + id);
            }
            ring.addNode(node);
            physicalNodes.put(id, node);
            LOG.info("[HashRingManager] Added node " + node
                    + " → ring now has " + ring.virtualNodeCount() + " v-nodes.");
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Remove a physical node and all its virtual nodes from the ring.
     *
     * <p>Keys previously owned by this node automatically reroute to the next
     * clockwise healthy node — no explicit key migration is required.
     *
     * @param node the node to remove
     */
    public void removeNode(CacheNode node) {
        writeLock.lock();
        try {
            ring.removeNode(node);
            physicalNodes.remove(node.getNodeId());
            // Clean up replication mappings.
            masterToSlave.remove(node.getNodeId());
            slaveToMaster.remove(node.getNodeId());
            LOG.info("[HashRingManager] Removed node " + node.getNodeId()
                    + " → ring now has " + ring.virtualNodeCount() + " v-nodes.");
        } finally {
            writeLock.unlock();
        }
    }

    // -----------------------------------------------------------------------
    // Replication topology registration
    // -----------------------------------------------------------------------

    /**
     * Register a master–slave pair so the replication and heartbeat layers
     * can query them without scanning the full physical-node list.
     *
     * @param master the master node
     * @param slave  the dedicated slave/replica for {@code master}
     */
    public void registerReplicationPair(CacheNode master, CacheNode slave) {
        writeLock.lock();
        try {
            masterToSlave.put(master.getNodeId(), slave);
            slaveToMaster.put(slave.getNodeId(), master);
            LOG.info("[HashRingManager] Replication pair registered: "
                    + master.getNodeId() + " → " + slave.getNodeId());
        } finally {
            writeLock.unlock();
        }
    }

    // -----------------------------------------------------------------------
    // Key routing — read-lock (hot path)
    // -----------------------------------------------------------------------

    /**
     * Route {@code key} to its responsible {@link CacheNode}.
     *
     * <p>This is the primary hot-path method called by every GET and SET request.
     * It holds only the <em>read</em> lock, so many threads can route concurrently.
     *
     * <p>Steps (all O(log N)):
     * <ol>
     *   <li>MD5-hash the key to a 64-bit ring position.</li>
     *   <li>{@code tailMap(position).firstEntry()} → nearest clockwise v-node.</li>
     *   <li>Wrap around to {@code ring.firstEntry()} if no clockwise entry exists.</li>
     *   <li>Skip unhealthy nodes (at most one full revolution).</li>
     * </ol>
     *
     * @param key the cache key to route
     * @return the owning {@link CacheNode}, or {@code null} if the ring is empty
     *         or all nodes are unhealthy
     */
    public CacheNode getNodeForKey(String key) {
        readLock.lock();
        try {
            CacheNode node = ring.getNodeForKey(key);
            if (node == null) {
                LOG.warning("[HashRingManager] No healthy node found for key=" + key);
            }
            return node;
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Like {@link #getNodeForKey} but explicitly skips {@code excludeNode}.
     * Used by the Smart Client to find a fallback when the primary node is down.
     *
     * @param key         the cache key
     * @param excludeNode the node to skip (e.g. a node just detected as failed)
     * @return next available node, or {@code null}
     */
    public CacheNode getNextNodeForKey(String key, CacheNode excludeNode) {
        readLock.lock();
        try {
            return ring.getNextNodeForKey(key, excludeNode);
        } finally {
            readLock.unlock();
        }
    }

    // -----------------------------------------------------------------------
    // Health management — write-lock (called by HeartbeatService)
    // -----------------------------------------------------------------------

    /**
     * Mark a node as healthy (called when heartbeat ACK is received).
     * The node's virtual nodes are already on the ring; marking it healthy
     * simply allows {@code getNodeForKey} to route to it again.
     *
     * @param nodeId host:port identifier
     */
    public void markNodeHealthy(String nodeId) {
        writeLock.lock();
        try {
            CacheNode node = physicalNodes.get(nodeId);
            if (node != null) {
                node.markHealthy();
                LOG.info("[HashRingManager] Node " + nodeId + " is now HEALTHY.");
            }
        } finally {
            writeLock.unlock();
        }
    }

    /**
     * Mark a node as unhealthy (called after N consecutive missed heartbeats).
     * The node's virtual nodes stay on the ring; they are silently bypassed
     * inside {@link ConsistentHashRing#getNodeForKey}.
     *
     * @param nodeId host:port identifier
     */
    public void markNodeUnhealthy(String nodeId) {
        writeLock.lock();
        try {
            CacheNode node = physicalNodes.get(nodeId);
            if (node != null) {
                node.markUnhealthy();
                LOG.warning("[HashRingManager] Node " + nodeId + " is now UNHEALTHY — traffic rerouted.");
            }
        } finally {
            writeLock.unlock();
        }
    }

    // -----------------------------------------------------------------------
    // Replication topology queries — read-lock
    // -----------------------------------------------------------------------

    /**
     * Returns the dedicated slave for the given master, or {@code null} if not
     * registered.
     */
    public CacheNode getSlaveFor(CacheNode master) {
        readLock.lock();
        try {
            return masterToSlave.get(master.getNodeId());
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Returns the master for the given slave, or {@code null} if not registered.
     * Used by PromotionHandler to determine which master just failed.
     */
    public CacheNode getMasterFor(CacheNode slave) {
        readLock.lock();
        try {
            return slaveToMaster.get(slave.getNodeId());
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Look up a physical node by its ID ({@code "host:port"}).
     * Returns {@code null} if unknown.
     */
    public CacheNode getNodeById(String nodeId) {
        readLock.lock();
        try {
            return physicalNodes.get(nodeId);
        } finally {
            readLock.unlock();
        }
    }

    /** Returns an unmodifiable snapshot of all registered physical nodes. */
    public List<CacheNode> getAllPhysicalNodes() {
        readLock.lock();
        try {
            return Collections.unmodifiableList(new ArrayList<>(physicalNodes.values()));
        } finally {
            readLock.unlock();
        }
    }

    /** Total number of virtual-node slots currently on the ring. */
    public int getVirtualNodeCount() {
        readLock.lock();
        try {
            return ring.virtualNodeCount();
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Diagnostic: returns the raw ring position (MD5 hash) for any string.
     * Useful for unit tests that verify key → node routing.
     */
    public long getRingPosition(String input) {
        readLock.lock();
        try {
            return ring.hashToLong(input);
        } finally {
            readLock.unlock();
        }
    }
}
