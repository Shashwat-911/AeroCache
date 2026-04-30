package aerocache.ring;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Consistent Hash Ring for AeroCache.
 *
 * <h3>Data structure</h3>
 * The ring is modelled as a {@link TreeMap}{@code <Long, CacheNode>} where each
 * {@code Long} key is a point on the 64-bit hash space and the value is the
 * physical node that "owns" that point.
 *
 * <pre>
 *
 *              0 ────────────────────────────────────── 2^63-1
 *              │                                             │
 *     vnode(A,0)  vnode(B,0)  vnode(A,1)  vnode(C,0)  vnode(B,1) ...
 *        [A]         [B]         [A]          [C]         [B]
 *
 * </pre>
 *
 * <h3>Virtual nodes (v-nodes)</h3>
 * Each physical node is placed at {@code VIRTUAL_NODES} positions on the ring.
 * The v-node key is derived by hashing the string {@code "host:port#vnode-N"}.
 * With 150 v-nodes the standard deviation of key distribution is kept below ~5%
 * even for small clusters of 2–8 nodes.
 *
 * <h3>Key lookup — O(log N)</h3>
 * To find the owning node for a given cache key:
 * <ol>
 *   <li>Hash the key to a {@code long} ring position {@code h}.</li>
 *   <li>Call {@link TreeMap#tailMap(Object)} to get all entries with position ≥ h.</li>
 *   <li>Take the {@link java.util.NavigableMap#firstEntry()} of that tail sub-map.</li>
 *   <li>If the tail sub-map is empty (h is past the last v-node), wrap around and
 *       take {@link TreeMap#firstEntry()} — i.e. the node at position 0.</li>
 * </ol>
 *
 * <h3>Thread safety</h3>
 * This class is <em>NOT</em> internally synchronised — all synchronisation is
 * delegated to {@link HashRingManager}, which wraps this ring with a
 * {@code ReentrantReadWriteLock}. This keeps the ring itself simple and testable.
 *
 * <h3>Node removal</h3>
 * Removing a node deletes all its virtual-node entries from the TreeMap. Keys
 * that were mapped to the removed node now resolve to the next clockwise node,
 * so only the keys on the removed node's segments are remapped — typically
 * O(K/N) keys, not the full keyspace.
 */
public class ConsistentHashRing {

    // -----------------------------------------------------------------------
    // Tuning constant
    // -----------------------------------------------------------------------

    /**
     * Number of virtual nodes placed on the ring per physical node.
     *
     * <p>150 is a well-understood sweet-spot: Karger et al.'s original paper
     * suggests ≥100 v-nodes to keep standard deviation below 10% for typical
     * cluster sizes. We use 150 for extra uniformity.
     */
    public static final int DEFAULT_VIRTUAL_NODES = 150;

    // -----------------------------------------------------------------------
    // Ring state
    // -----------------------------------------------------------------------

    /**
     * The core ring data structure.
     *
     * Key   → 64-bit hash position derived via MD5.
     * Value → the physical {@link CacheNode} that owns this virtual-node slot.
     *
     * We use {@code TreeMap} (a red-black tree) so that:
     * <ul>
     *   <li>Insertion / deletion of a v-node is O(log N).</li>
     *   <li>{@code tailMap(h).firstEntry()} gives the nearest clockwise node
     *       for any hash {@code h} in O(log N).</li>
     * </ul>
     */
    private final TreeMap<Long, CacheNode> ring = new TreeMap<>();

    /** Configurable v-node count (set once at construction). */
    private final int virtualNodes;

    /** Reusable MD5 digest — callers must NOT use this concurrently. */
    private final MessageDigest md5;

    // -----------------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------------

    public ConsistentHashRing() {
        this(DEFAULT_VIRTUAL_NODES);
    }

    public ConsistentHashRing(int virtualNodes) {
        if (virtualNodes < 1)
            throw new IllegalArgumentException("virtualNodes must be ≥ 1, got " + virtualNodes);
        this.virtualNodes = virtualNodes;
        try {
            this.md5 = MessageDigest.getInstance("MD5");
        } catch (NoSuchAlgorithmException e) {
            // MD5 is mandated by the Java spec — this should never happen.
            throw new IllegalStateException("MD5 not available in this JVM", e);
        }
    }

    // -----------------------------------------------------------------------
    // Ring mutation — O(V log V) per physical node, where V = virtualNodes
    // -----------------------------------------------------------------------

    /**
     * Add a physical node to the ring by placing {@link #virtualNodes} virtual
     * nodes at deterministically computed positions.
     *
     * <p>The v-node key string is {@code "host:port#vnode-N"} for N in
     * [0, virtualNodes). Hashing a compound key (rather than just the node ID)
     * spreads the v-nodes across the ring even when node IDs are similar strings.
     *
     * @param node the physical node to add
     */
    public void addNode(CacheNode node) {
        String nodeId = node.getNodeId(); // e.g. "aerocache-node-1:6001"
        for (int i = 0; i < virtualNodes; i++) {
            // Compound v-node key: deterministic, unique per (node, i) pair.
            String vnodeKey = nodeId + "#vnode-" + i;
            long ringPosition = hashToLong(vnodeKey);
            ring.put(ringPosition, node);
        }
    }

    /**
     * Remove a physical node from the ring by deleting all its virtual-node entries.
     *
     * <p>After removal, keys that were mapped to this node automatically resolve
     * to the next clockwise node — no key reassignment logic is needed.
     *
     * @param node the physical node to remove
     */
    public void removeNode(CacheNode node) {
        String nodeId = node.getNodeId();
        for (int i = 0; i < virtualNodes; i++) {
            String vnodeKey = nodeId + "#vnode-" + i;
            long ringPosition = hashToLong(vnodeKey);
            ring.remove(ringPosition);
        }
    }

    // -----------------------------------------------------------------------
    // Key routing — O(log N)
    // -----------------------------------------------------------------------

    /**
     * Find the physical node responsible for {@code key}.
     *
     * <p>Algorithm:
     * <ol>
     *   <li>Hash {@code key} → {@code long ringPosition}.</li>
     *   <li>{@code tailMap(ringPosition)} → all entries with position ≥ ringPosition.</li>
     *   <li>Take {@code firstEntry()} of the tail → nearest clockwise v-node.</li>
     *   <li>If tail is empty, wrap around: take {@code ring.firstEntry()}.</li>
     *   <li>Skip any v-nodes whose owning physical node is currently unhealthy
     *       (up to one full revolution — prevents routing to a dead node).</li>
     * </ol>
     *
     * @param key the cache key to route
     * @return the responsible {@link CacheNode}, or {@code null} if the ring is empty
     */
    public CacheNode getNodeForKey(String key) {
        if (ring.isEmpty()) return null;

        long ringPosition = hashToLong(key);

        // Walk clockwise from ringPosition, skipping unhealthy nodes.
        // We may need to do two passes (tailMap + wrap-around).
        CacheNode candidate = findHealthyClockwise(ringPosition);
        return candidate; // may be null only if ALL nodes are unhealthy
    }

    /**
     * Like {@link #getNodeForKey} but skips the specified node — used when the
     * caller already knows the primary node is unavailable and wants a fallback.
     *
     * @param key    the cache key
     * @param skip   the node to exclude (e.g. a failed master)
     * @return next healthy node, or {@code null} if none available
     */
    public CacheNode getNextNodeForKey(String key, CacheNode skip) {
        if (ring.isEmpty()) return null;
        long ringPosition = hashToLong(key);
        return findHealthyClockwise(ringPosition, skip);
    }

    // -----------------------------------------------------------------------
    // Diagnostic helpers
    // -----------------------------------------------------------------------

    /**
     * Returns all distinct physical nodes currently on the ring.
     * Duplicates caused by virtual nodes are collapsed.
     */
    public List<CacheNode> getPhysicalNodes() {
        // Use identity set semantics: CacheNode.equals() is based on host+port.
        List<CacheNode> seen = new ArrayList<>();
        for (CacheNode node : ring.values()) {
            if (!seen.contains(node)) seen.add(node);
        }
        return Collections.unmodifiableList(seen);
    }

    /** Total number of virtual-node slots currently on the ring. */
    public int virtualNodeCount() {
        return ring.size();
    }

    /** {@code true} if no physical nodes have been added yet. */
    public boolean isEmpty() {
        return ring.isEmpty();
    }

    // -----------------------------------------------------------------------
    // Hashing — MD5 → long
    // -----------------------------------------------------------------------

    /**
     * Hash any string to a {@code long} ring position using MD5.
     *
     * <p>MD5 produces 128 bits; we take the <em>first 8 bytes</em> and assemble
     * them into a signed {@code long} (big-endian). This gives us 2^63 distinct
     * positions — more than enough to avoid collisions even with 150 v-nodes × 8
     * physical nodes = 1 200 ring slots.
     *
     * <p>Why MD5 over {@code String.hashCode()}? MD5 distributes bits far more
     * uniformly and is deterministic across JVM versions, which is critical for
     * all nodes in the cluster to agree on the same ring layout.
     *
     * @param input any non-null string
     * @return a deterministic 64-bit ring position
     */
    public long hashToLong(String input) {
        // Reset the digest — MessageDigest is stateful, so we must reset before
        // each call. This is safe as long as ConsistentHashRing is used single-
        // threadedly (HashRingManager holds a write-lock during mutations).
        md5.reset();
        byte[] digest = md5.digest(input.getBytes(java.nio.charset.StandardCharsets.UTF_8));

        // Assemble first 8 bytes into a long (big-endian).
        long hash = 0;
        for (int i = 0; i < 8; i++) {
            hash = (hash << 8) | (digest[i] & 0xFFL);
        }
        return hash;
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /**
     * Walk clockwise from {@code startPosition}, returning the first healthy node.
     * Performs at most one full revolution (stops if it visits more unique nodes
     * than are on the ring).
     */
    private CacheNode findHealthyClockwise(long startPosition) {
        return findHealthyClockwise(startPosition, null);
    }

    /**
     * Walk clockwise from {@code startPosition}, returning the first healthy node
     * that is not {@code skip}.
     */
    private CacheNode findHealthyClockwise(long startPosition, CacheNode skip) {
        // Try the tail (clockwise from startPosition).
        Map.Entry<Long, CacheNode> entry = ring.tailMap(startPosition).entrySet()
                .stream()
                .filter(e -> isUsable(e.getValue(), skip))
                .findFirst()
                .orElse(null);

        if (entry != null) return entry.getValue();

        // Wrap around — search from the beginning of the ring.
        return ring.entrySet().stream()
                .filter(e -> isUsable(e.getValue(), skip))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(null); // all nodes unhealthy
    }

    /** A node is "usable" if it is healthy and is not the node to skip. */
    private boolean isUsable(CacheNode node, CacheNode skip) {
        return node.isHealthy() && !node.equals(skip);
    }
}
