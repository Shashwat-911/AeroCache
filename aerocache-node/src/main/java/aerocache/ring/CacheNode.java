package aerocache.ring;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Represents a single physical cache node in the AeroCache cluster.
 *
 * <p>Each node has:
 * <ul>
 *   <li>A network address (host + port) that uniquely identifies it.</li>
 *   <li>A {@link Role} — either {@code MASTER} (serves reads and writes) or
 *       {@code SLAVE} (receives async replication, promotes on master failure).</li>
 *   <li>A health flag toggled by the {@code HeartbeatService}.</li>
 *   <li>An {@link #epoch} counter used by {@code SplitBrainGuard}: every time a
 *       slave is promoted to master it increments the epoch, so a stale master
 *       that reconnects after a partition sees a lower epoch and demotes itself.</li>
 * </ul>
 */
public class CacheNode {

    // -----------------------------------------------------------------------
    // Role enum
    // -----------------------------------------------------------------------

    /**
     * MASTER — authoritative writer; accepts client GET/SET/DEL.
     * SLAVE  — read-only replica; promoted to MASTER on missed heartbeats.
     */
    public enum Role {
        MASTER, SLAVE
    }

    // -----------------------------------------------------------------------
    // Identity (immutable after construction)
    // -----------------------------------------------------------------------

    /** Hostname or IP address of this node (e.g. "aerocache-node-1"). */
    private final String host;

    /** TCP port this node's CacheServer listens on (e.g. 6001). */
    private final int port;

    // -----------------------------------------------------------------------
    // Mutable cluster state (thread-safe via atomics)
    // -----------------------------------------------------------------------

    /** Current role; updated on promotion without holding an external lock. */
    private volatile Role role;

    /**
     * Whether this node is currently reachable according to the heartbeat service.
     * Declared volatile so reads across threads always see the latest value.
     */
    private final AtomicBoolean healthy = new AtomicBoolean(true);

    /**
     * Monotonically increasing generation counter.
     * Incremented every time this node is promoted to MASTER.
     * The SplitBrainGuard rejects write commands from any peer whose
     * epoch is strictly less than the current node's epoch.
     */
    private final AtomicLong epoch = new AtomicLong(0);

    // -----------------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------------

    public CacheNode(String host, int port, Role role) {
        if (host == null || host.isBlank())
            throw new IllegalArgumentException("CacheNode host must not be blank");
        if (port < 1 || port > 65535)
            throw new IllegalArgumentException("CacheNode port out of range: " + port);
        this.host = host;
        this.port = port;
        this.role = role;
    }

    // -----------------------------------------------------------------------
    // Identity helpers
    // -----------------------------------------------------------------------

    /**
     * Canonical node identifier used as the seed for virtual-node hashing.
     * Format: {@code "host:port"} — e.g. {@code "aerocache-node-1:6001"}.
     */
    public String getNodeId() {
        return host + ":" + port;
    }

    // -----------------------------------------------------------------------
    // Getters
    // -----------------------------------------------------------------------

    public String  getHost()    { return host; }
    public int     getPort()    { return port; }
    public Role    getRole()    { return role; }
    public boolean isHealthy()  { return healthy.get(); }
    public long    getEpoch()   { return epoch.get(); }

    // -----------------------------------------------------------------------
    // State mutators (called by HeartbeatService / PromotionHandler)
    // -----------------------------------------------------------------------

    /** Mark this node as healthy (heartbeat ACK received). */
    public void markHealthy()   { healthy.set(true); }

    /** Mark this node as unreachable (heartbeat missed threshold exceeded). */
    public void markUnhealthy() { healthy.set(false); }

    /**
     * Promote this node to {@link Role#MASTER} and bump the epoch.
     * The incremented epoch is returned so the caller can broadcast it
     * to other nodes.
     *
     * @return the new epoch value after promotion
     */
    public long promote() {
        this.role = Role.MASTER;
        return epoch.incrementAndGet();
    }

    /**
     * Demote this node back to {@link Role#SLAVE}.
     * Called by SplitBrainGuard when a reconnected node discovers it has
     * a lower epoch than the currently active master.
     */
    public void demote() {
        this.role = Role.SLAVE;
    }

    /**
     * Force-update the epoch (e.g. when receiving a cluster broadcast
     * containing a higher epoch from the promoted slave).
     */
    public void updateEpoch(long newEpoch) {
        epoch.updateAndGet(current -> Math.max(current, newEpoch));
    }

    // -----------------------------------------------------------------------
    // Object overrides
    // -----------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CacheNode)) return false;
        CacheNode other = (CacheNode) o;
        return port == other.port && host.equals(other.host);
    }

    @Override
    public int hashCode() {
        return 31 * host.hashCode() + port;
    }

    @Override
    public String toString() {
        return "CacheNode{"
                + "id='"   + getNodeId() + '\''
                + ", role=" + role
                + ", healthy=" + healthy.get()
                + ", epoch=" + epoch.get()
                + '}';
    }
}
