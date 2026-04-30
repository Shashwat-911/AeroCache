package aerocache.replication;

import aerocache.ring.CacheNode;
import aerocache.ring.HashRingManager;
import java.util.logging.Logger;

/**
 * Executes the slave-to-master promotion state transition.
 *
 * <h3>Responsibilities</h3>
 * <ol>
 *   <li>Validate that this node is actually a slave (idempotency guard).</li>
 *   <li>Call {@link CacheNode#promote()} — atomically sets role=MASTER and
 *       increments the node's epoch counter.</li>
 *   <li>Inform {@link SplitBrainGuard} of the new epoch so it can reject
 *       stale writes from the old master if it reconnects.</li>
 *   <li>Mark the failed master unhealthy in {@link HashRingManager} and
 *       re-register self so routing uses the promoted node's virtual nodes.</li>
 *   <li>Log a high-visibility banner visible in {@code docker compose logs}.</li>
 * </ol>
 *
 * <h3>Thread safety</h3>
 * {@link #promote} is designed to be called exactly once per failure event.
 * {@link aerocache.replication.HeartbeatService} uses an {@code AtomicBoolean}
 * to guarantee single invocation even under concurrent heartbeat threads.
 */
public class PromotionHandler {

    private static final Logger LOG = Logger.getLogger(PromotionHandler.class.getName());

    // Visual separator for high-visibility log banners in Docker log streams.
    private static final String BAR = "=".repeat(65);

    /**
     * Perform the full slave → master promotion sequence.
     *
     * @param self          the local node (currently SLAVE) to promote
     * @param failedMaster  the master node that stopped responding
     * @param ringManager   cluster ring — updated to reroute traffic away from failedMaster
     * @param guard         split-brain guard — informed of the new epoch
     */
    public void promote(CacheNode self,
                        CacheNode failedMaster,
                        HashRingManager ringManager,
                        SplitBrainGuard guard) {

        // ----------------------------------------------------------------
        // Guard: never promote a node that is already a master.
        // HeartbeatService's AtomicBoolean normally prevents this, but
        // a defensive check here costs nothing and prevents corruption.
        // ----------------------------------------------------------------
        if (self.getRole() == CacheNode.Role.MASTER) {
            LOG.warning("[PromotionHandler] Promotion skipped — "
                    + self.getNodeId() + " is already MASTER.");
            return;
        }

        // ----------------------------------------------------------------
        // Step 1 — Log pre-promotion banner
        // ----------------------------------------------------------------
        LOG.severe("\n" + BAR);
        LOG.severe(" AEROCACHE PROMOTION EVENT");
        LOG.severe(" Failed master : " + failedMaster.getNodeId());
        LOG.severe(" Promoting     : " + self.getNodeId() + "  (SLAVE -> MASTER)");
        LOG.severe(BAR);

        // ----------------------------------------------------------------
        // Step 2 — Atomically promote CacheNode; capture new epoch.
        // CacheNode.promote() sets role=MASTER and increments AtomicLong epoch.
        // ----------------------------------------------------------------
        long newEpoch = self.promote();

        // ----------------------------------------------------------------
        // Step 3 — Advance the SplitBrainGuard epoch.
        // Any REPLICATE command arriving from the old master (epoch < newEpoch)
        // will now be rejected by SplitBrainGuard.validateReplication().
        // ----------------------------------------------------------------
        guard.onPromotion(newEpoch);

        // ----------------------------------------------------------------
        // Step 4 — Update the HashRingManager.
        //
        // (a) Mark the failed master unhealthy so getNodeForKey() bypasses
        //     its virtual nodes immediately.
        // (b) Re-add self to the ring. HashRingManager.addNode() replaces
        //     the existing entry (same host:port) with the updated CacheNode
        //     whose role is now MASTER. This is idempotent.
        // ----------------------------------------------------------------
        ringManager.markNodeUnhealthy(failedMaster.getNodeId());
        ringManager.addNode(self); // re-registers with updated MASTER role

        // ----------------------------------------------------------------
        // Step 5 — Log post-promotion banner
        // ----------------------------------------------------------------
        LOG.severe("\n" + BAR);
        LOG.severe(" PROMOTION COMPLETE");
        LOG.severe(" New master : " + self.getNodeId());
        LOG.severe(" New epoch  : " + newEpoch);
        LOG.severe(" Action     : " + failedMaster.getNodeId()
                + " traffic is now routed to " + self.getNodeId());
        LOG.severe(BAR + "\n");

        // ----------------------------------------------------------------
        // Step 6 — (Extension point) Broadcast promotion to other nodes.
        // In production this would send a NODE_PROMOTED <nodeId> <epoch>
        // command over TCP to every other node in the cluster so they can
        // update their local HashRingManager and SplitBrainGuard as well.
        // Omitted here to keep the prototype self-contained per node pair.
        // ----------------------------------------------------------------
    }
}
