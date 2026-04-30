package aerocache.replication;

import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

/**
 * Monotonic epoch counter that prevents the split-brain write scenario.
 *
 * <h3>Problem it solves</h3>
 * Consider this sequence:
 * <ol>
 *   <li>Node-1 (MASTER) loses network connectivity.</li>
 *   <li>Node-2 (SLAVE) misses 3 heartbeats → promotes itself to MASTER, epoch 1→2.</li>
 *   <li>Clients now write to Node-2 (new master).</li>
 *   <li>Node-1 regains connectivity. It still thinks it is MASTER (epoch=1).</li>
 *   <li>Node-1 attempts to send REPLICATE commands to Node-2.</li>
 * </ol>
 * Without a guard, Node-2 would blindly apply Node-1's stale writes, overwriting
 * fresh data from step 3. With {@code SplitBrainGuard}, Node-2 rejects any
 * REPLICATE command from a node whose epoch < its own, and Node-1 must demote itself.
 *
 * <h3>Epoch lifecycle</h3>
 * <ul>
 *   <li>Initial epoch = 0 for all nodes at cluster start.</li>
 *   <li>Every promotion increments the epoch by exactly 1.</li>
 *   <li>Epochs only ever increase — {@link #update} uses {@code Math.max}.</li>
 *   <li>Epochs are broadcast via the {@code NODE_PROMOTED} cluster message
 *       (implemented in {@code PromotionHandler}) so all peers sync up.</li>
 * </ul>
 *
 * <h3>Thread safety</h3>
 * All state is held in a single {@link AtomicLong} — all methods are lock-free.
 */
public class SplitBrainGuard {

    private static final Logger LOG = Logger.getLogger(SplitBrainGuard.class.getName());

    /** Monotonically increasing generation counter. Starts at 0. */
    private final AtomicLong epoch = new AtomicLong(0L);

    // -----------------------------------------------------------------------
    // Epoch management
    // -----------------------------------------------------------------------

    /**
     * Called by {@link PromotionHandler} immediately after promoting this node.
     * Records the newly assigned epoch so all future incoming writes can be
     * validated against it.
     *
     * @param promotedEpoch the epoch value returned by {@link aerocache.ring.CacheNode#promote()}
     * @return the effective epoch after the update
     */
    public long onPromotion(long promotedEpoch) {
        // Math.max guards against any out-of-order promotion signal.
        long updated = epoch.updateAndGet(current -> Math.max(current, promotedEpoch));
        LOG.severe("[SplitBrainGuard] Epoch advanced to " + updated + " after promotion.");
        return updated;
    }

    /**
     * Sync the local epoch with a value received from a peer (e.g. on cluster
     * join or after a NODE_PROMOTED broadcast). Uses {@code Math.max} so the
     * epoch only ever moves forward.
     *
     * @param peerEpoch epoch value received from a cluster peer
     */
    public void update(long peerEpoch) {
        long prev = epoch.getAndUpdate(current -> Math.max(current, peerEpoch));
        if (peerEpoch > prev) {
            LOG.info("[SplitBrainGuard] Epoch synced: " + prev + " → " + epoch.get());
        }
    }

    // -----------------------------------------------------------------------
    // Validation — called before processing any REPLICATE command
    // -----------------------------------------------------------------------

    /**
     * Validate that a replication command from a remote node should be accepted.
     *
     * <p>Returns {@code true} (accept) if {@code senderEpoch >= currentEpoch}.
     * Returns {@code false} (reject) if {@code senderEpoch < currentEpoch},
     * which means the sender is a stale master from before the last promotion.
     *
     * <p>If {@code senderEpoch > currentEpoch} we also advance our own epoch
     * (we may have missed a promotion broadcast).
     *
     * @param senderEpoch epoch value supplied by the remote node
     * @return {@code true} if the command is safe to apply; {@code false} if it must be dropped
     */
    public boolean validateReplication(long senderEpoch) {
        long current = epoch.get();

        if (senderEpoch < current) {
            // Stale master — reject.
            LOG.warning("[SplitBrainGuard] ⚠ SPLIT-BRAIN DETECTED — rejecting replication. "
                    + "Sender epoch=" + senderEpoch + " < current epoch=" + current
                    + ". The sender must demote itself.");
            return false;
        }

        if (senderEpoch > current) {
            // We are behind — sync up.
            epoch.compareAndSet(current, senderEpoch);
            LOG.info("[SplitBrainGuard] Epoch advanced by peer: " + current + " → " + senderEpoch);
        }

        return true; // senderEpoch >= current — safe to apply
    }

    /**
     * Convenience check: is {@code senderEpoch} strictly older than the current epoch?
     * Used by {@link HeartbeatService} when a zombie master reconnects.
     *
     * @param senderEpoch epoch reported by a reconnecting node
     * @return {@code true} if the sender is stale and should demote itself
     */
    public boolean isStale(long senderEpoch) {
        return senderEpoch < epoch.get();
    }

    /** Current epoch value. */
    public long getEpoch() {
        return epoch.get();
    }

    @Override
    public String toString() {
        return "SplitBrainGuard{epoch=" + epoch.get() + "}";
    }
}
