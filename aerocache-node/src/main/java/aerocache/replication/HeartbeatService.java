package aerocache.replication;

import aerocache.net.TCPClient;
import aerocache.ring.CacheNode;
import aerocache.ring.HashRingManager;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/**
 * Periodic TCP heartbeat monitor that detects peer node failures and
 * triggers slave-to-master promotion after N consecutive missed pings.
 *
 * <h3>Failure detection algorithm</h3>
 * <pre>
 *   Every HEARTBEAT_INTERVAL_SECONDS (2 s):
 *     Send PING → await PONG (via persistent TCPClient, no new handshake)
 *
 *     On PONG received:
 *       Reset missedHeartbeats to 0
 *       If peer was previously unhealthy → mark healthy, log recovery
 *
 *     On timeout / IOException / wrong response:
 *       missedHeartbeats++
 *       If missedHeartbeats >= MAX_MISSED (3):
 *         → Mark peer unhealthy in HashRingManager
 *         → Spawn a virtual thread to call PromotionHandler.promote()
 *         → Set promotionTriggered = true to stop further pings
 * </pre>
 *
 * <h3>Why 3 consecutive misses?</h3>
 * A single missed heartbeat could be a GC pause or transient packet loss — not a
 * real failure. Three consecutive misses over 6 seconds is a strong signal of a
 * real outage while still keeping failover time under 8 seconds.
 *
 * <h3>Split-brain guard on reconnect</h3>
 * If the peer reconnects after promotion (e.g. it was just slow, not dead),
 * the check {@link SplitBrainGuard#isStale(long)} lets the caller verify the
 * peer's reported epoch before re-admitting it as a writer.
 *
 * <h3>Thread safety</h3>
 * <ul>
 *   <li>{@link AtomicInteger} for {@code missedHeartbeats} — safe under the
 *       scheduler's single thread without additional locks.</li>
 *   <li>{@link AtomicBoolean} for {@code promotionTriggered} — the CAS
 *       ({@code compareAndSet(false, true)}) ensures promotion fires exactly once
 *       even if two scheduler ticks race.</li>
 * </ul>
 */
public class HeartbeatService {

    private static final Logger LOG = Logger.getLogger(HeartbeatService.class.getName());

    /** Interval between consecutive PING commands in seconds. */
    private static final int HEARTBEAT_INTERVAL_SECONDS = 2;

    /**
     * Number of consecutive missed heartbeats that triggers promotion.
     * 3 misses × 2 s interval = 6 s detection window.
     */
    private static final int MAX_MISSED_HEARTBEATS = 3;

    // -----------------------------------------------------------------------
    // Dependencies
    // -----------------------------------------------------------------------

    /** Persistent connection to the peer node (re-used across every ping). */
    private final TCPClient peerClient;

    /** The local node — will be promoted if peer fails and self is a slave. */
    private final CacheNode self;

    /** The peer node being monitored (usually the master, from slave's perspective). */
    private final CacheNode peer;

    private final HashRingManager   ringManager;
    private final PromotionHandler  promotionHandler;
    private final SplitBrainGuard   splitBrainGuard;

    // -----------------------------------------------------------------------
    // State
    // -----------------------------------------------------------------------

    /** Running count of consecutive missed heartbeats. Resets on any PONG. */
    private final AtomicInteger missedHeartbeats = new AtomicInteger(0);

    /**
     * CAS flag: once set to {@code true}, no further promotion attempts are made.
     * Prevents a second promotion if a scheduled tick fires while the first
     * promotion virtual thread is still running.
     */
    private final AtomicBoolean promotionTriggered = new AtomicBoolean(false);

    /** The scheduler that fires the heartbeat task at a fixed rate. */
    private ScheduledExecutorService scheduler;

    // -----------------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------------

    /**
     * @param peerClient      persistent TCPClient pointing at the peer node
     * @param self            the local CacheNode (this process)
     * @param peer            the CacheNode being monitored
     * @param ringManager     cluster ring — updated on health changes and promotion
     * @param promotionHandler executes the state-transition sequence on failure
     * @param splitBrainGuard  epoch guard informed after promotion
     */
    public HeartbeatService(TCPClient      peerClient,
                            CacheNode      self,
                            CacheNode      peer,
                            HashRingManager    ringManager,
                            PromotionHandler   promotionHandler,
                            SplitBrainGuard    splitBrainGuard) {
        this.peerClient       = peerClient;
        this.self             = self;
        this.peer             = peer;
        this.ringManager      = ringManager;
        this.promotionHandler = promotionHandler;
        this.splitBrainGuard  = splitBrainGuard;
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    /**
     * Start the heartbeat loop.
     *
     * <p>Uses a {@code ScheduledExecutorService} backed by a virtual-thread factory
     * (Java 21) so the scheduler's periodic task runs on a lightweight virtual
     * thread rather than blocking a platform carrier thread during the PING wait.
     */
    public void start() {
        // Java 21: virtual-thread-backed scheduler.
        scheduler = Executors.newScheduledThreadPool(
                1,
                Thread.ofVirtual().name("aerocache-heartbeat", 0).factory());

        // Initial delay of 2 s gives peer time to fully start before first ping.
        scheduler.scheduleAtFixedRate(
                this::tick,
                HEARTBEAT_INTERVAL_SECONDS,
                HEARTBEAT_INTERVAL_SECONDS,
                TimeUnit.SECONDS);

        LOG.info("[HeartbeatService] Started. Monitoring peer=" + peer.getNodeId()
                + " every " + HEARTBEAT_INTERVAL_SECONDS + " s.");
    }

    /** Stop the scheduler. The persistent TCPClient is NOT closed here —
     *  the caller (Main/ReplicationManager) manages its lifecycle. */
    public void stop() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        LOG.info("[HeartbeatService] Stopped.");
    }

    // -----------------------------------------------------------------------
    // Diagnostic accessors
    // -----------------------------------------------------------------------

    public int     getMissedHeartbeats()     { return missedHeartbeats.get(); }
    public boolean isPromotionTriggered()    { return promotionTriggered.get(); }

    // -----------------------------------------------------------------------
    // Heartbeat tick — executed every HEARTBEAT_INTERVAL_SECONDS
    // -----------------------------------------------------------------------

    /**
     * Single heartbeat tick: send one PING, evaluate the response, and update
     * internal state. Designed to run in the scheduled virtual thread.
     *
     * <p>This method never throws — any exception is caught and treated as a
     * missed heartbeat to prevent the scheduler from cancelling the task.
     */
    private void tick() {
        // If promotion already fired, there is nothing left to monitor.
        if (promotionTriggered.get()) return;

        boolean alive;
        try {
            // TCPClient.ping() sends PING over the persistent socket and
            // returns true iff "+PONG" is received. If the socket is broken
            // it automatically attempts reconnection (up to maxReconnectAttempts).
            alive = peerClient.ping();
        } catch (Exception e) {
            // Defensive catch — treat unexpected exceptions as a missed heartbeat.
            LOG.fine("[HeartbeatService] Unexpected error during PING: " + e.getMessage());
            alive = false;
        }

        if (alive) {
            onHeartbeatReceived();
        } else {
            onHeartbeatMissed();
        }
    }

    /**
     * Called when a PONG is successfully received.
     *
     * <p>Resets the miss counter. If the peer was previously marked unhealthy
     * (a prior miss streak that did not reach the promotion threshold), it is
     * re-admitted to the ring.
     */
    private void onHeartbeatReceived() {
        int previousMisses = missedHeartbeats.getAndSet(0);

        if (previousMisses > 0) {
            // Peer recovered before the promotion threshold — log recovery.
            LOG.info("[HeartbeatService] Peer " + peer.getNodeId()
                    + " recovered after " + previousMisses + " missed ping(s). Re-marking healthy.");
            ringManager.markNodeHealthy(peer.getNodeId());
        } else {
            LOG.fine("[HeartbeatService] PONG from " + peer.getNodeId() + ". All good.");
        }
    }

    /**
     * Called when a PING times out or the peer connection fails.
     *
     * <p>Increments the miss counter. At {@link #MAX_MISSED_HEARTBEATS} the
     * peer is declared dead and promotion is triggered exactly once via a
     * CAS on {@link #promotionTriggered}.
     */
    private void onHeartbeatMissed() {
        int missed = missedHeartbeats.incrementAndGet();

        LOG.warning("[HeartbeatService] Missed PING #" + missed
                + "/" + MAX_MISSED_HEARTBEATS
                + " from peer=" + peer.getNodeId());

        if (missed >= MAX_MISSED_HEARTBEATS) {
            // CAS: only the first thread to flip false→true runs promotion.
            if (promotionTriggered.compareAndSet(false, true)) {

                LOG.severe("[HeartbeatService] FAILURE THRESHOLD REACHED — "
                        + MAX_MISSED_HEARTBEATS + " consecutive missed heartbeats "
                        + "from " + peer.getNodeId() + ". Initiating promotion sequence.");

                // Immediately remove the failed peer from routing so no new
                // client requests are sent to it while promotion runs.
                ringManager.markNodeUnhealthy(peer.getNodeId());

                // Run promotion in its OWN virtual thread.
                // Rationale: promotion involves logging, ring mutation, and
                // potentially a network broadcast — we must not block the
                // scheduler thread, which would delay subsequent tick() calls
                // and could prevent the scheduler from shutting down cleanly.
                Thread.ofVirtual()
                        .name("aerocache-promotion-" + self.getNodeId())
                        .start(() -> promotionHandler.promote(
                                self, peer, ringManager, splitBrainGuard));
            }
        }
    }
}
