package aerocache.replication;

import aerocache.net.TCPClient;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Asynchronous master-to-slave replication pipeline.
 *
 * <h3>Why asynchronous?</h3>
 * If replication were synchronous, every client SET/DEL would block until the
 * slave acknowledged the write — adding a full network round-trip to the
 * client-visible latency. AeroCache prioritises write throughput: the master
 * confirms {@code +OK} to the client immediately and replicates in the background.
 *
 * <h3>Pipeline design</h3>
 * <pre>
 *   Client SET/DEL
 *         │
 *         ▼
 *   CacheEngine.put() / delete()
 *         │
 *         ▼
 *   ReplicationManager.replicateSet/Del()    ← non-blocking, O(1)
 *         │   offer() to LinkedBlockingQueue
 *         ▼
 *   [replication-drainer virtual thread]     ← blocking poll(), runs forever
 *         │   drains queue one command at a time
 *         ▼
 *   TCPClient.sendCommandAsync()             ← write to persistent socket, no ACK wait
 *         │
 *         ▼
 *   Slave node → CommandHandler.handleReplicate()
 * </pre>
 *
 * <h3>Back-pressure</h3>
 * The queue is bounded ({@link #QUEUE_CAPACITY}). If the slave falls so far
 * behind that the queue fills up, {@link #enqueue} drops the command and logs
 * a warning rather than blocking the client thread. In production this would
 * trigger an alert and potentially pause writes until the slave catches up.
 *
 * <h3>Ordering guarantee</h3>
 * {@link LinkedBlockingQueue} is FIFO. The single drainer thread processes
 * commands in the exact order they were enqueued, so the slave always sees
 * SET and DEL operations in the same sequence as the master applied them.
 *
 * <h3>Thread safety</h3>
 * {@link LinkedBlockingQueue} is fully thread-safe — multiple client threads
 * can call {@link #replicateSet}/{@link #replicateDel} concurrently without
 * any additional synchronisation.
 */
public class ReplicationManager {

    private static final Logger LOG = Logger.getLogger(ReplicationManager.class.getName());

    /**
     * Maximum number of pending replication commands.
     * At ~50 bytes per command, 10 000 commands ≈ 500 KB — comfortably bounded.
     */
    private static final int QUEUE_CAPACITY = 10_000;

    /**
     * How long the drainer blocks waiting for a new command before looping
     * to check {@link #running}. Short enough for responsive shutdown.
     */
    private static final long POLL_TIMEOUT_MS = 500L;

    // -----------------------------------------------------------------------
    // State
    // -----------------------------------------------------------------------

    /** FIFO buffer between the write path and the drainer thread. */
    private final LinkedBlockingQueue<String> queue;

    /** Persistent connection to the slave node. */
    private final TCPClient slaveClient;

    /** Controls the drainer loop. Set to false by {@link #stop()}. */
    private volatile boolean running = false;

    /** The background virtual thread that drains the queue. */
    private Thread drainerThread;

    // -----------------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------------

    /**
     * @param slaveClient a {@link TCPClient} pre-configured with the slave's host and port.
     *                    Must NOT be shared with other components (owned by this manager).
     */
    public ReplicationManager(TCPClient slaveClient) {
        this.slaveClient = slaveClient;
        this.queue       = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    /**
     * Start the background drainer virtual thread.
     * Call this once after constructing the manager, before accepting client writes.
     */
    public void start() {
        running = true;

        // Virtual thread: cheap, no pool sizing needed.
        drainerThread = Thread.ofVirtual()
                .name("aerocache-replication-drainer")
                .start(this::drainLoop);

        LOG.info("[ReplicationManager] Started. Slave target: " + slaveClient);
    }

    /**
     * Stop the drainer thread and close the slave connection.
     *
     * <p>Any commands still in the queue at shutdown time are dropped and counted
     * in the log. In production these would be durably journaled first.
     */
    public void stop() {
        running = false;

        if (drainerThread != null) {
            drainerThread.interrupt();
            try {
                drainerThread.join(3_000); // wait up to 3 s for clean exit
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        slaveClient.close();

        int dropped = queue.size();
        if (dropped > 0) {
            LOG.warning("[ReplicationManager] Stopped with " + dropped
                    + " replication command(s) still in queue — these will NOT be applied to slave.");
        } else {
            LOG.info("[ReplicationManager] Stopped cleanly. Queue was empty.");
        }
    }

    // -----------------------------------------------------------------------
    // Public replication API — called from the write path
    // -----------------------------------------------------------------------

    /**
     * Enqueue a {@code REPLICATE SET} command for asynchronous delivery to the slave.
     *
     * <p>This method returns immediately (O(1) queue offer) and never blocks the
     * calling client thread.
     *
     * @param key   the cache key that was written
     * @param value the value that was written
     */
    public void replicateSet(String key, String value) {
        // Wire format: "REPLICATE SET <key> <value>"
        // Matches CommandParser's REPLICATE handler which re-parses the inner command.
        enqueue("REPLICATE SET " + key + " " + value);
    }

    /**
     * Enqueue a {@code REPLICATE DEL} command for asynchronous delivery to the slave.
     *
     * @param key the cache key that was deleted
     */
    public void replicateDel(String key) {
        enqueue("REPLICATE DEL " + key);
    }

    /**
     * Number of commands currently waiting in the queue.
     * Useful for monitoring and back-pressure dashboards.
     */
    public int getPendingCount() {
        return queue.size();
    }

    // -----------------------------------------------------------------------
    // Drainer loop — runs in the background virtual thread
    // -----------------------------------------------------------------------

    /**
     * Continuously drain the queue and forward commands to the slave.
     *
     * <p>Uses {@link LinkedBlockingQueue#poll(long, TimeUnit)} with a short timeout
     * rather than {@link LinkedBlockingQueue#take()} so that the loop can notice
     * the {@link #running} flag being cleared by {@link #stop()} without delay.
     *
     * <p>If the slave is temporarily unavailable, {@link TCPClient#sendCommandAsync}
     * logs the error and invalidates the socket; the next command will trigger
     * a reconnect attempt. This means replication commands queued during a
     * slave outage will be silently dropped once the drainer processes them —
     * acceptable for the async model. A production system would add a retry buffer.
     */
    private void drainLoop() {
        LOG.fine("[ReplicationManager] Drainer started.");

        while (running) {
            try {
                // Block up to POLL_TIMEOUT_MS — releases the virtual carrier thread.
                String command = queue.poll(POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);

                if (command != null) {
                    // Fire-and-forget: we do NOT read the slave's +OK response.
                    // This keeps the drainer throughput as high as possible and avoids
                    // head-of-line blocking if the slave is slow.
                    slaveClient.sendCommandAsync(command);
                    LOG.fine("[ReplicationManager] Dispatched: " + command);
                }

            } catch (InterruptedException e) {
                // stop() interrupted us — honour the interrupt and exit.
                Thread.currentThread().interrupt();
                if (!running) break;
            }
        }

        LOG.fine("[ReplicationManager] Drainer exited.");
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /**
     * Non-blocking offer to the bounded queue.
     * Logs a warning and drops the command if the queue is full.
     */
    private void enqueue(String command) {
        boolean accepted = queue.offer(command);
        if (!accepted) {
            LOG.warning("[ReplicationManager] Queue full (" + QUEUE_CAPACITY
                    + "). Dropping command: " + command);
        }
    }
}
