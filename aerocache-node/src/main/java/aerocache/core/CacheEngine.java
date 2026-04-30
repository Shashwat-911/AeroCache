package aerocache.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

/**
 * AeroCache core storage engine.
 *
 * <h3>Data structures</h3>
 * <ul>
 *   <li>{@link ThreadSafeHashMap}{@code <K, Node<K,V>>} — O(1) key → node lookup</li>
 *   <li>{@link DoublyLinkedList}{@code <K,V>}           — O(1) LRU ordering</li>
 * </ul>
 *
 * <h3>Concurrency model</h3>
 * A single fair {@link ReentrantLock} ({@code engineLock}) guards every operation
 * that touches BOTH the map and the list together. This prevents the
 * classic "move-to-front lost update" race where two threads interleave
 * a map lookup and a DLL reorder.
 *
 * <p>The {@link ThreadSafeHashMap} carries its own internal RW-lock so it is
 * also independently safe as a standalone component; inside CacheEngine the
 * outer {@code engineLock} provides the additional atomicity needed.
 *
 * <h3>Eviction</h3>
 * The {@link EvictionPolicy} is pluggable at runtime:
 * <ul>
 *   <li>{@link LRUEvictionPolicy}  — always removes the DLL tail</li>
 *   <li>{@link AIEvictionPolicy}   — asks the Python microservice; falls back to LRU on timeout</li>
 * </ul>
 *
 * <h3>TTL</h3>
 * Optional per-key TTL (ms). Checked lazily on GET and periodically
 * by a background daemon thread every 30 s.
 *
 * <h3>Access logging</h3>
 * Every GET/PUT records a timestamp in an {@link AccessStats} object that
 * the networking layer can forward to the AI microservice.
 */
public class CacheEngine<K, V> {

    private static final Logger LOG = Logger.getLogger(CacheEngine.class.getName());

    // -----------------------------------------------------------------------
    // Core data structures
    // -----------------------------------------------------------------------
    private final ThreadSafeHashMap<K, DoublyLinkedList.Node<K, V>> nodeMap;
    private final DoublyLinkedList<K, V>                             lruList;

    // -----------------------------------------------------------------------
    // Configuration
    // -----------------------------------------------------------------------
    private final int capacity;

    // -----------------------------------------------------------------------
    // Optional TTL: key → absolute expiry epoch-ms  (0 = no expiry)
    // -----------------------------------------------------------------------
    private final ConcurrentHashMap<K, Long> expiryMap = new ConcurrentHashMap<>();

    // -----------------------------------------------------------------------
    // Access logs for AI microservice
    // -----------------------------------------------------------------------
    private final ConcurrentHashMap<K, AccessStats> accessLog = new ConcurrentHashMap<>();

    // -----------------------------------------------------------------------
    // Pluggable eviction policy
    // -----------------------------------------------------------------------
    private volatile EvictionPolicy<K, V> evictionPolicy;

    // -----------------------------------------------------------------------
    // Single fair engine lock — guards combined map + DLL operations
    // -----------------------------------------------------------------------
    private final ReentrantLock engineLock = new ReentrantLock(/*fair=*/true);

    // -----------------------------------------------------------------------
    // Background TTL cleaner
    // -----------------------------------------------------------------------
    private final ScheduledExecutorService cleaner =
        Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "aerocache-ttl-cleaner");
            t.setDaemon(true);
            return t;
        });

    // -----------------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------------
    public CacheEngine(int capacity) {
        if (capacity <= 0)
            throw new IllegalArgumentException("Capacity must be > 0, got " + capacity);
        this.capacity       = capacity;
        this.nodeMap        = new ThreadSafeHashMap<>(Math.min(capacity * 2, 1 << 14));
        this.lruList        = new DoublyLinkedList<>();
        this.evictionPolicy = new LRUEvictionPolicy<>();

        // Lazy TTL sweep every 30 s
        cleaner.scheduleAtFixedRate(this::sweepExpiredKeys, 30, 30, TimeUnit.SECONDS);
        LOG.info("[CacheEngine] Started. capacity=" + capacity);
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /**
     * Look up {@code key}.
     * On a hit: moves the entry to the MRU front and records the access.
     * On a miss or expired TTL: returns {@code null}.
     */
    public V get(K key) {
        engineLock.lock();
        try {
            DoublyLinkedList.Node<K, V> node = nodeMap.get(key);
            if (node == null) return null;

            if (isExpired(key)) {
                evictSingleKey(key, node);
                return null;
            }

            lruList.moveToFront(node);
            recordAccess(key);
            return node.value;
        } finally {
            engineLock.unlock();
        }
    }

    /**
     * Insert or update {@code key} with an optional TTL in milliseconds
     * ({@code ttlMillis} ≤ 0 means no expiry).
     * If the cache is at capacity the configured {@link EvictionPolicy} fires first.
     */
    public void put(K key, V value, long ttlMillis) {
        engineLock.lock();
        try {
            DoublyLinkedList.Node<K, V> existing = nodeMap.get(key);
            if (existing != null) {
                existing.value = value;
                lruList.moveToFront(existing);
            } else {
                // Evict before inserting so we never exceed capacity
                if (nodeMap.size() >= capacity) {
                    evictionPolicy.evict(this);
                }
                DoublyLinkedList.Node<K, V> node = new DoublyLinkedList.Node<>(key, value);
                lruList.addToFront(node);
                nodeMap.put(key, node);
            }

            if (ttlMillis > 0) {
                expiryMap.put(key, System.currentTimeMillis() + ttlMillis);
            } else {
                expiryMap.remove(key);
            }
            recordAccess(key);
        } finally {
            engineLock.unlock();
        }
    }

    /** Insert or update with no TTL. */
    public void put(K key, V value) {
        put(key, value, 0);
    }

    /**
     * Delete {@code key}.
     *
     * @return {@code true} if the key existed, {@code false} otherwise.
     */
    public boolean delete(K key) {
        engineLock.lock();
        try {
            DoublyLinkedList.Node<K, V> node = nodeMap.get(key);
            if (node == null) return false;
            evictSingleKey(key, node);
            return true;
        } finally {
            engineLock.unlock();
        }
    }

    /**
     * Evict the least-recently-used entry.
     * Called by {@link LRUEvictionPolicy} while the engine lock is held.
     *
     * @return {@code true} if an entry was removed.
     */
    public boolean evictLRU() {
        DoublyLinkedList.Node<K, V> lru = lruList.removeLast();
        if (lru == null) return false;
        nodeMap.remove(lru.key);
        expiryMap.remove(lru.key);
        LOG.fine("[CacheEngine] LRU-evicted key=" + lru.key);
        return true;
    }

    /**
     * Evict a specific list of keys (called by {@link AIEvictionPolicy}).
     * Silently skips keys that are no longer present.
     *
     * @return the number of entries actually removed.
     */
    public int evictKeys(List<K> keys) {
        int removed = 0;
        for (K key : keys) {
            DoublyLinkedList.Node<K, V> node = nodeMap.get(key);
            if (node != null) {
                evictSingleKey(key, node);
                removed++;
                LOG.fine("[CacheEngine] AI-evicted key=" + key);
            }
        }
        return removed;
    }

    // -----------------------------------------------------------------------
    // Introspection / AI support
    // -----------------------------------------------------------------------

    /** Snapshot of all access statistics — safe to read outside the engine lock. */
    public Map<K, AccessStats> getAccessLogSnapshot() {
        return new HashMap<>(accessLog);
    }

    public int size()         { return nodeMap.size(); }
    public int getCapacity()  { return capacity; }
    public boolean isFull()   { return nodeMap.size() >= capacity; }

    public boolean containsKey(K key) {
        engineLock.lock();
        try {
            if (!nodeMap.containsKey(key)) return false;
            if (isExpired(key)) {
                evictSingleKey(key, nodeMap.get(key));
                return false;
            }
            return true;
        } finally {
            engineLock.unlock();
        }
    }

    // -----------------------------------------------------------------------
    // Eviction policy management
    // -----------------------------------------------------------------------

    public void setEvictionPolicy(EvictionPolicy<K, V> policy) {
        this.evictionPolicy = policy;
    }

    public EvictionPolicy<K, V> getEvictionPolicy() {
        return evictionPolicy;
    }

    // -----------------------------------------------------------------------
    // Lifecycle
    // -----------------------------------------------------------------------

    /** Stops the background cleaner. Call on node shutdown. */
    public void shutdown() {
        cleaner.shutdownNow();
        LOG.info("[CacheEngine] Shut down.");
    }

    // -----------------------------------------------------------------------
    // Package-private accessors for eviction policy implementations
    // -----------------------------------------------------------------------

    DoublyLinkedList<K, V> getLruList()                                { return lruList; }
    ThreadSafeHashMap<K, DoublyLinkedList.Node<K, V>> getNodeMap()    { return nodeMap; }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /** Remove a key-node pair from all internal structures atomically. */
    private void evictSingleKey(K key, DoublyLinkedList.Node<K, V> node) {
        lruList.remove(node);
        nodeMap.remove(key);
        expiryMap.remove(key);
    }

    private boolean isExpired(K key) {
        Long exp = expiryMap.get(key);
        return exp != null && exp > 0 && System.currentTimeMillis() > exp;
    }

    /** Record access — called inside engineLock so no additional sync needed. */
    private void recordAccess(K key) {
        long now = System.currentTimeMillis();
        accessLog.computeIfAbsent(key, k -> new AccessStats()).recordAccess(now);
    }

    /** Background TTL sweep — acquires engineLock. */
    private void sweepExpiredKeys() {
        engineLock.lock();
        try {
            List<K> expired = new ArrayList<>();
            for (K key : expiryMap.keySet()) {
                if (isExpired(key)) expired.add(key);
            }
            for (K key : expired) {
                DoublyLinkedList.Node<K, V> node = nodeMap.get(key);
                if (node != null) evictSingleKey(key, node);
            }
            if (!expired.isEmpty())
                LOG.info("[CacheEngine] TTL sweep removed " + expired.size() + " key(s).");
        } finally {
            engineLock.unlock();
        }
    }
}
