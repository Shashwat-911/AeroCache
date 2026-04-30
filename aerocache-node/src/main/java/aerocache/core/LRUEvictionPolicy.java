package aerocache.core;

import java.util.logging.Logger;

/**
 * Baseline LRU eviction policy — removes the least-recently-used cache entry
 * in O(1) time by popping the tail node from the {@link DoublyLinkedList}.
 *
 * <h3>How it works</h3>
 * {@link CacheEngine} keeps a {@link DoublyLinkedList} ordered from
 * most-recently-used (head) to least-recently-used (tail). Every GET promotes
 * the accessed node to the front; every SET inserts at the front. The tail is
 * therefore always the coldest entry. {@link DoublyLinkedList#removeLast()}
 * unlinks it in O(1); {@link ThreadSafeHashMap#remove} deletes the key in O(1).
 *
 * <h3>Called under the engine lock</h3>
 * This method is invoked from within {@link CacheEngine#put} while
 * {@code CacheEngine.engineLock} is held, so no additional synchronisation
 * is needed here — direct calls to package-private helpers are safe.
 */
public class LRUEvictionPolicy<K, V> implements EvictionPolicy<K, V> {

    private static final Logger LOG = Logger.getLogger(LRUEvictionPolicy.class.getName());

    /**
     * Evict the single least-recently-used entry.
     *
     * <p>Delegates to {@link CacheEngine#evictLRU()} which performs the
     * two-step atomic removal:
     * <ol>
     *   <li>{@code DoublyLinkedList.removeLast()} — O(1) DLL tail unlink.</li>
     *   <li>{@code ThreadSafeHashMap.remove(key)} — O(1) hash-map removal.</li>
     * </ol>
     *
     * @param engine the cache engine to evict from (lock already held by caller)
     */
    @Override
    public void evict(CacheEngine<K, V> engine) {
        // Peek at the LRU candidate before evicting so we can log its key.
        DoublyLinkedList.Node<K, V> lru = engine.getLruList().peekLast();

        if (lru == null) {
            // The cache is empty — nothing to evict. This should not normally
            // happen (evict() is only called when size >= capacity > 0), but
            // we guard defensively.
            LOG.warning("[LRUEvictionPolicy] evict() called on an empty cache — no-op.");
            return;
        }

        boolean removed = engine.evictLRU();

        if (removed) {
            LOG.fine("[LRUEvictionPolicy] Evicted LRU key=" + lru.key
                    + " (cache size was " + (engine.size() + 1) + ")");
        }
    }
}
