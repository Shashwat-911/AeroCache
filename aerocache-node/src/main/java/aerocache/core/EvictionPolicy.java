package aerocache.core;

/**
 * Strategy interface for cache eviction.
 * Implementations are called while the CacheEngine lock is held —
 * they must NOT acquire any additional locks to avoid deadlock.
 */
public interface EvictionPolicy<K, V> {
    /**
     * Make room for at least one new entry.
     * Implementations should call {@link CacheEngine#evictLRU()} or
     * {@link CacheEngine#evictKeys(java.util.List)}.
     *
     * @param engine the cache engine to evict from
     */
    void evict(CacheEngine<K, V> engine);
}
