package aerocache.core;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Custom separate-chaining HashMap with a single {@link ReentrantReadWriteLock}.
 *
 * <ul>
 *   <li>Multiple concurrent readers share the read lock → high-throughput GETs.</li>
 *   <li>Writers (put / remove / resize) acquire the exclusive write lock.</li>
 *   <li>Resize doubles capacity when load exceeds 0.75, preserving O(1) amortised ops.</li>
 * </ul>
 *
 * No external libraries are used — only java.util and java.util.concurrent.locks.
 */
@SuppressWarnings("unchecked")
public class ThreadSafeHashMap<K, V> {

    // -----------------------------------------------------------------------
    // Constants
    // -----------------------------------------------------------------------
    private static final int   DEFAULT_CAPACITY = 16;
    private static final float LOAD_FACTOR      = 0.75f;

    // -----------------------------------------------------------------------
    // Internal bucket entry (singly-linked list per bucket)
    // -----------------------------------------------------------------------
    private static final class Entry<K, V> {
        final K key;
        V value;
        final int hash;
        Entry<K, V> next;

        Entry(K key, V value, int hash, Entry<K, V> next) {
            this.key   = key;
            this.value = value;
            this.hash  = hash;
            this.next  = next;
        }
    }

    // -----------------------------------------------------------------------
    // State
    // -----------------------------------------------------------------------
    private Entry<K, V>[] table;
    private int size;
    private int threshold;

    private final ReentrantReadWriteLock rwLock    = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock  readLock  = rwLock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock = rwLock.writeLock();

    // -----------------------------------------------------------------------
    // Constructors
    // -----------------------------------------------------------------------
    public ThreadSafeHashMap() {
        this(DEFAULT_CAPACITY);
    }

    public ThreadSafeHashMap(int initialCapacity) {
        if (initialCapacity <= 0)
            throw new IllegalArgumentException("Capacity must be > 0");
        int cap   = nextPowerOfTwo(initialCapacity);
        table     = new Entry[cap];
        threshold = (int)(cap * LOAD_FACTOR);
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /** O(1) average — acquires read lock. */
    public V get(K key) {
        readLock.lock();
        try {
            int h   = hash(key);
            Entry<K, V> e = table[indexFor(h, table.length)];
            while (e != null) {
                if (e.hash == h && keysEqual(e.key, key)) return e.value;
                e = e.next;
            }
            return null;
        } finally {
            readLock.unlock();
        }
    }

    /**
     * Insert or update. Returns the previous value, or {@code null} if absent.
     * Acquires write lock; triggers resize if load factor exceeded.
     */
    public V put(K key, V value) {
        writeLock.lock();
        try {
            int h   = hash(key);
            int idx = indexFor(h, table.length);
            // Walk the bucket chain — update if key exists
            for (Entry<K, V> e = table[idx]; e != null; e = e.next) {
                if (e.hash == h && keysEqual(e.key, key)) {
                    V old = e.value;
                    e.value = value;
                    return old;
                }
            }
            // Prepend new entry
            table[idx] = new Entry<>(key, value, h, table[idx]);
            if (++size > threshold) resize();
            return null;
        } finally {
            writeLock.unlock();
        }
    }

    /** Remove by key. Returns the removed value, or {@code null} if absent. */
    public V remove(K key) {
        writeLock.lock();
        try {
            int h    = hash(key);
            int idx  = indexFor(h, table.length);
            Entry<K, V> prev = null, e = table[idx];
            while (e != null) {
                if (e.hash == h && keysEqual(e.key, key)) {
                    if (prev == null) table[idx] = e.next;
                    else prev.next = e.next;
                    size--;
                    return e.value;
                }
                prev = e;
                e    = e.next;
            }
            return null;
        } finally {
            writeLock.unlock();
        }
    }

    public boolean containsKey(K key) {
        return get(key) != null;
    }

    public int size() {
        readLock.lock();
        try   { return size; }
        finally { readLock.unlock(); }
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    /** Returns a snapshot of all keys — acquires read lock. */
    public Set<K> keySet() {
        readLock.lock();
        try {
            Set<K> keys = new HashSet<>(size * 2);
            for (Entry<K, V> bucket : table) {
                for (Entry<K, V> e = bucket; e != null; e = e.next)
                    keys.add(e.key);
            }
            return keys;
        } finally {
            readLock.unlock();
        }
    }

    public void clear() {
        writeLock.lock();
        try {
            Arrays.fill(table, null);
            size = 0;
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public String toString() {
        readLock.lock();
        try {
            StringBuilder sb = new StringBuilder("{");
            boolean first = true;
            for (Entry<K, V> bucket : table) {
                for (Entry<K, V> e = bucket; e != null; e = e.next) {
                    if (!first) sb.append(", ");
                    sb.append(e.key).append('=').append(e.value);
                    first = false;
                }
            }
            return sb.append('}').toString();
        } finally {
            readLock.unlock();
        }
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /** Wang/Jenkins bit-spreading to minimise clustering in power-of-2 tables. */
    private int hash(K key) {
        if (key == null) return 0;
        int h = key.hashCode();
        return h ^ (h >>> 16);
    }

    private int indexFor(int hash, int length) {
        return hash & (length - 1);
    }

    private boolean keysEqual(K a, K b) {
        return a == b || (a != null && a.equals(b));
    }

    /** Must be called under writeLock. Doubles table capacity. */
    private void resize() {
        int oldCap = table.length;
        int newCap = oldCap << 1;
        if (newCap < 0) return; // int overflow guard — table already huge
        Entry<K, V>[] newTable = new Entry[newCap];
        for (Entry<K, V> bucket : table) {
            Entry<K, V> e = bucket;
            while (e != null) {
                Entry<K, V> next = e.next;
                int idx = indexFor(e.hash, newCap);
                e.next = newTable[idx];
                newTable[idx] = e;
                e = next;
            }
        }
        table     = newTable;
        threshold = (int)(newCap * LOAD_FACTOR);
    }

    private int nextPowerOfTwo(int n) {
        n--;
        n |= n >> 1; n |= n >> 2; n |= n >> 4; n |= n >> 8; n |= n >> 16;
        return n + 1;
    }
}
