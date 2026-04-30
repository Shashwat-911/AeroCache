package aerocache.core;

/**
 * Custom intrusive doubly-linked list for O(1) LRU tracking.
 *
 * <pre>
 *   [head sentinel] <-> [MRU node] <-> ... <-> [LRU node] <-> [tail sentinel]
 * </pre>
 *
 * <ul>
 *   <li>{@link #addToFront} — insert a node as the most-recently-used entry: O(1)</li>
 *   <li>{@link #remove}     — unlink any node at any position: O(1)</li>
 *   <li>{@link #moveToFront}— re-promote a node on cache hit: O(1)</li>
 *   <li>{@link #removeLast} — evict the least-recently-used entry: O(1)</li>
 * </ul>
 *
 * <strong>Thread safety:</strong> this class is NOT internally synchronised.
 * Callers (i.e. {@link CacheEngine}) must hold an appropriate lock.
 */
public class DoublyLinkedList<K, V> {

    // -----------------------------------------------------------------------
    // Node — public so CacheEngine can store direct references in its HashMap
    // -----------------------------------------------------------------------
    public static class Node<K, V> {
        public K key;
        public V value;
        Node<K, V> prev;
        Node<K, V> next;

        /** Create a real data node. */
        public Node(K key, V value) {
            this.key   = key;
            this.value = value;
        }

        /** Sentinel constructor — no data. */
        Node() {}

        @Override
        public String toString() {
            return "Node{key=" + key + ", value=" + value + "}";
        }
    }

    // -----------------------------------------------------------------------
    // State — sentinel nodes are always present; never removed
    // -----------------------------------------------------------------------
    private final Node<K, V> head = new Node<>();  // MRU boundary
    private final Node<K, V> tail = new Node<>();  // LRU boundary
    private int size;

    public DoublyLinkedList() {
        head.next = tail;
        tail.prev = head;
    }

    // -----------------------------------------------------------------------
    // Core operations — all O(1)
    // -----------------------------------------------------------------------

    /**
     * Link {@code node} immediately after the head sentinel, making it the
     * most-recently-used entry. The node must NOT already be in the list.
     */
    public void addToFront(Node<K, V> node) {
        node.prev      = head;
        node.next      = head.next;
        head.next.prev = node;
        head.next      = node;
        size++;
    }

    /**
     * Unlink {@code node} from its current position. Safe to call even if
     * the node has already been unlinked (guards against double-remove).
     */
    public void remove(Node<K, V> node) {
        if (node.prev == null || node.next == null) return; // already unlinked
        node.prev.next = node.next;
        node.next.prev = node.prev;
        node.prev      = null;
        node.next      = null;
        size--;
    }

    /**
     * Move an existing (already-linked) node to the MRU front.
     * Used on every cache hit to maintain LRU order.
     */
    public void moveToFront(Node<K, V> node) {
        remove(node);
        addToFront(node);
    }

    /**
     * Remove and return the least-recently-used real node (the node just
     * before the tail sentinel). Returns {@code null} if the list is empty.
     */
    public Node<K, V> removeLast() {
        Node<K, V> lru = tail.prev;
        if (lru == head) return null; // list is empty
        remove(lru);
        return lru;
    }

    /**
     * Peek at the LRU node without removing it.
     * Returns {@code null} if empty.
     */
    public Node<K, V> peekLast() {
        Node<K, V> lru = tail.prev;
        return (lru == head) ? null : lru;
    }

    // -----------------------------------------------------------------------
    // Accessors
    // -----------------------------------------------------------------------

    public int size()      { return size; }
    public boolean isEmpty() { return size == 0; }

    /**
     * Diagnostic snapshot: returns keys from MRU → LRU for logging/testing.
     * O(n) — do not call on hot paths.
     */
    public Object[] toKeyArray() {
        Object[] keys = new Object[size];
        Node<K, V> cur = head.next;
        int i = 0;
        while (cur != tail) {
            keys[i++] = cur.key;
            cur = cur.next;
        }
        return keys;
    }
}
