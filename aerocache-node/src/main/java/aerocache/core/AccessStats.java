package aerocache.core;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Per-key access statistics collected by CacheEngine and forwarded
 * to the AI microservice to identify "cold" keys for predictive eviction.
 */
public class AccessStats {

    private static final int MAX_TIMESTAMPS = 20;

    private int frequency;
    private long lastAccessedMs;
    private final Deque<Long> recentTimestamps;

    public AccessStats() {
        this.frequency = 0;
        this.lastAccessedMs = 0;
        this.recentTimestamps = new ArrayDeque<>();
    }

    /** Record a single access at the given epoch-millisecond timestamp. */
    public synchronized void recordAccess(long timestampMs) {
        frequency++;
        lastAccessedMs = timestampMs;
        recentTimestamps.addLast(timestampMs);
        if (recentTimestamps.size() > MAX_TIMESTAMPS) {
            recentTimestamps.pollFirst();
        }
    }

    public synchronized int getFrequency() {
        return frequency;
    }

    public synchronized long getLastAccessedMs() {
        return lastAccessedMs;
    }

    public synchronized List<Long> getRecentTimestamps() {
        return new ArrayList<>(recentTimestamps);
    }

    @Override
    public synchronized String toString() {
        return "AccessStats{freq=" + frequency
                + ", lastMs=" + lastAccessedMs
                + ", samples=" + recentTimestamps.size() + "}";
    }
}
