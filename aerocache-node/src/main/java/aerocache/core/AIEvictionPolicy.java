package aerocache.core;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

/**
 * AI-driven eviction policy that queries the Python FastAPI microservice for
 * "cold" keys to evict, overriding standard LRU when memory is at capacity.
 *
 * <h3>Flow</h3>
 * <pre>
 *   CacheEngine at capacity
 *         │
 *         ▼
 *   AIEvictionPolicy.evict()
 *         │
 *         ├─→ Build JSON payload from AccessStats snapshot
 *         │        (key, frequency, last_accessed, recent timestamps)
 *         │
 *         ├─→ HttpClient.send() POST /predict-evictions
 *         │        timeout = 200 ms
 *         │
 *         ├─[success]─→ Parse cold_keys[] from JSON response
 *         │              engine.evictKeys(coldKeys)
 *         │
 *         └─[timeout / error / 5xx]
 *                   │
 *                   ▼
 *              fallback.evict(engine)   ← standard LRU, always succeeds
 * </pre>
 *
 * <h3>200 ms timeout rationale</h3>
 * The cache must never stall client requests waiting for an ML model to respond.
 * 200 ms is roughly 4× the typical LAN round-trip (≤50 ms) — generous enough
 * for IsolationForest inference, strict enough to protect throughput.
 *
 * <h3>JSON handling without external libraries</h3>
 * The Java standard library includes {@link java.net.http.HttpClient} (Java 11+)
 * but no JSON parser. We manually build the request payload with {@link StringBuilder}
 * and parse the response with simple string operations. This keeps the core engine
 * dependency-free, as per the project requirements.
 *
 * <h3>Thread safety</h3>
 * {@link HttpClient} is thread-safe and reused across evictions (built once in
 * the constructor). The {@code AccessStats} snapshot is taken atomically by
 * {@link CacheEngine#getAccessLogSnapshot()} — a copy returned while the engine
 * lock is held. HTTP I/O happens on a virtual thread inside the {@code send()} call.
 */
public class AIEvictionPolicy<K, V> implements EvictionPolicy<K, V> {

    private static final Logger LOG = Logger.getLogger(AIEvictionPolicy.class.getName());

    /** Hard timeout for the HTTP round-trip to the AI microservice. */
    private static final Duration AI_TIMEOUT = Duration.ofMillis(200);

    /** Number of cold keys to request from the AI service per eviction event. */
    private static final int DEFAULT_EVICT_COUNT = 5;

    // -----------------------------------------------------------------------
    // Dependencies
    // -----------------------------------------------------------------------

    /** Reusable HTTP/1.1 client — thread-safe, built once. */
    private final HttpClient httpClient;

    /** Full URI of the AI prediction endpoint. */
    private final URI predictUri;

    /**
     * Fallback invoked when the AI service is unavailable, slow, or returns an error.
     * Always succeeds — removes the DLL tail in O(1).
     */
    private final LRUEvictionPolicy<K, V> fallback = new LRUEvictionPolicy<>();

    /** Number of cold keys to request per eviction call. */
    private final int evictCount;

    // -----------------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------------

    /**
     * @param aiHost     hostname of the Python microservice (e.g. "aerocache-ai")
     * @param aiPort     port of the Python microservice (e.g. 8000)
     * @param evictCount how many cold keys to request per eviction event
     */
    public AIEvictionPolicy(String aiHost, int aiPort, int evictCount) {
        this.predictUri = URI.create("http://" + aiHost + ":" + aiPort + "/predict-evictions");
        this.evictCount = evictCount;
        // Connect timeout is separate from the request timeout below.
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(500))
                .build();
        LOG.info("[AIEvictionPolicy] Configured. AI endpoint=" + predictUri
                + ", timeout=" + AI_TIMEOUT.toMillis() + " ms, evictCount=" + evictCount);
    }

    public AIEvictionPolicy(String aiHost, int aiPort) {
        this(aiHost, aiPort, DEFAULT_EVICT_COUNT);
    }

    // -----------------------------------------------------------------------
    // EvictionPolicy implementation
    // -----------------------------------------------------------------------

    /**
     * Attempt AI-driven eviction; fall back to LRU on any failure.
     *
     * <p>This method is invoked while {@code CacheEngine.engineLock} is held,
     * so it must complete quickly. The 200 ms HTTP timeout enforces this.
     * On fallback the method delegates to {@link LRUEvictionPolicy#evict} which
     * is guaranteed O(1) and never throws.
     *
     * @param engine the cache engine to evict from
     */
    @Override
    public void evict(CacheEngine<K, V> engine) {
        try {
            // --- Step 1: Snapshot access logs (safe copy, engine lock held) ---
            Map<K, AccessStats> logs = engine.getAccessLogSnapshot();

            if (logs.isEmpty()) {
                LOG.fine("[AIEvictionPolicy] No access logs yet — falling back to LRU.");
                fallback.evict(engine);
                return;
            }

            // --- Step 2: Build JSON request body ---
            String requestBody = buildRequestJson(logs, evictCount);

            // --- Step 3: Send HTTP POST with strict timeout ---
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(predictUri)
                    .timeout(AI_TIMEOUT)                          // 200 ms total
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                    .build();

            HttpResponse<String> response = httpClient.send(
                    request,
                    HttpResponse.BodyHandlers.ofString());

            // --- Step 4: Validate response status ---
            int status = response.statusCode();
            if (status < 200 || status >= 300) {
                LOG.warning("[AIEvictionPolicy] AI service returned HTTP " + status
                        + " — falling back to LRU.");
                fallback.evict(engine);
                return;
            }

            // --- Step 5: Parse cold_keys from JSON response ---
            List<String> coldKeys = parseColdKeys(response.body());

            if (coldKeys.isEmpty()) {
                LOG.warning("[AIEvictionPolicy] AI returned empty cold_keys — falling back to LRU.");
                fallback.evict(engine);
                return;
            }

            // --- Step 6: Evict the cold keys ---
            // CacheEngine.evictKeys() silently skips any key no longer present.
            @SuppressWarnings("unchecked")
            List<K> typedKeys = (List<K>) coldKeys;
            int removed = engine.evictKeys(typedKeys);

            LOG.info("[AIEvictionPolicy] AI eviction: requested=" + coldKeys.size()
                    + " evicted=" + removed + " cold keys: " + coldKeys);

        } catch (java.net.http.HttpTimeoutException e) {
            // 200 ms deadline exceeded — this is the primary expected failure mode.
            LOG.warning("[AIEvictionPolicy] AI service timed out (" + AI_TIMEOUT.toMillis()
                    + " ms). Falling back to LRU.");
            fallback.evict(engine);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warning("[AIEvictionPolicy] HTTP call interrupted — falling back to LRU.");
            fallback.evict(engine);

        } catch (Exception e) {
            // Catch-all: connection refused, malformed response, cast failure, etc.
            LOG.warning("[AIEvictionPolicy] AI call failed (" + e.getClass().getSimpleName()
                    + ": " + e.getMessage() + ") — falling back to LRU.");
            fallback.evict(engine);
        }
    }

    // -----------------------------------------------------------------------
    // JSON building — stdlib only, no external libraries
    // -----------------------------------------------------------------------

    /**
     * Manually construct the JSON request body matching the Python
     * {@code EvictionRequest} Pydantic schema:
     *
     * <pre>
     * {
     *   "evict_count": 5,
     *   "access_logs": [
     *     {
     *       "key":           "session:user123",
     *       "frequency":     42,
     *       "last_accessed": 1714500000000,
     *       "timestamps":    [1714499900000, 1714499950000, 1714500000000]
     *     },
     *     ...
     *   ]
     * }
     * </pre>
     */
    private String buildRequestJson(Map<K, AccessStats> logs, int count) {
        StringBuilder sb = new StringBuilder(256 + logs.size() * 128);
        sb.append("{\"evict_count\":").append(count).append(",\"access_logs\":[");

        boolean firstEntry = true;
        for (Map.Entry<K, AccessStats> entry : logs.entrySet()) {
            if (!firstEntry) sb.append(',');
            firstEntry = false;

            AccessStats s = entry.getValue();
            List<Long> ts = s.getRecentTimestamps();

            sb.append("{\"key\":\"").append(jsonEscape(entry.getKey().toString())).append('"');
            sb.append(",\"frequency\":").append(s.getFrequency());
            sb.append(",\"last_accessed\":").append(s.getLastAccessedMs());
            sb.append(",\"timestamps\":[");

            for (int i = 0; i < ts.size(); i++) {
                if (i > 0) sb.append(',');
                sb.append(ts.get(i));
            }
            sb.append("]}");
        }

        sb.append("]}");
        return sb.toString();
    }

    /**
     * Escape a string value for safe inclusion in a JSON string literal.
     * Handles backslash and double-quote, which are the only characters
     * that can legally appear in cache keys and would break JSON.
     */
    private String jsonEscape(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    // -----------------------------------------------------------------------
    // JSON parsing — stdlib only, no external libraries
    // -----------------------------------------------------------------------

    /**
     * Extract the {@code cold_keys} string array from the AI response JSON.
     *
     * <p>Expected response format:
     * <pre>{"cold_keys":["key1","key2","key3"],"model_used":"IsolationForest"}</pre>
     *
     * <p>We use simple index-based string search rather than a full JSON parser.
     * This is intentionally minimal — the response schema is fixed and controlled
     * by our own Python microservice.
     *
     * @param json raw response body from the AI microservice
     * @return list of cold key strings; empty list on any parse failure
     */
    private List<String> parseColdKeys(String json) {
        List<String> keys = new ArrayList<>();
        if (json == null || json.isBlank()) return keys;

        try {
            // Locate the cold_keys array: find "[" after "cold_keys"
            int labelIdx = json.indexOf("\"cold_keys\"");
            if (labelIdx < 0) return keys;

            int arrayStart = json.indexOf('[', labelIdx);
            int arrayEnd   = json.indexOf(']', arrayStart);
            if (arrayStart < 0 || arrayEnd < 0) return keys;

            String arrayContent = json.substring(arrayStart + 1, arrayEnd).trim();
            if (arrayContent.isEmpty()) return keys;

            // Split on comma and strip surrounding quotes from each element.
            for (String token : arrayContent.split(",")) {
                String key = token.trim();
                // Strip leading/trailing double-quotes.
                if (key.startsWith("\"")) key = key.substring(1);
                if (key.endsWith("\""))   key = key.substring(0, key.length() - 1);
                if (!key.isEmpty()) keys.add(key);
            }

        } catch (Exception e) {
            LOG.warning("[AIEvictionPolicy] Failed to parse AI response JSON: " + e.getMessage()
                    + " | raw=" + json);
        }

        return keys;
    }
}
