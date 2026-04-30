package aerocache.net;

import aerocache.core.CacheEngine;
import java.util.logging.Logger;

/**
 * Dispatches a {@link CommandParser.ParsedCommand} to the local {@link CacheEngine}
 * and formats a wire-protocol response string.
 *
 * <h3>Response format (returned as a plain String — TCPServer appends \r\n)</h3>
 * <pre>
 *   +OK         → successful SET / DEL
 *   +PONG       → PING reply
 *   $&lt;value&gt;   → GET hit  (the '$' prefix signals a string bulk reply)
 *   $-1         → GET miss or DEL of a non-existent key
 *   -ERR &lt;msg&gt; → protocol error or unexpected exception
 * </pre>
 *
 * <h3>REPLICATE command</h3>
 * The REPLICATE verb carries an inner command line as its first argument.
 * The handler re-parses that inner line and applies the corresponding write
 * directly to the local {@code CacheEngine} without going through the hash ring —
 * the master already decided routing; the slave just persists the change locally.
 *
 * <h3>Thread safety</h3>
 * {@code CommandHandler} holds no mutable state of its own. Thread safety of
 * all data mutations is guaranteed by {@link CacheEngine}'s internal
 * {@code ReentrantLock}. This class is safe to share across all client sessions.
 */
public class CommandHandler {

    private static final Logger LOG = Logger.getLogger(CommandHandler.class.getName());

    // -----------------------------------------------------------------------
    // Wire-protocol response constants
    // -----------------------------------------------------------------------

    /** Successful write acknowledgement. */
    static final String RESP_OK        = "+OK";

    /** Heartbeat acknowledgement. */
    static final String RESP_PONG      = "+PONG";

    /** Null bulk-string reply — cache miss or key not found. */
    static final String RESP_NULL      = "$-1";

    /** Error response prefix — followed by a human-readable message. */
    static final String RESP_ERR       = "-ERR ";

    /** Bulk-string reply prefix — followed by the actual value. */
    static final String RESP_BULK      = "$";

    // -----------------------------------------------------------------------
    // State
    // -----------------------------------------------------------------------

    private final CacheEngine<String, String> engine;

    // -----------------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------------

    /**
     * @param engine the local cache engine this handler reads from / writes to
     */
    public CommandHandler(CacheEngine<String, String> engine) {
        if (engine == null) throw new IllegalArgumentException("CacheEngine must not be null");
        this.engine = engine;
    }

    // -----------------------------------------------------------------------
    // Public dispatch
    // -----------------------------------------------------------------------

    /**
     * Handle a parsed command and return the wire-protocol response string.
     *
     * <p>This is the single entry-point called by every virtual thread's
     * client-session loop inside {@link TCPServer}. The caller appends
     * {@code \r\n} before writing to the socket.
     *
     * @param cmd the result of {@link CommandParser#parse(String)}
     * @return a non-null, non-empty response string (without trailing CRLF)
     */
    public String handle(CommandParser.ParsedCommand cmd) {
        try {
            switch (cmd.command) {
                case GET:       return handleGet(cmd);
                case SET:       return handleSet(cmd);
                case DEL:       return handleDel(cmd);
                case PING:      return RESP_PONG;
                case REPLICATE: return handleReplicate(cmd);
                default:
                    // UNKNOWN — args[0] contains the error reason from the parser
                    String reason = cmd.arg(0);
                    return RESP_ERR + (reason != null ? reason : "unknown command");
            }
        } catch (Exception e) {
            // Defensive catch: no exception should escape and crash the session thread.
            LOG.warning("[CommandHandler] Unhandled error for cmd='" + cmd.raw + "': " + e.getMessage());
            return RESP_ERR + "internal error: " + e.getMessage();
        }
    }

    // -----------------------------------------------------------------------
    // Individual command handlers
    // -----------------------------------------------------------------------

    /**
     * GET &lt;key&gt;
     *
     * Returns {@code $<value>} on a cache hit, {@code $-1} on a miss.
     * A miss can mean the key never existed, was evicted, or its TTL expired.
     */
    private String handleGet(CommandParser.ParsedCommand cmd) {
        String key = cmd.arg(0);
        if (key == null) return RESP_ERR + "GET requires <key>";

        String value = engine.get(key);

        if (value == null) {
            LOG.fine("[CommandHandler] GET miss: key=" + key);
            return RESP_NULL;
        }

        LOG.fine("[CommandHandler] GET hit:  key=" + key);
        // Prefix the value with '$' to distinguish it from status replies.
        return RESP_BULK + value;
    }

    /**
     * SET &lt;key&gt; &lt;value&gt;
     *
     * Inserts or updates the key. If the cache is at capacity the configured
     * {@code EvictionPolicy} fires inside {@link CacheEngine#put} before the
     * new entry is admitted.
     */
    private String handleSet(CommandParser.ParsedCommand cmd) {
        String key   = cmd.arg(0);
        String value = cmd.arg(1);
        if (key == null || value == null) return RESP_ERR + "SET requires <key> <value>";

        engine.put(key, value);
        LOG.fine("[CommandHandler] SET key=" + key);
        return RESP_OK;
    }

    /**
     * DEL &lt;key&gt;
     *
     * Removes the key. Returns {@code +OK} if the key existed, {@code $-1} if not.
     * This allows callers to distinguish between "deleted" and "never existed"
     * without a separate EXISTS command.
     */
    private String handleDel(CommandParser.ParsedCommand cmd) {
        String key = cmd.arg(0);
        if (key == null) return RESP_ERR + "DEL requires <key>";

        boolean removed = engine.delete(key);
        LOG.fine("[CommandHandler] DEL key=" + key + " removed=" + removed);
        return removed ? RESP_OK : RESP_NULL;
    }

    /**
     * REPLICATE &lt;inner-command-line&gt;
     *
     * <p>The inner command is re-parsed by {@link CommandParser} and then applied
     * directly to the local {@code CacheEngine}. Only SET and DEL are supported
     * as inner commands — GET and PING have no write-side effect to replicate.
     *
     * <p>Example wire format from master:
     * <pre>
     *   REPLICATE SET session:user123 {"name":"Alice","role":"admin"}\r\n
     *   REPLICATE DEL session:user999\r\n
     * </pre>
     *
     * <p>The handler logs at FINE level to keep slave logs quiet during normal
     * steady-state replication. Promotion events are logged by PromotionHandler.
     */
    private String handleReplicate(CommandParser.ParsedCommand cmd) {
        String innerLine = cmd.arg(0);
        if (innerLine == null || innerLine.isBlank())
            return RESP_ERR + "REPLICATE requires an inner command";

        // Re-parse the inner command using the same protocol parser.
        CommandParser.ParsedCommand inner = CommandParser.parse(innerLine);

        switch (inner.command) {

            case SET: {
                String key   = inner.arg(0);
                String value = inner.arg(1);
                if (key == null || value == null)
                    return RESP_ERR + "REPLICATE SET malformed — requires <key> <value>";
                engine.put(key, value);
                LOG.fine("[CommandHandler] REPLICATE SET key=" + key);
                return RESP_OK;
            }

            case DEL: {
                String key = inner.arg(0);
                if (key == null)
                    return RESP_ERR + "REPLICATE DEL malformed — requires <key>";
                engine.delete(key);
                LOG.fine("[CommandHandler] REPLICATE DEL key=" + key);
                return RESP_OK;
            }

            default:
                // Reject anything that shouldn't be replicated (GET, PING, etc.)
                return RESP_ERR + "REPLICATE only supports SET and DEL, got: " + inner.command;
        }
    }
}
