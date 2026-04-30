package aerocache.net;

/**
 * Stateless parser for the AeroCache wire protocol.
 *
 * <h3>Wire format</h3>
 * All commands are plain ASCII text, terminated by {@code \r\n} (CRLF).
 * {@link java.io.BufferedReader#readLine()} strips the terminator automatically,
 * so this class works on pre-stripped single-line strings.
 *
 * <pre>
 * Client → Server:
 *   GET  &lt;key&gt;\r\n
 *   SET  &lt;key&gt; &lt;value&gt;\r\n          ← value may contain spaces
 *   DEL  &lt;key&gt;\r\n
 *   PING\r\n
 *   REPLICATE SET &lt;key&gt; &lt;value&gt;\r\n  ← internal master-to-slave sync
 *   REPLICATE DEL &lt;key&gt;\r\n
 *
 * Server → Client:
 *   +OK\r\n                         ← successful write
 *   +PONG\r\n                       ← heartbeat reply
 *   $&lt;value&gt;\r\n                    ← successful read (GET hit)
 *   $-1\r\n                         ← cache miss or key deleted
 *   -ERR &lt;message&gt;\r\n              ← protocol / internal error
 * </pre>
 *
 * <h3>SET value quoting</h3>
 * The value in a SET command is captured as everything after the first space
 * following the key, so values containing internal spaces are handled naturally
 * without any quoting scheme (similar to memcached's approach).
 *
 * <h3>Thread safety</h3>
 * {@link #parse(String)} is a pure function with no shared state — safe to call
 * from any number of virtual threads concurrently.
 */
public final class CommandParser {

    // -----------------------------------------------------------------------
    // Command enum
    // -----------------------------------------------------------------------

    /** All commands understood by the AeroCache text protocol. */
    public enum Command {
        /** Read a value by key. */
        GET,
        /** Insert or update a key-value pair. */
        SET,
        /** Remove a key from the cache. */
        DEL,
        /** Heartbeat probe — server must reply +PONG immediately. */
        PING,
        /**
         * Internal master-to-slave replication command.
         * Carries an inner SET or DEL command as its payload.
         */
        REPLICATE,
        /** Returned when the input cannot be parsed into a known command. */
        UNKNOWN
    }

    // -----------------------------------------------------------------------
    // ParsedCommand — returned by parse()
    // -----------------------------------------------------------------------

    /**
     * Immutable result of parsing one line of the wire protocol.
     *
     * <p>Access arguments with {@link #arg(int)} rather than indexing {@link #args}
     * directly — it performs a bounds-checked retrieval and returns {@code null}
     * instead of throwing {@link ArrayIndexOutOfBoundsException}.
     */
    public static final class ParsedCommand {

        /** The recognised command verb, or {@link Command#UNKNOWN} on parse failure. */
        public final Command command;

        /**
         * Arguments extracted from the command line:
         * <ul>
         *   <li>GET       → args[0] = key</li>
         *   <li>SET       → args[0] = key, args[1] = value (may contain spaces)</li>
         *   <li>DEL       → args[0] = key</li>
         *   <li>PING      → args is empty</li>
         *   <li>REPLICATE → args[0] = the raw inner command line</li>
         *   <li>UNKNOWN   → args[0] = error description (for -ERR replies)</li>
         * </ul>
         */
        public final String[] args;

        /** The original, unmodified input line (useful for logging). */
        public final String raw;

        ParsedCommand(Command command, String[] args, String raw) {
            this.command = command;
            this.args    = args;
            this.raw     = raw;
        }

        /**
         * Bounds-safe argument accessor.
         *
         * @param index zero-based argument index
         * @return the argument string, or {@code null} if out of range
         */
        public String arg(int index) {
            return (args != null && index < args.length) ? args[index] : null;
        }

        @Override
        public String toString() {
            return "ParsedCommand{cmd=" + command + ", args=" + java.util.Arrays.toString(args) + "}";
        }
    }

    // Private constructor — this is a static utility class.
    private CommandParser() {}

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /**
     * Parse a single CRLF-stripped line into a {@link ParsedCommand}.
     *
     * <p>The method is deliberately lenient: it never throws. On any parse
     * failure it returns a {@link Command#UNKNOWN} result whose first arg
     * contains a human-readable reason — {@link CommandHandler} turns this
     * into a {@code -ERR ...} response.
     *
     * @param line a single command line with the trailing \r\n already removed
     * @return a non-null {@link ParsedCommand}
     */
    public static ParsedCommand parse(String line) {
        if (line == null || line.isBlank()) {
            return unknown(line, "empty input");
        }

        String trimmed = line.trim();

        // Split on the FIRST space only so that values with spaces are preserved.
        // parts[0] = verb, parts[1] = everything after the first space (may be empty).
        String[] parts = trimmed.split(" ", 2);
        String verb = parts[0].toUpperCase();
        String rest = parts.length > 1 ? parts[1] : "";

        switch (verb) {

            case "GET": {
                // GET <key>
                if (rest.isBlank())
                    return unknown(line, "GET requires <key>");
                return new ParsedCommand(Command.GET, new String[]{rest.trim()}, line);
            }

            case "SET": {
                // SET <key> <value>
                // Split rest into key and value on the first space so the value
                // can itself contain spaces (e.g. JSON blobs, serialised objects).
                String[] kv = rest.split(" ", 2);
                if (kv.length < 2 || kv[0].isBlank())
                    return unknown(line, "SET requires <key> <value>");
                // kv[1] may be an empty string — we allow empty values.
                return new ParsedCommand(Command.SET, new String[]{kv[0].trim(), kv[1]}, line);
            }

            case "DEL": {
                // DEL <key>
                if (rest.isBlank())
                    return unknown(line, "DEL requires <key>");
                return new ParsedCommand(Command.DEL, new String[]{rest.trim()}, line);
            }

            case "PING": {
                // PING  (no arguments)
                return new ParsedCommand(Command.PING, new String[0], line);
            }

            case "REPLICATE": {
                // REPLICATE <inner-command-line>
                // The inner command is stored verbatim as args[0] so that
                // CommandHandler can re-parse it for the appropriate local write.
                if (rest.isBlank())
                    return unknown(line, "REPLICATE requires an inner command");
                return new ParsedCommand(Command.REPLICATE, new String[]{rest}, line);
            }

            default:
                return unknown(line, "unrecognised verb '" + parts[0] + "'");
        }
    }

    // -----------------------------------------------------------------------
    // Private helpers
    // -----------------------------------------------------------------------

    /** Construct an UNKNOWN command carrying the error reason in args[0]. */
    private static ParsedCommand unknown(String raw, String reason) {
        return new ParsedCommand(Command.UNKNOWN, new String[]{reason}, raw);
    }
}
