package aerocache.client;

/**
 * Thrown by {@link AeroCacheClient} when a cache node is unreachable after
 * one reconnect attempt and the request cannot be completed.
 *
 * <p>Callers should catch this exception and implement application-level
 * fallback logic (e.g. read from a database, return a default value).
 *
 * <p>This is a checked exception so callers are forced by the compiler to
 * acknowledge that network operations can fail — an intentional API design
 * choice for a distributed system client library.
 */
public class AeroCacheConnectionException extends Exception {

    private final String targetNode; // "host:port" of the failed node

    public AeroCacheConnectionException(String targetNode, String message) {
        super("AeroCache node [" + targetNode + "] unreachable: " + message);
        this.targetNode = targetNode;
    }

    public AeroCacheConnectionException(String targetNode, String message, Throwable cause) {
        super("AeroCache node [" + targetNode + "] unreachable: " + message, cause);
        this.targetNode = targetNode;
    }

    /** The "host:port" string of the node that could not be reached. */
    public String getTargetNode() {
        return targetNode;
    }
}
