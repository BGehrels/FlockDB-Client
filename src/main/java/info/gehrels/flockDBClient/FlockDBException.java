package info.gehrels.flockDBClient;

/**
 * Thrown to indicate, that either the FlockDB server threw an FlockException and propagated it to the client (us), or a
 * ThriftException was thrown, which may indicate a network communication error. A FlockDBException wraps the original
 * exception, so that some information concerning the original problem should be provided. Additional information may be
 * found in the FlockDB server logs.
 */
public class FlockDBException extends RuntimeException {

	public FlockDBException(Exception e) {
		super(e);
	}
}
