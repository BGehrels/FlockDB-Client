package info.gehrels.flockDBClient;

import com.twitter.flockdb.thrift.FlockException;

/**
 * Thrown to indicate, that the FlockDB server threw an FlockException and propagated it to the client (us). Wraps the
 * FlockException, so that some information concerning the original problem should be provided. Additional information
 * may be found in the FlockDB server logs.
 */
public class FlockDBException extends RuntimeException {

	public FlockDBException(FlockException e) {
		super(e);
	}
}
