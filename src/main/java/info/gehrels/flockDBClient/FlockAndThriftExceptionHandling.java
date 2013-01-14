package info.gehrels.flockDBClient;

import com.twitter.flockdb.thrift.FlockException;
import org.apache.thrift.TException;

import java.io.IOException;

public class FlockAndThriftExceptionHandling {
	public static <T> T handleFlockAndThriftExceptions(MethodObject<T> methodObject) throws IOException {
		try {
			return methodObject.call();
		} catch (TException e) {
			throw new IOException(e);
		} catch (FlockException e) {
			throw new FlockDBException(e);
		}
	}

	abstract static class MethodObject<T> {
		public abstract T call() throws TException, FlockException;
	}
}
