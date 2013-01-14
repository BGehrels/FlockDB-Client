package info.gehrels.flockDBClient;

import com.twitter.flockdb.thrift.FlockException;
import info.gehrels.flockDBClient.FlockAndThriftExceptionHandling.MethodObject;
import org.apache.thrift.TException;
import org.junit.Test;

import java.io.IOException;

import static info.gehrels.flockDBClient.FlockAndThriftExceptionHandling.handleFlockAndThriftExceptions;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class FlockAndThriftExceptionHandlingTest {

	private static final String ARBITRARY_TEST_STRING = "arbitraryTestString";

	@Test
	public void returnsMethodObjectsValueWhenNoExceptionOccured() throws IOException {
		String s = handleFlockAndThriftExceptions(new MethodObject<String>() {
			@Override
			public String call() throws TException, FlockException {
				return ARBITRARY_TEST_STRING;
			}
		});

		assertThat(s, is(ARBITRARY_TEST_STRING));
	}

	@Test(expected = FlockDBException.class)
	public void wrapsAThrownFlockExceptionAsARuntimeExceptionCalledFlockDBException() throws IOException {
		handleFlockAndThriftExceptions(new MethodObject<String>() {
					@Override
					public String call() throws TException, FlockException {
						throw new FlockException();
					}
				});

	}

	@Test(expected = FlockDBException.class)
	public void wrapsAThrownTExceptionAsAFlockDBException() throws IOException {
		handleFlockAndThriftExceptions(new MethodObject<String>() {
					@Override
					public String call() throws TException, FlockException {
						throw new TException();
					}
				});
	}

	@Test(expected = IllegalArgumentException.class)
	public void rethrowsAnyOtherExceptions() throws IOException {
		handleFlockAndThriftExceptions(new MethodObject<String>() {
			@Override
			public String call() throws TException, FlockException {
				throw new IllegalArgumentException();
			}
		});

	}


}
