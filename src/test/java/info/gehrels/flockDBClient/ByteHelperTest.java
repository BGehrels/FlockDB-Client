package info.gehrels.flockDBClient;

import org.junit.Test;

import static info.gehrels.flockDBClient.ByteHelper.asByteBuffer;
import static info.gehrels.flockDBClient.ByteHelper.toLongArray;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ByteHelperTest {
	@Test
	public void encodesAndDecodesTransparently() {
		long[] testData = new long[]{12345L, 987654321L, 1L, 9L};

		long[] result = toLongArray(asByteBuffer(testData).array());

		assertThat(result, is(equalTo(testData)));
	}

	@Test
	public void decodesAndEncodesTransparently() {
		byte[] testData = new byte[]{57, 48, 0, 0, 0, 0, 0, 0, -79, 104, -34, 58, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0};
		byte[] result = asByteBuffer(toLongArray(testData)).array();

		assertThat(result, is(equalTo(testData)));
	}


}
