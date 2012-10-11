/*
 * Copyright 2012 Benjamin Gehrels
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 *
 * limitations under the License.
 */

package info.gehrels.flockDBClient;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;

import static org.apache.commons.lang.ArrayUtils.isNotEmpty;

final class ByteHelper {
	static ByteBuffer asByteBufferOrNull(long... destinationIds) {
		ByteBuffer buf = null;
		if (isNotEmpty(destinationIds)) {
			buf = ByteBuffer.wrap(new byte[destinationIds.length * (Long.SIZE / 8)]);
			buf.order(ByteOrder.LITTLE_ENDIAN);
			for (long destinationId : destinationIds) {
				buf.putLong(destinationId);
			}
			buf.rewind();
		}
		return buf;
	}


	static ByteBuffer asByteBuffer(long... destinationIds) {
		ByteBuffer buf = null;
		buf = ByteBuffer.wrap(new byte[destinationIds.length * (Long.SIZE / 8)]);
		buf.order(ByteOrder.LITTLE_ENDIAN);
		for (long destinationId : destinationIds) {
			buf.putLong(destinationId);
		}
		buf.rewind();
		return buf;
	}

	static long[] toLongArray(byte[] ids) {
		LongBuffer buffy = ByteBuffer.wrap(ids).order(ByteOrder.LITTLE_ENDIAN).asLongBuffer();
		long[] result = new long[buffy.capacity()];

		int i = 0;
		while (buffy.hasRemaining()) {
			result[i++] = buffy.get();
		}

		return result;
	}
}
