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

import com.twitter.flockdb.thrift.FlockDB.Iface;
import com.twitter.flockdb.thrift.FlockException;
import com.twitter.flockdb.thrift.SelectOperation;
import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

final class CountBuilder {
    private final Iface backingFlockClient;
	private List<List<SelectOperation>> queries = new ArrayList<>();

	CountBuilder(Iface backingFlockClient) {
        this.backingFlockClient = backingFlockClient;
    }

	public CountBuilder count(SelectionQuery selectionQuery) {
		queries.add(selectionQuery.getSelectOperations());
		return this;
	}

	public List<Integer> execute() throws FlockException, IOException {
		try {
			return createIntegerListFromByteBuffer(backingFlockClient.count2(queries));
		} catch (TException e) {
			throw new IOException(e);
		}
	}

	List<List<SelectOperation>> getQueries() {
		return queries;
	}

    Iface getBackingFlockClient() {
        return backingFlockClient;
    }

	private List<Integer> createIntegerListFromByteBuffer(ByteBuffer byteBuffer) throws FlockException, TException {
		List<Integer> result = new ArrayList<>();
		byteBuffer.order(LITTLE_ENDIAN);
		while (byteBuffer.hasRemaining()) {
			result.add(byteBuffer.getInt());
		}
		byteBuffer.rewind();
		return result;
	}
}
