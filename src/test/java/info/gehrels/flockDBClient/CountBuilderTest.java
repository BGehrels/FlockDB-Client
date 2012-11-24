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
 * limitations under the License.
 */

package info.gehrels.flockDBClient;

import com.twitter.flockdb.thrift.FlockDB.Iface;
import com.twitter.flockdb.thrift.FlockException;
import com.twitter.flockdb.thrift.SelectOperation;
import org.apache.thrift.TException;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static info.gehrels.flockDBClient.Direction.OUTGOING;
import static info.gehrels.flockDBClient.SelectionQuery.simpleSelection;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class CountBuilderTest {
	@Test
	public void delegatesExecutionToFlockClientAndReturnsResults() throws FlockException, TException,
		IOException {
		ByteBuffer flockClientResultStub = ByteBuffer.allocate(2 * (Integer.SIZE / 8));
		flockClientResultStub.order(LITTLE_ENDIAN);
		flockClientResultStub.putInt(5);
		flockClientResultStub.putInt(78);
		flockClientResultStub.rewind();


		Iface flockClient = mock(Iface.class);
		ArgumentCaptor<List> queryListCaptor = ArgumentCaptor.forClass(List.class);
		doReturn(flockClientResultStub).when(flockClient).count2(any(List.class));

		SelectionQuery firstSelectionQuery = simpleSelection(1, 1, OUTGOING);
		SelectionQuery secondSelectionQuery = simpleSelection(1, 2, OUTGOING);
		List<Integer> results = new CountBuilder(flockClient)
			.count(firstSelectionQuery)
			.count(secondSelectionQuery)
			.execute();

		verify(flockClient).count2(queryListCaptor.capture());
		List<List<SelectOperation>> actualParameters = queryListCaptor.getValue();
		assertThat(actualParameters, contains(
			is(firstSelectionQuery.getSelectOperations()),
			is(secondSelectionQuery.getSelectOperations())
		));
		assertThat(results, contains(5, 78));
	}
}
