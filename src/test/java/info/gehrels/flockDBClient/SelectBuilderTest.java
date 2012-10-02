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
import com.twitter.flockdb.thrift.Results;
import com.twitter.flockdb.thrift.SelectOperation;
import com.twitter.flockdb.thrift.SelectQuery;
import org.apache.thrift.TException;
import org.hamcrest.Matcher;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static info.gehrels.flockDBClient.SelectMatchers.aSelectQuery;
import static info.gehrels.flockDBClient.SelectMatchers.withMaxResults;
import static info.gehrels.flockDBClient.SelectMatchers.withOperations;
import static info.gehrels.flockDBClient.SelectMatchers.withStartIndex;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class SelectBuilderTest {
	@Test
	public void delegatesExecutionToFlockClientAndReturnsResultsWithPaging() throws FlockException, TException, IOException {
		Iface flockClient = mock(Iface.class);
		ArgumentCaptor<List> queryListCaptor = ArgumentCaptor.forClass(List.class);
		List<Results> resultStub = new ArrayList<>();
		doReturn(resultStub).when(flockClient).select2(any(List.class));

		SelectionQuery firstSelectionQuery = SelectionQuery.simpleSelection(1, 1, true);
		SelectionQuery secondSelectionQuery = SelectionQuery.simpleSelection(1, 2, true);
		List<Results> results = new SelectBuilder(flockClient)
			.select(firstSelectionQuery, -1, 5)
			.select(secondSelectionQuery, 3, 10)
			.execute();

		verify(flockClient).select2(queryListCaptor.capture());
		List<SelectQuery> actualParameters = queryListCaptor.getValue();
		assertThat(actualParameters,
		           contains(
			           aSelectQuery(
				           withOperations(is(firstSelectionQuery.getSelectOperations())),
				           withStartIndex(-1),
				           withMaxResults(5)
			           ),
			           aSelectQuery(
				           withOperations(is(secondSelectionQuery.getSelectOperations())),
				           withStartIndex(3),
				           withMaxResults(10)
			           )
		           )
		);
		assertThat(results, sameInstance(resultStub));
	}

	@Test
	public void delegatesExecutionToFlockClientAndReturnsResultsWithoutPaging() throws FlockException, TException,
		IOException {
		Iface flockClient = mock(Iface.class);
		ArgumentCaptor<List> queryListCaptor = ArgumentCaptor.forClass(List.class);
		List<Results> resultStub = new ArrayList<>();
		doReturn(resultStub).when(flockClient).select2(any(List.class));

		SelectionQuery firstSelectionQuery = SelectionQuery.simpleSelection(1, 1, true);
		SelectionQuery secondSelectionQuery = SelectionQuery.simpleSelection(1, 2, true);
		List<Results> results = new SelectBuilder(flockClient)
			.select(firstSelectionQuery)
			.select(secondSelectionQuery)
			.execute();

		verify(flockClient).select2(queryListCaptor.capture());
		List<SelectQuery> actualParameters = queryListCaptor.getValue();
		assertThat(actualParameters,
		           contains(
			           aSelectQuery(
				           withOperations(
					           is(firstSelectionQuery.getSelectOperations()))
			           ),
			           aSelectQuery(
				           withOperations(
					           is(secondSelectionQuery.getSelectOperations()))
			           )
		           )
		);
		assertThat(results, sameInstance(resultStub));
	}

	private Matcher<Iterable<? extends SelectOperation>> is(List<SelectOperation> selectOperations) {
		return org.hamcrest.Matchers
			.<Iterable<? extends SelectOperation>>is(selectOperations);
	}


}
