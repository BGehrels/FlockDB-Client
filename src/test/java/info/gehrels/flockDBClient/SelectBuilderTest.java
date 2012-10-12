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
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static info.gehrels.flockDBClient.SelectMatchers.aSelectQuery;
import static info.gehrels.flockDBClient.SelectMatchers.withMaxResults;
import static info.gehrels.flockDBClient.SelectMatchers.withOperations;
import static info.gehrels.flockDBClient.SelectMatchers.withCursor;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class SelectBuilderTest {

	private List<Results> resultStub;

	@Before
	public void createResultStub() {
		resultStub = new ArrayList<>();
		resultStub.add(new Results(ByteHelper.asByteBufferOrNull(123, 5), 0, -1));
		resultStub.add(new Results(ByteHelper.asByteBufferOrNull(4, 12), 0, -1));
	}

	@Test
	public void delegatesExecutionToFlockClientAndReturnsResultsWithPaging() throws FlockException, TException,
		IOException {
		Iface flockClient = mock(Iface.class);
		ArgumentCaptor<List> queryListCaptor = ArgumentCaptor.forClass(List.class);
		doReturn(resultStub).when(flockClient).select2(any(List.class));

		SelectionQuery firstSelectionQuery = SelectionQuery.simpleSelection(1, 1, true);
		SelectionQuery secondSelectionQuery = SelectionQuery.simpleSelection(1, 2, true);
		List<PagedNodeIdList> results = new SelectBuilder(flockClient)
			.select(firstSelectionQuery, 5)
			.select(secondSelectionQuery, 10)
			.execute();

		verify(flockClient).select2(queryListCaptor.capture());
		List<SelectQuery> actualParameters = queryListCaptor.getValue();
		assertThat(actualParameters,
		           contains(
			           aSelectQuery(
				           withOperations(isList(firstSelectionQuery.getSelectOperations())),
				           withCursor(-1),
				           withMaxResults(5)
			           ),
			           aSelectQuery(
				           withOperations(isList(secondSelectionQuery.getSelectOperations())),
				           withCursor(-1),
				           withMaxResults(10)
			           )
		           )
		);

		verifyResultStub(results);
	}

	@Test
	public void delegatesExecutionToFlockClientAndReturnsResultsWithoutPaging() throws FlockException, TException,
		IOException {
		Iface flockClient = mock(Iface.class);
		ArgumentCaptor<List> queryListCaptor = ArgumentCaptor.forClass(List.class);
		doReturn(resultStub).when(flockClient).select2(any(List.class));

		SelectionQuery firstSelectionQuery = SelectionQuery.simpleSelection(1, 1, true);
		SelectionQuery secondSelectionQuery = SelectionQuery.simpleSelection(1, 2, true);
		List<PagedNodeIdList> results = new SelectBuilder(flockClient)
			.select(firstSelectionQuery)
			.select(secondSelectionQuery)
			.execute();

		verify(flockClient).select2(queryListCaptor.capture());
		List<SelectQuery> actualParameters = queryListCaptor.getValue();
		assertThat(actualParameters,
		           contains(
			           aSelectQuery(
				           withOperations(
					           isList(firstSelectionQuery.getSelectOperations()))
			           ),
			           aSelectQuery(
				           withOperations(
					           isList(secondSelectionQuery.getSelectOperations()))
			           )
		           )
		);

		verifyResultStub(results);
	}

	private void verifyResultStub(List<PagedNodeIdList> results) {
		assertThat(results,
		           contains(
			           pagedNodeIdListWithElements(contains(is(123L), is(5L))),
			           pagedNodeIdListWithElements(contains(is(4L), is(12L)))));
	}

	private Matcher<PagedNodeIdList> pagedNodeIdListWithElements(
		Matcher<? super Iterable<Long>> subMatcher) {
		return new FeatureMatcher<PagedNodeIdList, Iterable<Long>>(subMatcher, "a paged node-id list",
		                                                           "a paged node-id list") {

			@Override
			protected Iterable<Long> featureValueOf(PagedNodeIdList actual) {
				return actual;
			}
		};
	}

	private Matcher<Iterable<? extends SelectOperation>> isList(List<SelectOperation> selectOperations) {
		return org.hamcrest.Matchers
			.<Iterable<? extends SelectOperation>>is(selectOperations);
	}


}
