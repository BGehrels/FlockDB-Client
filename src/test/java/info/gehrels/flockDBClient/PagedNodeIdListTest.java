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
import com.twitter.flockdb.thrift.Page;
import com.twitter.flockdb.thrift.Results;
import com.twitter.flockdb.thrift.SelectQuery;
import org.apache.thrift.TException;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.List;

import static com.twitter.flockdb.thrift.SelectOperationType.SimpleQuery;
import static info.gehrels.flockDBClient.Direction.OUTGOING;
import static info.gehrels.flockDBClient.SelectMatchers.aSelectOperation;
import static info.gehrels.flockDBClient.SelectMatchers.aSelectQuery;
import static info.gehrels.flockDBClient.SelectMatchers.withOperations;
import static info.gehrels.flockDBClient.SelectMatchers.withType;
import static info.gehrels.flockDBClient.SelectionQuery.simpleSelection;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class PagedNodeIdListTest {
	private Iface backingFlockClient = mock(Iface.class);
	private SelectQuery selectQuery = new SelectQuery(simpleSelection(1, 2, OUTGOING).getSelectOperations(),
	                                                  new Page(10, 12));

	private ArgumentCaptor<List> captor = (ArgumentCaptor<List>) ArgumentCaptor.forClass(List.class);

	@Test
	public void returnsEmptyIteratorForEmptyResults() {
		Results results = new Results(ByteHelper.asByteBuffer(), 0, -1);
		PagedNodeIdList list = new PagedNodeIdList(backingFlockClient, selectQuery, results);

		assertThat(list, emptyIterable());
	}

	@Test
	public void hasNoNextPageForZeroAsNextCursor() {
		Results results = new Results(ByteHelper.asByteBuffer(), 0, -1);
		PagedNodeIdList list = new PagedNodeIdList(backingFlockClient, selectQuery, results);

		assertThat(list.hasNextPage(), is(false));
	}

	@Test
	public void hasNoPreviousPageForMinusOneAsCurrentCursor() {
		Results results = new Results(ByteHelper.asByteBuffer(), 0, -2);
		selectQuery.setPage(new Page(15, -1));
		PagedNodeIdList list = new PagedNodeIdList(backingFlockClient, selectQuery, results);

		assertThat(list.hasPreviousPage(), is(false));
	}


	@Test
	public void returnsCorrectIteratorForNonEmptyResults() {
		Results results = new Results(ByteHelper.asByteBuffer(4, 9, 2), 0, -1);
		PagedNodeIdList list = new PagedNodeIdList(backingFlockClient, selectQuery, results);

		assertThat(list, contains(is(4L), is(9L), is(2L)));
	}

	@Test
	public void hasNextPageForNonZeroAsNextCursor() {
		Results results = new Results(ByteHelper.asByteBuffer(), 2, -1);
		PagedNodeIdList list = new PagedNodeIdList(backingFlockClient, selectQuery, results);

		assertThat(list.hasNextPage(), is(true));
	}

	@Test
	public void hasPreviousForNonMinusOneAsPreviousCursor() {
		Results results = new Results(ByteHelper.asByteBuffer(), 0, 5);
		PagedNodeIdList list = new PagedNodeIdList(backingFlockClient, selectQuery, results);

		assertThat(list.hasPreviousPage(), is(true));
	}

	@Test
	public void executesCorrectQueryForNextPage() throws IOException, FlockException, TException {
		Results stubResults = new Results(ByteHelper.asByteBuffer(1, 2), 11, -1);
		doReturn(singletonList(stubResults)).when(backingFlockClient).select2(any(List.class));
		PagedNodeIdList list = new PagedNodeIdList(backingFlockClient, selectQuery, stubResults);

		list.getNextPage();

		verify(backingFlockClient).select2(captor.capture());
		List<SelectQuery> queries = captor.getValue();
		assertThat(queries,
		           contains(
			           aSelectQuery(
				           withOperations(
					           contains(
						           aSelectOperation(
							           withType(SimpleQuery),
							           SelectMatchers.withSourceId(1),
							           SelectMatchers.withGraphId(2),
							           SelectMatchers.withForward(true)
						           )
					           )
				           ),

				           SelectMatchers.withMaxResults(10),
				           SelectMatchers.withCursor(11)
			           )
		           ));
	}


	@Test
	public void executesCorrectQueryForPreviousPage() throws IOException, FlockException, TException {
		Results stubResults = new Results(ByteHelper.asByteBuffer(1, 2), 0, 13);
		doReturn(singletonList(stubResults)).when(backingFlockClient).select2(any(List.class));
		PagedNodeIdList list = new PagedNodeIdList(backingFlockClient, selectQuery, stubResults);

		list.getPreviousPage();

		verify(backingFlockClient).select2(captor.capture());
		List<SelectQuery> queries = captor.getValue();
		assertThat(queries,
		           contains(
			           aSelectQuery(
				           withOperations(
					           contains(
						           aSelectOperation(
							           withType(SimpleQuery),
							           SelectMatchers.withSourceId(1),
							           SelectMatchers.withGraphId(2),
							           SelectMatchers.withForward(true)
						           )
					           )
				           ),

				           SelectMatchers.withMaxResults(10),
				           SelectMatchers.withCursor(13)
			           )
		           ));
	}


}
