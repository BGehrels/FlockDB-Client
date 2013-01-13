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

import com.twitter.flockdb.thrift.Edge;
import com.twitter.flockdb.thrift.FlockDB.Iface;
import com.twitter.flockdb.thrift.FlockException;
import com.twitter.flockdb.thrift.Metadata;
import com.twitter.flockdb.thrift.Priority;
import com.twitter.flockdb.thrift.SelectOperation;
import org.apache.thrift.TException;
import org.hamcrest.CoreMatchers;
import org.junit.Test;

import java.io.IOException;

import static info.gehrels.flockDBClient.Direction.OUTGOING;
import static info.gehrels.flockDBClient.EdgeSelectMatchers.anEdgeQuery;
import static info.gehrels.flockDBClient.EdgeSelectMatchers.withDestinationIds;
import static info.gehrels.flockDBClient.EdgeSelectMatchers.withForward;
import static info.gehrels.flockDBClient.EdgeSelectMatchers.withGraphId;
import static info.gehrels.flockDBClient.EdgeSelectMatchers.withSourceId;
import static info.gehrels.flockDBClient.SelectMatchers.aSelectQuery;
import static info.gehrels.flockDBClient.SelectMatchers.withMaxResults;
import static info.gehrels.flockDBClient.SelectMatchers.withOperations;
import static info.gehrels.flockDBClient.SelectMatchers.withCursor;
import static info.gehrels.flockDBClient.SelectionQuery.simpleSelection;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FlockDBTest {
	public static final Edge ARBITRARY_EDGE = new Edge(12, 56, 123, 123, 123, 1);
	private static final Metadata ARBITRARY_METADATA = new Metadata(123, 123, 123, 123);

	private Iface backingFlockMock = mock(Iface.class);

	@Test
	public void delegatesContainsCallsToBackingClient() throws IOException, FlockException, TException {
		when(backingFlockMock.contains(12, 34, 56)).thenReturn(true);

		boolean result = new FlockDB(backingFlockMock).contains(12, 34, 56);

		verify(backingFlockMock).contains(12, 34, 56);
		assertThat(result, is(true));
	}

	@Test
	public void delegatesGetCallsToBackingClient() throws IOException, FlockException, TException {
		when(backingFlockMock.get(12, 34, 56)).thenReturn(ARBITRARY_EDGE);

		Edge edge = new FlockDB(backingFlockMock).get(12, 34, 56);

		verify(backingFlockMock).get(12, 34, 56);
		assertThat(edge, is(sameInstance(ARBITRARY_EDGE)));
	}

	@Test
	public void delegatesContainsMetadataCallsToBackingClient() throws IOException, FlockException, TException {
		when(backingFlockMock.contains_metadata(12, 34)).thenReturn(true);

		boolean result = new FlockDB(backingFlockMock).containsMetadata(12, 34);

		verify(backingFlockMock).contains_metadata(12, 34);
		assertThat(result, is(true));
	}

	@Test
	public void delegatesGetMetadataCallsToBackingClient() throws IOException, FlockException, TException {
		when(backingFlockMock.get_metadata(12, 34)).thenReturn(ARBITRARY_METADATA);

		Metadata result = new FlockDB(backingFlockMock).getMetadata(12, 34);

		verify(backingFlockMock).get_metadata(12, 34);
		assertThat(result, is(sameInstance(ARBITRARY_METADATA)));
	}

	@Test
	public void returnsBuilderWithReferenceToBackingClientAndSelectionQueryOnSelect() throws IOException,
		FlockException {
		SelectionQuery firstQuery = simpleSelection(1, 2, OUTGOING);
		SelectionBuilder builder = new FlockDB(backingFlockMock).select(firstQuery);

		assertThat(builder.getBackingFlockClient(), is(sameInstance(backingFlockMock)));
		assertThat(builder.getQueries(),
		           contains(
			           aSelectQuery(
				           withOperations(
					           CoreMatchers.<Iterable<? extends SelectOperation>>is(firstQuery.getSelectOperations())
				           ),
				           withCursor(-1)
			           )
		           )
		);
	}

	@Test
	public void returnsBuilderWithReferenceToBackingClientAndSelectionQueryOnSelectWithPaging() throws IOException,
		FlockException {
		SelectionQuery firstQuery = simpleSelection(1, 2, OUTGOING);
		SelectionBuilder builder = new FlockDB(backingFlockMock).select(firstQuery).withPageSize(10);

		assertThat(builder.getBackingFlockClient(), is(sameInstance(backingFlockMock)));
		assertThat(builder.getQueries(),
		           contains(
			           aSelectQuery(
				           withOperations(
					           CoreMatchers.<Iterable<? extends SelectOperation>>is(firstQuery.getSelectOperations())
				           ),
			               withCursor(-1),
			               withMaxResults(10)
			           )
		           )
		);
	}

	@Test
	public void returnsBuilderWithReferenceToBackingClientAndSavedQueryOnCount() throws IOException, FlockException {
		SelectionQuery selectionQuery = simpleSelection(1, 2, OUTGOING);
		CountBuilder builder = new FlockDB(backingFlockMock).count(selectionQuery);

		assertThat(builder.getBackingFlockClient(), is(sameInstance(backingFlockMock)));
		assertThat(builder.getQueries(),
		           contains(
			           CoreMatchers.<Iterable<SelectOperation>>is(selectionQuery.getSelectOperations())
		           )
		);
	}

	@Test
	public void returnsBuilderWithReferenceToBackingClientAndSavedQueryOnSelectEdge() throws IOException, FlockException {
		EdgeSelectionBuilder builder = new FlockDB(backingFlockMock).selectEdges(1,2, OUTGOING, 4, 3);

		assertThat(builder.getBackingFlockClient(), is(sameInstance(backingFlockMock)));
		assertThat(builder.getQueries(),
		           contains(
			           anEdgeQuery(
				           withSourceId(1),
			               withGraphId(2),
			               withForward(true),
			               withDestinationIds(4L, 3L),
			               EdgeSelectMatchers.withCursor(-1)
			           )
		           )
		);
	}

	@Test
	public void returnsBuilderWithReferenceToBackingClientAndSavedQueryOnSelectEdgeWithPaging() throws IOException, FlockException {
		EdgeSelectionBuilder builder = new FlockDB(backingFlockMock).selectEdges(1,2,20, OUTGOING, 4, 3, 2, 1);

		assertThat(builder.getBackingFlockClient(), is(sameInstance(backingFlockMock)));
		assertThat(builder.getQueries(),
		           contains(
			           anEdgeQuery(
				           withSourceId(1),
			               withGraphId(2),
			               withForward(true),
			               withDestinationIds(4L, 3L, 2L, 1L),
			               EdgeSelectMatchers.withCursor(-1),
			               EdgeSelectMatchers.withMaxResults(20)
			           )
		           )
		);
	}

	@Test
	public void returnsBuilderWithReferenceToBackingClientAndSavedPriorityOnExecute() throws IOException, FlockException, TException {
		ExecutionBuilder builder = new FlockDB(backingFlockMock).batchExecution(Priority.High);

		assertThat(builder.getBackingFlockClient(), is(sameInstance(backingFlockMock)));
		assertThat(builder.getPriority(), is(Priority.High));
	}
}
