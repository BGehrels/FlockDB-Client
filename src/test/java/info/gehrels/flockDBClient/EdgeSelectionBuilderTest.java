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
import com.twitter.flockdb.thrift.EdgeQuery;
import com.twitter.flockdb.thrift.EdgeResults;
import com.twitter.flockdb.thrift.FlockDB.Iface;
import com.twitter.flockdb.thrift.FlockException;
import org.apache.thrift.TException;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static info.gehrels.flockDBClient.Direction.INCOMING;
import static info.gehrels.flockDBClient.Direction.OUTGOING;
import static info.gehrels.flockDBClient.EdgeSelectMatchers.anEdgeQuery;
import static info.gehrels.flockDBClient.EdgeSelectMatchers.withCursor;
import static info.gehrels.flockDBClient.EdgeSelectMatchers.withDestinationIds;
import static info.gehrels.flockDBClient.EdgeSelectMatchers.withForward;
import static info.gehrels.flockDBClient.EdgeSelectMatchers.withGraphId;
import static info.gehrels.flockDBClient.EdgeSelectMatchers.withMaxResults;
import static info.gehrels.flockDBClient.EdgeSelectMatchers.withSourceId;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class EdgeSelectionBuilderTest {
	private final Iface backingFlockMock = mock(Iface.class);
	private final ArgumentCaptor<List> captor = (ArgumentCaptor<List>) ArgumentCaptor.forClass(List.class);
	private final EdgeSelectionBuilder edgeSelectionBuilder = new EdgeSelectionBuilder(backingFlockMock);
	private Edge stubEdge = mock(Edge.class);

	@Before
	public void createStubResults() throws TException, FlockException {
		List<EdgeResults> stubResults = new ArrayList<>();
		stubResults.add(new EdgeResults(asList(stubEdge), 0, -1));

		doReturn(stubResults).when(backingFlockMock).select_edges(any(List.class));
	}

	@Test
	public void executesCorrectQueryAndReturnsResultsWithoutPaging() throws IOException,
		FlockException, TException {
		List<PagedEdgeList> results = edgeSelectionBuilder
			.selectEdges(1, 2, OUTGOING, 4, 3)
			.selectEdges(5, 6, INCOMING)
			.execute();

		verify(backingFlockMock).select_edges(captor.capture());

		assertThat((List<EdgeQuery>) captor.getValue(),
		           contains(
			           anEdgeQuery(
				           withSourceId(1),
				           withGraphId(2),
				           withForward(true),
				           withDestinationIds(4L, 3L),
				           withCursor(-1)
			           ),
			           anEdgeQuery(
				           withSourceId(5),
				           withGraphId(6),
				           withForward(false),
				           EdgeSelectMatchers.withoutDestinationIds(),
				           withCursor(-1)
			           )
		           )
		);

		verifyCorrectResult(results);
	}

	@Test
	public void executesCorrectQueryAndReturnsResultsOnSelectEdgeWithPageSize() throws IOException, FlockException,
		TException {
		List<PagedEdgeList> results = edgeSelectionBuilder
			.selectEdges(1, 2, OUTGOING).withPageSize(20)
			.selectEdges(3, 4, INCOMING).withPageSize(30)
			.execute();

		verify(backingFlockMock).select_edges(captor.capture());


		assertThat((List<EdgeQuery>) captor.getValue(),
		           contains(
			           anEdgeQuery(
				           withSourceId(1),
				           withCursor(-1),
				           withMaxResults(20)
			           ),
			           anEdgeQuery(
				           withSourceId(3),
				           withCursor(-1),
				           withMaxResults(30)
			           )
		           )
		);

		verifyCorrectResult(results);
	}

	@Test
	public void executesCorrectQueryAndReturnsResultsOnSelectEdgeWithPageStartNode() throws IOException, FlockException,
		TException {
		List<PagedEdgeList> results =
			edgeSelectionBuilder
				.selectEdges(1, 2, OUTGOING).withPageStartNode(20)
				.selectEdges(2, 3, OUTGOING).withPageStartNode(30)
				.execute();

		verify(backingFlockMock).select_edges(captor.capture());


		assertThat((List<EdgeQuery>) captor.getValue(),
		           contains(
			           anEdgeQuery(
				           withSourceId(1),
				           withCursor(20),
				           withMaxResults(Integer.MAX_VALUE - 1)
			           ),
			           anEdgeQuery(
				           withSourceId(2),
				           withCursor(30),
				           withMaxResults(Integer.MAX_VALUE - 1)
			           )
		           )
		);

		verifyCorrectResult(results);
	}

	@Test
	public void executesCorrectQueryAndReturnsResultsOnSelectEdgeWithBothPagingOptions() throws IOException, FlockException,
		TException {
		List<PagedEdgeList> results =
			edgeSelectionBuilder
				.selectEdges(1, 2, OUTGOING).withPageStartNode(20).withPageSize(100)
				.selectEdges(2, 3, OUTGOING).withPageSize(200).withPageStartNode(30)
				.execute();

		verify(backingFlockMock).select_edges(captor.capture());


		assertThat((List<EdgeQuery>) captor.getValue(),
		           contains(
			           anEdgeQuery(
				           withSourceId(1),
				           withCursor(20),
				           withMaxResults(100)
			           ),
			           anEdgeQuery(
				           withSourceId(2),
				           withCursor(30),
				           withMaxResults(200)
			           )
		           )
		);

		verifyCorrectResult(results);
	}

	private void verifyCorrectResult(List<PagedEdgeList> results) {
		assertThat(results,
		           contains(
			           pagedEdgeListWithElements(contains(sameInstance(stubEdge)))));
	}

	private Matcher<PagedEdgeList> pagedEdgeListWithElements(
		Matcher<? super Iterable<Edge>> subMatcher) {
		return new FeatureMatcher<PagedEdgeList, Iterable<Edge>>(subMatcher, "a paged edge list",
		                                                         "a paged edge list") {

			@Override
			protected Iterable<Edge> featureValueOf(PagedEdgeList actual) {
				return actual;
			}
		};
	}

}
