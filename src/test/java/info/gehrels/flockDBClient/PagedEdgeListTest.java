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

import com.twitter.flockdb.thrift.Edge;
import com.twitter.flockdb.thrift.EdgeQuery;
import com.twitter.flockdb.thrift.EdgeResults;
import com.twitter.flockdb.thrift.FlockDB.Iface;
import com.twitter.flockdb.thrift.FlockException;
import com.twitter.flockdb.thrift.Page;
import com.twitter.flockdb.thrift.QueryTerm;
import org.apache.thrift.TException;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static info.gehrels.flockDBClient.EdgeSelectMatchers.anEdgeQuery;
import static info.gehrels.flockDBClient.EdgeSelectMatchers.withForward;
import static info.gehrels.flockDBClient.EdgeSelectMatchers.withGraphId;
import static info.gehrels.flockDBClient.EdgeSelectMatchers.withMaxResults;
import static info.gehrels.flockDBClient.EdgeSelectMatchers.withSourceId;
import static info.gehrels.flockDBClient.EdgeSelectMatchers.withCursor;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class PagedEdgeListTest {
	private Iface backingFlockClient = mock(Iface.class);
	private final QueryTerm term = new QueryTerm(1, 2, true);
	private EdgeQuery edgeQuery = new EdgeQuery(term, new Page(15, 10));

	private ArgumentCaptor<List> captor = (ArgumentCaptor<List>) ArgumentCaptor.forClass(List.class);

	@Test
	public void returnsEmptyIteratorForEmptyResults() {
		EdgeResults results = new EdgeResults(new ArrayList<Edge>(), 0, -1);
		PagedEdgeList list = new PagedEdgeList(backingFlockClient, edgeQuery, results);

		assertThat(list, emptyIterable());
	}

	@Test
	public void hasNoNextPageForZeroAsNextCursor() {
		EdgeResults results = new EdgeResults(new ArrayList<Edge>(), 0, -1);
		PagedEdgeList list = new PagedEdgeList(backingFlockClient, edgeQuery, results);

		assertThat(list.hasNextPage(), is(false));
	}

	@Test
	public void hasNoPreviousPageForMinusOneAsCurrentCursor() {
		EdgeResults results = new EdgeResults(new ArrayList<Edge>(), 0, -2);
		EdgeQuery edgeQuery = new EdgeQuery(term, new Page(15, 0));

		PagedEdgeList list = new PagedEdgeList(backingFlockClient, edgeQuery, results);

		assertThat(list.hasPreviousPage(), is(false));
	}


	@Test
	public void returnsCorrectIteratorForNonEmptyResults() {
		Edge edge1 = new Edge(1, 2, 3, 4, 5, 6);
		Edge edge2 = new Edge(6, 5, 4, 3, 2, 1);
		EdgeResults results = new EdgeResults(new ArrayList<>(asList(edge1, edge2)), 0, -1);
		PagedEdgeList list = new PagedEdgeList(backingFlockClient, edgeQuery, results);

		assertThat(list, contains(sameInstance(edge1), sameInstance(edge2)));
	}

	@Test
	public void hasNextPageForNonZeroAsNextCursor() {
		EdgeResults results = new EdgeResults(new ArrayList<Edge>(), 2, -1);
		PagedEdgeList list = new PagedEdgeList(backingFlockClient, edgeQuery, results);

		assertThat(list.hasNextPage(), is(true));
	}

	@Test
	public void hasPreviousForNonMinusOneAsPreviousCursor() {
		EdgeResults results = new EdgeResults(new ArrayList<Edge>(), 0, 5);
		PagedEdgeList list = new PagedEdgeList(backingFlockClient, edgeQuery, results);

		assertThat(list.hasPreviousPage(), is(true));
	}

	@Test
	public void executesCorrectQueryForNextPage() throws IOException, FlockException, TException {
		EdgeResults stubResults = new EdgeResults(new ArrayList<Edge>(), 10, -1);
		doReturn(singletonList(stubResults)).when(backingFlockClient).select_edges(any(List.class));
		PagedEdgeList list = new PagedEdgeList(backingFlockClient, edgeQuery, stubResults);

		list.getNextPage();

		verify(backingFlockClient).select_edges(captor.capture());
		List<EdgeQuery> queries = captor.getValue();
		assertThat(queries,
		           contains(
			           anEdgeQuery(
				           withSourceId(1),
				           withGraphId(2),
				           withForward(true),
				           withCursor(10),
				           withMaxResults(15)
			           )
		           ));
	}


	@Test
	public void executesCorrectQueryForPreviousPage() throws IOException, FlockException, TException {
		EdgeResults stubResults = new EdgeResults(new ArrayList<Edge>(), 0, 12);
		doReturn(singletonList(stubResults)).when(backingFlockClient).select_edges(any(List.class));
		PagedEdgeList list = new PagedEdgeList(backingFlockClient, edgeQuery, stubResults);

		list.getPreviousPage();

		verify(backingFlockClient).select_edges(captor.capture());
		List<EdgeQuery> queries = captor.getValue();
		assertThat(queries,
		           contains(
			           anEdgeQuery(
				           withSourceId(1),
				           withGraphId(2),
				           withForward(true),
				           withCursor(12),
				           withMaxResults(15)
			           )
		           ));
	}


}
