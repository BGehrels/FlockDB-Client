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

import com.twitter.flockdb.thrift.EdgeQuery;
import com.twitter.flockdb.thrift.EdgeResults;
import com.twitter.flockdb.thrift.FlockDB.Iface;
import com.twitter.flockdb.thrift.FlockException;
import com.twitter.flockdb.thrift.Page;
import com.twitter.flockdb.thrift.QueryTerm;
import info.gehrels.flockDBClient.FlockAndThriftExceptionHandling.MethodObject;
import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static info.gehrels.flockDBClient.ByteHelper.asByteBufferOrNull;
import static info.gehrels.flockDBClient.FlockAndThriftExceptionHandling.handleFlockAndThriftExceptions;

public class EdgeSelectionBuilder {
	private final Iface backingFlockClient;
	private List<EdgeQuery> queries = new ArrayList<>();

	EdgeSelectionBuilder(Iface backingFlockClient) {
		this.backingFlockClient = backingFlockClient;
	}

	public EdgeSelectionBuilder selectEdges(long sourceId, int graphId, Direction direction, long... destinationIds) {
		ByteBuffer buffy = asByteBufferOrNull(destinationIds);
		QueryTerm term = new QueryTerm(sourceId, graphId, direction.forward).setDestination_ids(buffy);
		this.queries.add(new EdgeQuery(term, new Page(Integer.MAX_VALUE - 1, -1)));
		return this;
	}

	public EdgeSelectionBuilder withPageSize(int maxResults) {
		EdgeQuery lastAddedQuery = getLastAddedQuery();
		lastAddedQuery.setPage(new Page(maxResults, lastAddedQuery.getPage().getCursor()));

		return this;
	}

	public EdgeSelectionBuilder withPageStartNode(long nodeId) {
		EdgeQuery lastAddedQuery = getLastAddedQuery();
		lastAddedQuery.setPage(new Page(lastAddedQuery.getPage().getCount(), nodeId));

		return this;
	}

	public List<PagedEdgeList> execute() throws IOException {
		List<PagedEdgeList> result = new ArrayList<>();

		List<EdgeResults> rawResults = handleFlockAndThriftExceptions(new MethodObject<List<EdgeResults>>() {
			@Override
			public List<EdgeResults> call() throws TException, FlockException {
				return backingFlockClient.select_edges(queries);
			}
		});

		for (int i = 0; i < rawResults.size(); i++) {
			result.add(new PagedEdgeList(backingFlockClient, this.queries.get(i), rawResults.get(i)));
		}
		return result;
	}

	private EdgeQuery getLastAddedQuery() {
		return queries.get(queries.size() - 1);
	}

	Iface getBackingFlockClient() {
		return backingFlockClient;
	}

	List<EdgeQuery> getQueries() {
		return queries;
	}
}
