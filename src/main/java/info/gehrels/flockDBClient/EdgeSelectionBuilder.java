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
import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static info.gehrels.flockDBClient.ByteHelper.asByteBufferOrNull;

public class EdgeSelectionBuilder {
	private final Iface backingFlockClient;
	private List<EdgeQuery> queries = new ArrayList<>();

	EdgeSelectionBuilder(Iface backingFlockClient) {
		this.backingFlockClient = backingFlockClient;
	}

	public EdgeSelectionBuilder selectEdges(long sourceId, int graphId, boolean forward, long... destinationIds) {
		return selectEdges(sourceId, graphId, Integer.MAX_VALUE, forward, destinationIds);
	}

	public EdgeSelectionBuilder selectEdges(long sourceId, int graphId, int maxResults, boolean forward, long... destinationIds) {
		ByteBuffer buffy = asByteBufferOrNull(destinationIds);
		QueryTerm term = new QueryTerm(sourceId, graphId, forward).setDestination_ids(buffy);
		this.queries.add(new EdgeQuery(term, new Page(maxResults, -1)));
		return this;
	}

	Iface getBackingFlockClient() {
		return backingFlockClient;
	}

	List<EdgeQuery> getQueries() {
		return queries;
	}

	public List<EdgeResults> execute() throws IOException, FlockException {
		try {
			return backingFlockClient.select_edges(queries);
		} catch (TException e) {
			throw new IOException(e);
		}
	}
}
