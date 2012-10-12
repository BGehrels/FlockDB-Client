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
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class PagedEdgeList implements Iterable<Edge> {
	private final Iface backingFlockClient;
	private final EdgeQuery edgeQuery;
	private final EdgeResults results;

	public PagedEdgeList(Iface backingFlockClient, EdgeQuery edgeQuery, EdgeResults results) {
		this.backingFlockClient = backingFlockClient;
		this.edgeQuery = edgeQuery;
		this.results = results;
	}

	public PagedEdgeList getNextPage() throws FlockException, IOException{
		return getOtherPage(results.next_cursor);
	}


	public PagedEdgeList getPreviousPage() throws FlockException, IOException {
		return getOtherPage(results.prev_cursor);

	}

	public boolean hasNextPage() {
		return results.next_cursor != 0;
	}

	public boolean hasPreviousPage() {
		return results.prev_cursor != -1;
	}

	private PagedEdgeList getOtherPage(long otherPagesCursor) throws FlockException, IOException {
		EdgeQuery nextPageQuery = edgeQuery.setPage(edgeQuery.getPage().setCursor(otherPagesCursor));
		List<EdgeResults> results;
		try {
			results = backingFlockClient.select_edges(Collections.singletonList(nextPageQuery));
		} catch (TException e) {
			throw new IOException(e);
		}
		return new PagedEdgeList(backingFlockClient, nextPageQuery, results.get(0));
	}


	@Override
	public Iterator<Edge> iterator() {
		return this.results.getEdges().iterator();
	}
}
