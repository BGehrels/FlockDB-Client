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
import info.gehrels.flockDBClient.FlockAndThriftExceptionHandling.MethodObject;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.List;

import static info.gehrels.flockDBClient.FlockAndThriftExceptionHandling.handleFlockAndThriftExceptions;

public class SelectionBuilder {
	private final Iface backingFlockClient;
	private final List<SelectQuery> queries = new ArrayList<>();

	SelectionBuilder(Iface backingFlockClient) {
		this.backingFlockClient = backingFlockClient;
	}

	public SelectionBuilder select(SelectionQuery firstQuery) {
		this.queries.add(new SelectQuery(firstQuery.getSelectOperations(), new Page(Integer.MAX_VALUE - 1, -1)));

		return this;
	}

	public SelectionBuilder withPageSize(int maxResults) {
		SelectQuery lastAddedQuery = getLastAddedQuery();
		lastAddedQuery.setPage(new Page(maxResults, lastAddedQuery.getPage().getCursor()));

		return this;
	}

	public SelectionBuilder withPageStartNode(long nodeId) {
		SelectQuery lastAddedQuery = getLastAddedQuery();
		lastAddedQuery.setPage(new Page(lastAddedQuery.getPage().getCount(), nodeId));

		return this;
	}

	public List<PagedNodeIdList> execute() {
		return handleFlockAndThriftExceptions(new MethodObject<List<PagedNodeIdList>>() {
			@Override
			public List<PagedNodeIdList> call() throws TException, FlockException {
				return tryToExecute();
			}
		});
	}

	private List<PagedNodeIdList> tryToExecute() throws FlockException, TException {
		List<PagedNodeIdList> result = new ArrayList<>();
		List<Results> rawResults = backingFlockClient.select2(queries);

		for (int i = 0; i < rawResults.size(); i++) {
			result.add(new PagedNodeIdList(backingFlockClient, queries.get(i), rawResults.get(i)));
		}

		return result;
	}

	Iface getBackingFlockClient() {
		return backingFlockClient;
	}

	List<SelectQuery> getQueries() {
		return queries;
	}

	private SelectQuery getLastAddedQuery() {
		return queries.get(queries.size() - 1);
	}
}
