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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;

public class SelectBuilder {
	private final Iface backingFlockClient;
	private List<SelectQuery> queries;

	SelectBuilder(Iface backingFlockClient, SelectQuery... queries) {
		this.backingFlockClient = backingFlockClient;
		this.queries = new ArrayList<>(asList(queries));
	}

	public SelectBuilder select(SelectionQuery firstQuery) {
		this.queries.add(new SelectQuery(firstQuery.getSelectOperations(), new Page(Integer.MAX_VALUE, -1)));
		return this;
	}

	public SelectBuilder select(SelectionQuery firstQuery, int maxResults) {
		this.queries.add(new SelectQuery(firstQuery.getSelectOperations(), new Page(maxResults, -1)));
		return this;
	}

	public List<Results> execute() throws IOException, FlockException {
		try {
			return backingFlockClient.select2(this.queries);
		} catch (TException e) {
			throw new IOException(e);
		}
	}

	Iface getBackingFlockClient() {
		return backingFlockClient;
	}

	List<SelectQuery> getQueries() {
		return queries;
	}
}
