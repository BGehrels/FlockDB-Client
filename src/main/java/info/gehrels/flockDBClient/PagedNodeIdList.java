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
import com.twitter.flockdb.thrift.Results;
import com.twitter.flockdb.thrift.SelectQuery;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class PagedNodeIdList implements Iterable<Long> {
	private final Iface backingFlockClient;
	private final SelectQuery selectQuery;
	private final Results results;
	private long[] ids;

	PagedNodeIdList(Iface backingFlockClient, SelectQuery selectQuery, Results results) {
		this.backingFlockClient = backingFlockClient;
		this.selectQuery = selectQuery;
		this.results = results;
		this.ids = ByteHelper.toLongArray(results.getIds());
	}

	public PagedNodeIdList getNextPage() throws FlockException, IOException{
		return getOtherPage(results.next_cursor);
	}


	public PagedNodeIdList getPreviousPage() throws FlockException, IOException {
		return getOtherPage(results.prev_cursor);

	}

	public boolean hasNextPage() {
		return results.next_cursor != 0;
	}

	public boolean hasPreviousPage() {
		return this.selectQuery.getPage().getCursor() != -1;
	}

	private PagedNodeIdList getOtherPage(long otherPagesCursor) throws FlockException, IOException {
		SelectQuery nextPageQuery = selectQuery.setPage(selectQuery.getPage().setCursor(otherPagesCursor));
		List<Results> results;
		try {
			results = backingFlockClient.select2(Collections.singletonList(nextPageQuery));
		} catch (TException e) {
			throw new IOException(e);
		}
		return new PagedNodeIdList(backingFlockClient, nextPageQuery, results.get(0));
	}


	@Override
	public Iterator<Long> iterator() {
		return new Iterator<Long>() {
			int index = 0;
			@Override
			public boolean hasNext() {
				return ids.length >= index+1;
			}

			@Override
			public Long next() {
				return ids[index++];
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}
}
