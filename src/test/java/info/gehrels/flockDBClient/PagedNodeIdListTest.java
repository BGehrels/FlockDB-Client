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
import com.twitter.flockdb.thrift.Results;
import com.twitter.flockdb.thrift.SelectQuery;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class PagedNodeIdListTest {
	private Iface backingFlockClient = mock(Iface.class);
	private SelectQuery selectQuery = mock(SelectQuery.class);

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
	public void hasNoPreviousPageForMinusOneAsPreviousCursor() {
		Results results = new Results(ByteHelper.asByteBuffer(), 0, -1);
		PagedNodeIdList list = new PagedNodeIdList(backingFlockClient, selectQuery, results);

		assertThat(list.hasPreviousPage(), is(false));
	}


	@Test
	public void returnsCorrectIteratorForNonEmptyResults() {
		Results results = new Results(ByteHelper.asByteBuffer(4,9,2), 0, -1);
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


}
