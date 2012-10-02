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

import org.junit.Test;

import static com.twitter.flockdb.thrift.SelectOperationType.Difference;
import static com.twitter.flockdb.thrift.SelectOperationType.Intersection;
import static com.twitter.flockdb.thrift.SelectOperationType.SimpleQuery;
import static com.twitter.flockdb.thrift.SelectOperationType.Union;
import static info.gehrels.flockDBClient.SelectMatchers.aSelectOperation;
import static info.gehrels.flockDBClient.SelectMatchers.withForward;
import static info.gehrels.flockDBClient.SelectMatchers.withGraphId;
import static info.gehrels.flockDBClient.SelectMatchers.withSourceId;
import static info.gehrels.flockDBClient.SelectMatchers.withType;
import static info.gehrels.flockDBClient.SelectionQuery.difference;
import static info.gehrels.flockDBClient.SelectionQuery.intersect;
import static info.gehrels.flockDBClient.SelectionQuery.simpleSelection;
import static info.gehrels.flockDBClient.SelectionQuery.union;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;

public class SelectionQueryTest {
	@Test
	public void constructsCorrectSimpleSelectionWithoutDestinationIds() {
		SelectionQuery selectionQuery = simpleSelection(1, 2, true);

		assertThat(selectionQuery.getSelectOperations(),
		           contains(
			           aSelectOperation(
				           withType(SimpleQuery),
				           withSourceId(1),
				           withGraphId(2),
				           withForward(true)
			           )
		           )
		);
	}

	@Test
	public void constructsCorrectSimpleSelectionWithDestinationIds() {
		SelectionQuery selectionQuery = simpleSelection(1, 2, true, 2, 3, 4, 5);

		assertThat(selectionQuery.getSelectOperations(),
		           contains(
			           aSelectOperation(
				           withType(SimpleQuery),
				           withSourceId(1),
				           withGraphId(2),
				           withForward(true),
			               SelectMatchers.withDestinationIds(2L, 3L, 4L, 5L)
			           )
		           )
		);
	}

	@Test
	public void constructsCorrectIntersection() {
		SelectionQuery selectionQuery1 = simpleSelection(1, 2, true);
		SelectionQuery selectionQuery2 = simpleSelection(1, 3, true);
		SelectionQuery selectionQuery = intersect(selectionQuery1, selectionQuery2);

		assertThat(selectionQuery.getSelectOperations(),
		           contains(
			           is(selectionQuery1.getSelectOperations().get(0)),
			           is(selectionQuery2.getSelectOperations().get(0)),
			           aSelectOperation(
				           withType(Intersection),
			               SelectMatchers.withoutQueryTerm()
			           )
		           )
		);
	}

	@Test
	public void constructsCorrectUnion() {
		SelectionQuery selectionQuery1 = simpleSelection(1, 2, true);
		SelectionQuery selectionQuery2 = simpleSelection(1, 3, true);
		SelectionQuery selectionQuery = union(selectionQuery1, selectionQuery2);

		assertThat(selectionQuery.getSelectOperations(),
		           contains(
			           is(selectionQuery1.getSelectOperations().get(0)),
			           is(selectionQuery2.getSelectOperations().get(0)),
			           aSelectOperation(
				           withType(Union),
			               SelectMatchers.withoutQueryTerm()
			           )
		           )
		);
	}

	@Test
	public void constructsCorrectDifference() {
		SelectionQuery selectionQuery1 = simpleSelection(1, 2, true);
		SelectionQuery selectionQuery2 = simpleSelection(1, 3, true);
		SelectionQuery selectionQuery = difference(selectionQuery1, selectionQuery2);

		assertThat(selectionQuery.getSelectOperations(),
		           contains(
			           is(selectionQuery1.getSelectOperations().get(0)),
			           is(selectionQuery2.getSelectOperations().get(0)),
			           aSelectOperation(
				           withType(Difference),
			               SelectMatchers.withoutQueryTerm()
			           )
		           )
		);
	}


}
