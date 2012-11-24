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

import com.twitter.flockdb.thrift.QueryTerm;
import com.twitter.flockdb.thrift.SelectOperation;
import com.twitter.flockdb.thrift.SelectOperationType;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.singletonList;

public class SelectionQuery {
	private final List<SelectOperation> selectOperations;

	private SelectionQuery(List<SelectOperation> selectOperations) {
		this.selectOperations = selectOperations;
	}

	List<SelectOperation> getSelectOperations() {
		return selectOperations;
	}

	public static SelectionQuery simpleSelection(long sourceId, int graphId, Direction direction,
	                                             long... destinationIds) {
		ByteBuffer buf = ByteHelper.asByteBufferOrNull(destinationIds);

		return new SelectionQuery(
			singletonList(
				new SelectOperation(SelectOperationType.SimpleQuery).setTerm(
					new QueryTerm(sourceId, graphId, direction.forward).setDestination_ids(buf))));
	}

	public static SelectionQuery intersect(SelectionQuery selectionQuery1, SelectionQuery selectionQuery2) {
		return selectionSetOperation(selectionQuery1, selectionQuery2, SelectOperationType.Intersection);
	}

	public static SelectionQuery difference(SelectionQuery selectionQuery1, SelectionQuery selectionQuery2) {
		return selectionSetOperation(selectionQuery1, selectionQuery2, SelectOperationType.Difference);
	}

	public static SelectionQuery union(SelectionQuery selectionQuery1, SelectionQuery selectionQuery2) {
		return selectionSetOperation(selectionQuery1, selectionQuery2, SelectOperationType.Union);
	}

	private static SelectionQuery selectionSetOperation(SelectionQuery selectOperation1,
	                                                    SelectionQuery selectOperation2,
	                                                    SelectOperationType operationType) {
		ArrayList<SelectOperation> result = new ArrayList<>(selectOperation1.selectOperations);
		result.addAll(selectOperation2.selectOperations);
		result.add(new SelectOperation(operationType));
		return new SelectionQuery(result);
	}
}
