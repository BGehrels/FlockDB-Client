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

import com.twitter.flockdb.thrift.ExecuteOperation;
import com.twitter.flockdb.thrift.ExecuteOperationType;
import com.twitter.flockdb.thrift.ExecuteOperations;
import com.twitter.flockdb.thrift.Priority;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;

import java.util.List;

import static info.gehrels.flockDBClient.ByteHelper.toLongIterable;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ExecutionMatchers {
	static Matcher<ExecuteOperation> withoutPosition() {
		return new FeatureMatcher<ExecuteOperation, Boolean>(is(false), "has position", "has position") {
			@Override
			protected Boolean featureValueOf(ExecuteOperation actual) {
				return actual.isSetPosition();
			}
		};
	}

	static Matcher<ExecuteOperation> withoutDestinationIds() {
		return new FeatureMatcher<ExecuteOperation, Boolean>(is(false), "has destination ids", "has destination ids") {
			@Override
			protected Boolean featureValueOf(ExecuteOperation actual) {
				return actual.getTerm().isSetDestination_ids();
			}
		};
	}

	static Matcher<ExecuteOperation> withPosition(long position) {
		return new FeatureMatcher<ExecuteOperation, Long>(is(position), "with position", "position") {
			@Override
			protected Long featureValueOf(ExecuteOperation actual) {
				return actual.getPosition();
			}
		};
	}

	static FeatureMatcher<ExecuteOperation, Iterable<Long>> withDestinationIds(Long... destinationIds) {
		return new FeatureMatcher<ExecuteOperation, Iterable<Long>>(contains(destinationIds), "with destination ids",
		                                                            "destination ids") {
			@Override
			protected Iterable<Long> featureValueOf(ExecuteOperation actual) {
				return toLongIterable(actual.getTerm().getDestination_ids());
			}
		};
	}

	static FeatureMatcher<ExecuteOperation, Boolean> withForward(boolean forward) {
		return new FeatureMatcher<ExecuteOperation, Boolean>(is(forward), "with forward", "forward") {
			@Override
			protected Boolean featureValueOf(ExecuteOperation actual) {
				return actual.getTerm().isIs_forward();
			}
		};
	}

	static FeatureMatcher<ExecuteOperation, Integer> withGraphId(int graphId) {
		return new FeatureMatcher<ExecuteOperation, Integer>(is(graphId), "with graph id", "graph id") {
			@Override
			protected Integer featureValueOf(ExecuteOperation actual) {
				return actual.getTerm().getGraph_id();
			}
		};
	}

	static Matcher<ExecuteOperation> withSourceId(long startNodeId) {
		return new FeatureMatcher<ExecuteOperation, Long>(is(startNodeId), "with source id", "source id") {
			@Override
			protected Long featureValueOf(ExecuteOperation actual) {
				return actual.getTerm().getSource_id();
			}
		};
	}

	static Matcher<ExecuteOperation> anOperation(Matcher<ExecuteOperation>... matchers) {
		return allOf(matchers);
	}

	static Matcher<ExecuteOperation> withType(ExecuteOperationType type) {
		return new FeatureMatcher<ExecuteOperation, ExecuteOperationType>(is(type), "with type", "type") {
			@Override
			protected ExecuteOperationType featureValueOf(ExecuteOperation actual) {
				return actual.getOperation_type();
			}
		};
	}

	static Matcher<? super ExecuteOperations> hasOperations(Matcher<ExecuteOperation>... subMatcher) {
		return new FeatureMatcher<ExecuteOperations, List<ExecuteOperation>>(contains(subMatcher), "has operations",
		                                                                     "operations") {
			@Override
			protected List<ExecuteOperation> featureValueOf(ExecuteOperations actual) {
				return actual.getOperations();
			}
		};
	}

	static FeatureMatcher<ExecuteOperations, Priority> hasPriority(Priority priority) {
		return new FeatureMatcher<ExecuteOperations, Priority>(equalTo(priority), "has priority", "priority") {
			@Override
			protected Priority featureValueOf(ExecuteOperations actual) {
				return actual.getPriority();
			}
		};
	}
}
