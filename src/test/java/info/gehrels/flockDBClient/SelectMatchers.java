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

import com.twitter.flockdb.thrift.SelectOperation;
import com.twitter.flockdb.thrift.SelectOperationType;
import com.twitter.flockdb.thrift.SelectQuery;
import org.hamcrest.Description;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import java.util.List;

import static info.gehrels.flockDBClient.ByteHelper.toLongArray;
import static info.gehrels.flockDBClient.PrimitiveMatchers.isLongArray;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;

class SelectMatchers {
	static TypeSafeDiagnosingMatcher<SelectOperation> aSelectOperation(
		final TypeSafeDiagnosingMatcher<SelectOperation>... subMatchers) {
		return new TypeSafeDiagnosingMatcher<SelectOperation>() {
			@Override
			protected boolean matchesSafely(SelectOperation selectOperation, Description mismatchDescription) {
				for (TypeSafeDiagnosingMatcher<SelectOperation> matcher : subMatchers) {
					if (!matcher.matches(selectOperation)) {
						mismatchDescription.appendText("a SelectOperation[");
						matcher.describeMismatch(selectOperation, mismatchDescription);
						mismatchDescription.appendText("]");
						return false;
					}
				}
				return true;
			}

			@Override
			public void describeTo(Description description) {
				description.appendText("a SelectOperation[");
				boolean first = true;
				for (TypeSafeDiagnosingMatcher<SelectOperation> subMatcher : subMatchers) {
					if (first) {
						first = false;
					} else {
						description.appendText(", ");
					}

					subMatcher.describeTo(description);
				}
				description.appendText("]");
			}
		};
	}

	static FeatureMatcher<SelectOperation, SelectOperationType> withType(SelectOperationType operationType) {
		return new FeatureMatcher<SelectOperation, SelectOperationType>(is(operationType), "operation type",
		                                                                "operation type") {
			@Override
			protected SelectOperationType featureValueOf(SelectOperation actual) {
				return actual.getOperation_type();
			}
		};
	}

	static FeatureMatcher<SelectOperation, Long> withSourceId(long sourceId) {
		return new FeatureMatcher<SelectOperation, Long>(is(sourceId), "source id", "source id") {
			@Override
			protected Long featureValueOf(SelectOperation selectOperation) {
				return selectOperation.getTerm().getSource_id();
			}
		};
	}

	static FeatureMatcher<SelectOperation, Integer> withGraphId(int graphId) {
		return new FeatureMatcher<SelectOperation, Integer>(is(graphId), "graph id", "graph id") {
			@Override
			protected Integer featureValueOf(SelectOperation selectOperation) {
				return selectOperation.getTerm().getGraph_id();
			}
		};
	}

	static FeatureMatcher<SelectOperation, Boolean> withForward(boolean isForward) {
		return new FeatureMatcher<SelectOperation, Boolean>(is(isForward), "forward", "forward") {
			@Override
			protected Boolean featureValueOf(SelectOperation selectOperation) {
				return selectOperation.getTerm().isIs_forward();
			}
		};
	}

	static FeatureMatcher<SelectQuery, List<SelectOperation>> withOperations(
		Matcher<Iterable<? extends SelectOperation>> subMatcher) {
		return new FeatureMatcher<SelectQuery, List<SelectOperation>>(subMatcher, "selectOperations:",
		                                                              "selectOperations:") {
			@Override
			protected List<SelectOperation> featureValueOf(SelectQuery selectQuery) {
				return selectQuery.getOperations();
			}
		};
	}

	static Matcher<SelectQuery> aSelectQuery(final TypeSafeDiagnosingMatcher<SelectQuery>... subMatchers) {
		return new TypeSafeDiagnosingMatcher<SelectQuery>() {
			@Override
			protected boolean matchesSafely(SelectQuery selectQuery, Description mismatchDescription) {
				for (TypeSafeDiagnosingMatcher<SelectQuery> subMatcher : subMatchers) {
					if (!subMatcher.matches(selectQuery)) {
						mismatchDescription.appendText("a SelectQuery[");
						subMatcher.describeMismatch(selectQuery, mismatchDescription);
						mismatchDescription.appendText("]");
						return false;
					}
				}


				return true;
			}

			@Override
			public void describeTo(Description description) {
				description.appendText("a SelectQuery[");
				boolean first = true;
				for (TypeSafeDiagnosingMatcher<SelectQuery> subMatcher : subMatchers) {
					if (first) {
						first = false;
					} else {
						description.appendText(", ");
					}
					description.appendDescriptionOf(subMatcher);
				}

				description.appendText("]");
			}
		};
	}

	static FeatureMatcher<SelectOperation, long[]> withDestinationIds(long... destinationIds) {
		return new FeatureMatcher<SelectOperation, long[]>(isLongArray(destinationIds), "destination ids:", "destination ids:") {
			@Override
			protected long[] featureValueOf(SelectOperation actual) {
				return toLongArray(actual.getTerm().getDestination_ids());
			}
		};
	}

	static TypeSafeDiagnosingMatcher<SelectOperation> withoutQueryTerm() {
		return new TypeSafeDiagnosingMatcher<SelectOperation>() {
			@Override
			protected boolean matchesSafely(SelectOperation item, Description mismatchDescription) {
				if (item.isSetTerm()) {
					mismatchDescription.appendText("queryTerm was set");
					return false;
				}

				return true;
			}

			@Override
			public void describeTo(Description description) {
				description.appendText("without a query Term");
			}
		};
	}

	static FeatureMatcher<SelectQuery, Long> withCursor(long startIndex) {
		return new FeatureMatcher<SelectQuery, Long>(equalTo(startIndex), "start index", "start index") {
			@Override
			protected Long featureValueOf(SelectQuery actual) {
				return actual.getPage().getCursor();
			}
		};
	}

	static FeatureMatcher<SelectQuery, Integer> withMaxResults(int maxResults) {
		return new FeatureMatcher<SelectQuery, Integer>(equalTo(maxResults), "max. results", "max. results") {
			@Override
			protected Integer featureValueOf(SelectQuery actual) {
				return actual.getPage().getCount();
			}
		};
	}
}
