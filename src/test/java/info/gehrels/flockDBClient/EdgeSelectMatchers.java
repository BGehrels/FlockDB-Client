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

import com.twitter.flockdb.thrift.EdgeQuery;
import org.hamcrest.Description;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;

public class EdgeSelectMatchers {
	static TypeSafeDiagnosingMatcher<EdgeQuery> anEdgeQuery(
		final TypeSafeDiagnosingMatcher<EdgeQuery>... subMatchers) {
		return new TypeSafeDiagnosingMatcher<EdgeQuery>() {
			@Override
			protected boolean matchesSafely(EdgeQuery edegQueries, Description mismatchDescription) {
				for (TypeSafeDiagnosingMatcher<EdgeQuery> matcher : subMatchers) {
					if (!matcher.matches(edegQueries)) {
						mismatchDescription.appendText("an EdgeQuery[");
						matcher.describeMismatch(edegQueries, mismatchDescription);
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
				for (TypeSafeDiagnosingMatcher<EdgeQuery> subMatcher : subMatchers) {
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

	static FeatureMatcher<EdgeQuery, Long> withSourceId(long sourceId) {
		return new FeatureMatcher<EdgeQuery, Long>(is(sourceId), "source id", "source id") {
			@Override
			protected Long featureValueOf(EdgeQuery selectOperation) {
				return selectOperation.getTerm().getSource_id();
			}
		};
	}

	static FeatureMatcher<EdgeQuery, Integer> withGraphId(int graphId) {
		return new FeatureMatcher<EdgeQuery, Integer>(is(graphId), "graph id", "graph id") {
			@Override
			protected Integer featureValueOf(EdgeQuery selectOperation) {
				return selectOperation.getTerm().getGraph_id();
			}
		};
	}

	static FeatureMatcher<EdgeQuery, Boolean> withForward(boolean isForward) {
		return new FeatureMatcher<EdgeQuery, Boolean>(is(isForward), "forward", "forward") {
			@Override
			protected Boolean featureValueOf(EdgeQuery selectOperation) {
				return selectOperation.getTerm().isIs_forward();
			}
		};
	}

	static FeatureMatcher<EdgeQuery, Iterable<Long>> withDestinationIds(
		Matcher<? super Iterable<Long>> subMatcher) {
		return new FeatureMatcher<EdgeQuery, Iterable<Long>>(subMatcher, "destination ids:", "destination ids:") {
			@Override
			protected Iterable<Long> featureValueOf(EdgeQuery actual) {
				return ByteHelper.toLongIterable(actual.getTerm().getDestination_ids());
			}
		};
	}

	static FeatureMatcher<EdgeQuery, Long> withStartIndex(long startIndex) {
		return new FeatureMatcher<EdgeQuery, Long>(equalTo(startIndex), "start index", "start index") {
			@Override
			protected Long featureValueOf(EdgeQuery actual) {
				return actual.getPage().getCursor();
			}
		};
	}

	static FeatureMatcher<EdgeQuery, Integer> withMaxResults(int maxResults) {
		return new FeatureMatcher<EdgeQuery, Integer>(equalTo(maxResults), "max. results", "max. results") {
			@Override
			protected Integer featureValueOf(EdgeQuery actual) {
				return actual.getPage().getCount();
			}
		};
	}
}
