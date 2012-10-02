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

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;

public class PrimitiveMatchers {
	static Matcher<long[]> isLongArray(final long... expected) {
		return new TypeSafeDiagnosingMatcher<long[]>() {
			@Override
			protected boolean matchesSafely(long[] actual, Description mismatchDescription) {
				if (expected.length != actual.length) {
					mismatchDescription.appendText("had wrong length: " + actual.length);
					return false;
				}

				for (int i = 0; i < expected.length; i++) {
					if (expected[i] != actual[i]) {
						mismatchDescription.appendText("item " + i + ": " + actual[i]);
						return false;
					}
				}
				return true;
			}

			@Override
			public void describeTo(Description description) {
				description.appendText("long[");
				boolean first = true;
				for (long l : expected) {
					if (!first) {
						description.appendText(",");
					}
					description.appendText(Long.toString(l));
					first = false;
				}
				description.appendText("]");
			}
		};
	}
}
