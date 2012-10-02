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
