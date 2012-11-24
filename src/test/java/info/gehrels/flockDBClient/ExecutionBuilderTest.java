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

import com.twitter.flockdb.thrift.ExecuteOperations;
import com.twitter.flockdb.thrift.FlockDB.Iface;
import com.twitter.flockdb.thrift.FlockException;
import org.apache.thrift.TException;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;

import static com.twitter.flockdb.thrift.ExecuteOperationType.Add;
import static com.twitter.flockdb.thrift.ExecuteOperationType.Archive;
import static com.twitter.flockdb.thrift.ExecuteOperationType.Negate;
import static com.twitter.flockdb.thrift.ExecuteOperationType.Remove;
import static com.twitter.flockdb.thrift.Priority.Low;
import static info.gehrels.flockDBClient.Direction.OUTGOING;
import static info.gehrels.flockDBClient.ExecutionMatchers.anOperation;
import static info.gehrels.flockDBClient.ExecutionMatchers.hasOperations;
import static info.gehrels.flockDBClient.ExecutionMatchers.hasPriority;
import static info.gehrels.flockDBClient.ExecutionMatchers.withDestinationIds;
import static info.gehrels.flockDBClient.ExecutionMatchers.withForward;
import static info.gehrels.flockDBClient.ExecutionMatchers.withGraphId;
import static info.gehrels.flockDBClient.ExecutionMatchers.withPosition;
import static info.gehrels.flockDBClient.ExecutionMatchers.withSourceId;
import static info.gehrels.flockDBClient.ExecutionMatchers.withType;
import static info.gehrels.flockDBClient.ExecutionMatchers.withoutDestinationIds;
import static info.gehrels.flockDBClient.ExecutionMatchers.withoutPosition;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ExecutionBuilderTest {
	private Iface mock = mock(Iface.class);
	private ExecutionBuilder builderUnderTest = new ExecutionBuilder(mock, Low);
	private ArgumentCaptor<ExecuteOperations> executeOperationsCapture =
		ArgumentCaptor.forClass(ExecuteOperations.class);


	@Test
	public void callsFlockDbWithCorrectQueryOnAddWithoutDestinationIds() throws FlockException, TException, IOException {
		builderUnderTest.add(1, 2, 3, OUTGOING).execute();
		verify(mock).execute(executeOperationsCapture.capture());
		ExecuteOperations executeOperations = executeOperationsCapture.getValue();

		assertThat(executeOperations, allOf(
			hasPriority(Low),
			hasOperations(
				anOperation(
					withType(Add),
					withSourceId(1),
					withGraphId(2),
					withPosition(3),
					withForward(true),
					withoutDestinationIds()
				)
			)
		));
	}

	@Test
	public void callsFlockDbWithCorrectQueryOnAddWithDestinationIds() throws FlockException, TException, IOException {
		builderUnderTest.add(1, 2, 3, OUTGOING, 4, 5, 6).execute();

		verify(mock).execute(executeOperationsCapture.capture());
		ExecuteOperations executeOperations = executeOperationsCapture.getValue();

		assertThat(executeOperations, allOf(
			hasPriority(Low),
			hasOperations(
				anOperation(
					withType(Add),
					withSourceId(1),
					withGraphId(2),
					withPosition(3),
					withForward(true),
					withDestinationIds(4L, 5L, 6L)
				)
			)
		));
	}

	@Test
	public void callsFlockDbWithCorrectQueryOnRemoveWithoutDestinationIds() throws FlockException, TException, IOException {
		builderUnderTest.remove(1, 2, OUTGOING).execute();

		verify(mock).execute(executeOperationsCapture.capture());
		ExecuteOperations executeOperations = executeOperationsCapture.getValue();

		assertThat(executeOperations, allOf(
			hasPriority(Low),
			hasOperations(
				anOperation(
					withType(Remove),
					withSourceId(1),
					withGraphId(2),
					withoutPosition(),
					withForward(true),
					withoutDestinationIds()
				)
			)
		));
	}

	@Test
	public void callsFlockDbWithCorrectQueryOnRemoveWithDestinationIds() throws FlockException, TException, IOException {
		builderUnderTest.remove(1, 2, OUTGOING, 4, 5, 6).execute();

		verify(mock).execute(executeOperationsCapture.capture());
		ExecuteOperations executeOperations = executeOperationsCapture.getValue();

		assertThat(executeOperations, allOf(
			hasPriority(Low),
			hasOperations(
				anOperation(
					withType(Remove),
					withSourceId(1),
					withGraphId(2),
					withoutPosition(),
					withForward(true),
					withDestinationIds(4L, 5L, 6L)
				)
			)
		));
	}

	@Test
	public void callsFlockDbWithCorrectQueryOnNegateWithoutDestinationIds() throws FlockException, TException, IOException {
		builderUnderTest.negate(1, 2, OUTGOING).execute();

		verify(mock).execute(executeOperationsCapture.capture());
		ExecuteOperations executeOperations = executeOperationsCapture.getValue();

		assertThat(executeOperations, allOf(
			hasPriority(Low),
			hasOperations(
				anOperation(
					withType(Negate),
					withSourceId(1),
					withGraphId(2),
					withoutPosition(),
					withForward(true),
					withoutDestinationIds()
				)
			)
		));
	}

	@Test
	public void callsFlockDbWithCorrectQueryOnNegateWithDestinationIds() throws FlockException, TException, IOException {
		builderUnderTest.negate(1, 2, OUTGOING, 4, 5, 6).execute();

		verify(mock).execute(executeOperationsCapture.capture());
		ExecuteOperations executeOperations = executeOperationsCapture.getValue();

		assertThat(executeOperations, allOf(
			hasPriority(Low),
			hasOperations(
				anOperation(
					withType(Negate),
					withSourceId(1),
					withGraphId(2),
					withoutPosition(),
					withForward(true),
					withDestinationIds(4L, 5L, 6L)
				)
			)
		));
	}

	@Test
	public void callsFlockDbWithCorrectQueryOnArchiveWithoutDestinationIds() throws FlockException, TException, IOException {
		builderUnderTest.archive(1, 2, OUTGOING).execute();

		verify(mock).execute(executeOperationsCapture.capture());
		ExecuteOperations executeOperations = executeOperationsCapture.getValue();

		assertThat(executeOperations, allOf(
			hasPriority(Low),
			hasOperations(
				anOperation(
					withType(Archive),
					withSourceId(1),
					withGraphId(2),
					withoutPosition(),
					withForward(true),
					withoutDestinationIds()
				)
			)
		));
	}

	@Test
	public void callsFlockDbWithCorrectQueryOnArchiveWithDestinationIds() throws FlockException, TException, IOException {
		builderUnderTest.archive(1, 2, OUTGOING, 4, 5, 6).execute();

		verify(mock).execute(executeOperationsCapture.capture());
		ExecuteOperations executeOperations = executeOperationsCapture.getValue();

		assertThat(executeOperations, allOf(
			hasPriority(Low),
			hasOperations(
				anOperation(
					withType(Archive),
					withSourceId(1),
					withGraphId(2),
					withoutPosition(),
					withForward(true),
					withDestinationIds(4L, 5L, 6L)
				)
			)
		));
	}


}
