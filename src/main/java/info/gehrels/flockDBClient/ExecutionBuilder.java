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

import com.twitter.flockdb.thrift.ExecuteOperation;
import com.twitter.flockdb.thrift.ExecuteOperationType;
import com.twitter.flockdb.thrift.ExecuteOperations;
import com.twitter.flockdb.thrift.FlockDB.Iface;
import com.twitter.flockdb.thrift.FlockException;
import com.twitter.flockdb.thrift.Priority;
import com.twitter.flockdb.thrift.QueryTerm;
import info.gehrels.flockDBClient.FlockAndThriftExceptionHandling.MethodObject;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.List;

import static info.gehrels.flockDBClient.ByteHelper.asByteBufferOrNull;

public class ExecutionBuilder {
	private final Iface backingFlockClient;
	private final Priority priority;
	private final List<ExecuteOperation> operations = new ArrayList<>();

	ExecutionBuilder(Iface backingFlockClient, Priority priority) {
		this.backingFlockClient = backingFlockClient;
		this.priority = priority;
	}

	public ExecutionBuilder add(long sourceId, int graphId, long position, Direction direction,
	                            long... destinationIds) {
		this.operations.add(
			new ExecuteOperation(
				ExecuteOperationType.Add,
				new QueryTerm(sourceId, graphId, direction.forward)
					.setDestination_ids(asByteBufferOrNull(destinationIds))
			).setPosition(position)
		);
		return this;
	}

	public ExecutionBuilder remove(long sourceId, int graphId, Direction direction, long... destinationIds) {
		this.operations.add(
			new ExecuteOperation(
				ExecuteOperationType.Remove,
				new QueryTerm(sourceId, graphId, direction.forward)
					.setDestination_ids(asByteBufferOrNull(destinationIds))
			)
		);
		return this;
	}

	public ExecutionBuilder negate(long sourceId, int graphId, Direction direction, long... destinationIds) {
		this.operations.add(
			new ExecuteOperation(
				ExecuteOperationType.Negate,
				new QueryTerm(sourceId, graphId, direction.forward)
					.setDestination_ids(asByteBufferOrNull(destinationIds))
			)
		);
		return this;
	}

	public ExecutionBuilder archive(long sourceId, int graphId, Direction direction, long... destinationIds) {
		this.operations.add(
			new ExecuteOperation(
				ExecuteOperationType.Archive,
				new QueryTerm(sourceId, graphId, direction.forward)
					.setDestination_ids(asByteBufferOrNull(destinationIds))
			)
		);
		return this;
	}

	public void execute() {
		FlockAndThriftExceptionHandling.handleFlockAndThriftExceptions(new MethodObject<Void>() {
			@Override
			public Void call() throws TException, FlockException {
				backingFlockClient.execute(new ExecuteOperations(operations, priority));
				return null;
			}
		});
	}

	Iface getBackingFlockClient() {
		return backingFlockClient;
	}

	Priority getPriority() {
		return priority;
	}
}
