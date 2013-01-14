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

import com.twitter.flockdb.thrift.Edge;
import com.twitter.flockdb.thrift.FlockDB.Client;
import com.twitter.flockdb.thrift.FlockDB.Iface;
import com.twitter.flockdb.thrift.FlockException;
import com.twitter.flockdb.thrift.Metadata;
import com.twitter.flockdb.thrift.Priority;
import info.gehrels.flockDBClient.FlockAndThriftExceptionHandling.MethodObject;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;

import static info.gehrels.flockDBClient.FlockAndThriftExceptionHandling.handleFlockAndThriftExceptions;

public class FlockDB {
	private TTransport transport;
	private Iface backingFlockClient;

	public FlockDB(String hostname, int port) throws IOException {
		this(hostname, port, 1000);
	}

	public FlockDB(String hostname, int port, int timeoutInMilliSeconds) throws IOException {
		transport = new TFramedTransport(new TSocket(hostname, port, timeoutInMilliSeconds));
		TProtocol protocol = new TBinaryProtocol(transport);
		try {
			transport.open();
		} catch (TTransportException e) {
			throw new IOException("Opening the transport failed", e);
		}
		backingFlockClient = new Client(protocol);
	}

	FlockDB(Iface flockDbIFaceMock) {
		backingFlockClient = flockDbIFaceMock;
	}

	public boolean contains(final long sourceId, final int graphId, final long destinationId) throws IOException {
		return handleFlockAndThriftExceptions(new MethodObject<Boolean>() {
			@Override
			public Boolean call() throws TException, FlockException {
				return backingFlockClient.contains(sourceId, graphId, destinationId);
			}
		});
	}

	public Edge get(final long sourceId, final int graphId, final long destinationId) throws IOException {
		return handleFlockAndThriftExceptions(new MethodObject<Edge>() {
			@Override
			public Edge call() throws TException, FlockException {
				return backingFlockClient.get(sourceId, graphId, destinationId);
			}
		});
	}

	public Metadata getMetadata(final long sourceId, final int graphId) throws IOException {
		return handleFlockAndThriftExceptions(new MethodObject<Metadata>() {
			@Override
			public Metadata call() throws TException, FlockException {
				return backingFlockClient.get_metadata(sourceId, graphId);
			}
		});
	}

	public boolean containsMetadata(final long sourceId, final int graphId) throws IOException {
		return handleFlockAndThriftExceptions(new MethodObject<Boolean>() {
			@Override
			public Boolean call() throws TException, FlockException {
				return backingFlockClient.contains_metadata(sourceId, graphId);
			}
		});
	}

	public SelectionBuilder select(SelectionQuery firstQuery) throws IOException {
		return new SelectionBuilder(backingFlockClient).select(firstQuery);
	}

	public CountBuilder count(SelectionQuery selectionQuery) throws IOException {
		return new CountBuilder(backingFlockClient).count(selectionQuery);
	}

	public EdgeSelectionBuilder selectEdges(long sourceId, int graphId, Direction direction,
	                                        long... destinationIds) throws IOException {
		return new EdgeSelectionBuilder(backingFlockClient).selectEdges(sourceId, graphId, direction, destinationIds);
	}

	public ExecutionBuilder batchExecution(Priority priority) throws IOException {
		return new ExecutionBuilder(backingFlockClient, priority);
	}

	public void close() {
		transport.close();
	}

}
