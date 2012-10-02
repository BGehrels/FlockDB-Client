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
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.IOException;

public class FlockDB {
    Iface backingFlockClient;

    public FlockDB(String hostname, int port) throws IOException {
        TTransport transport = new TFramedTransport(new TSocket(hostname, port, 1000));
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

    public boolean contains(long sourceId, int graphId, long destinationId) throws FlockException, IOException {
        try {
            return backingFlockClient.contains(sourceId, graphId, destinationId);
        } catch (TException e) {
            throw new IOException(e);
        }
    }

    public Edge get(long sourceId, int graphId, long destinationId) throws FlockException, IOException {
        try {
            return backingFlockClient.get(sourceId, graphId, destinationId);
        } catch (TException e) {
            throw new IOException(e);
        }
    }

    public Metadata getMetadata(long sourceId, int graphId) throws FlockException, IOException {
        try {
            return backingFlockClient.get_metadata(sourceId, graphId);
        } catch (TException e) {
            throw new IOException(e);
        }
    }

    public boolean containsMetadata(long sourceId, int graphId) throws FlockException, IOException {
        try {
            return backingFlockClient.contains_metadata(sourceId, graphId);
        } catch (TException e) {
            throw new IOException(e);
        }
    }

    public SelectBuilder select(SelectionQuery firstQuery) throws FlockException, IOException {
        return new SelectBuilder(backingFlockClient).select(firstQuery);
    }

	public SelectBuilder select(SelectionQuery firstQuery, long startIndex, int maxResults) throws FlockException, IOException {
     return new SelectBuilder(backingFlockClient).select(firstQuery, startIndex, maxResults);
 }

    public CountBuilder count(SelectionQuery selectionQuery) throws FlockException, IOException {
        return new CountBuilder(backingFlockClient).count(selectionQuery);
    }

    public EdgeSelectionBuilder selectEdges(long sourceId, int graphId, boolean forward, long... destinationIds) throws FlockException, IOException {
        return new EdgeSelectionBuilder(backingFlockClient).selectEdges(sourceId, graphId, forward, destinationIds);
    }

	public EdgeSelectionBuilder selectEdges(long sourceId, int graphId, long startIndex, int maxResults, boolean forward, long... destinationIds) throws FlockException, IOException {
     return new EdgeSelectionBuilder(backingFlockClient).selectEdges(sourceId, graphId, startIndex, maxResults, forward, destinationIds);
 }

    public ExecutionBuilder batchExecution(Priority priority) throws FlockException, IOException {
        return new ExecutionBuilder(backingFlockClient, priority);
    }
}
