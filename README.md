<!---
Copyright 2012 Benjamin Gehrels

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
--->
FlockDBClient
=============

*FlockDB Client* is a lightweight, idiomatic Java wrapper around the Thrift API of FlockDB, providing fluent interface
based access. You may call it a FlockDB Java driver.

Why should i use this Wrapper?
---------------------------

FlockDB uses  [Thrift](http://thrift.apache.org/ "Thrift") as it's remote communication technology. Thrift is a great
tool. It offers a simple way to create cross-platform binary remote communication APIs (RPC). But: The code generated by
Thrift is not what one would call a clean and beautiful API. Furthermore, FlockDB uses ByteBuffers a lot when it has to
send long[] over the wire, so you have to serialize and deserialize arguments yourself when using it.

This library tries to solve those issues. It provides you with builders for all the query methods supported by FlockDB,
an easy paging abstraction and a static import based, DSL-style approach to construct selection predicates.

How to get it
-------------
This library is written using Java 7 and will not work with prior Versions. If you are using Maven, you will (soon)
find this library on Maven Central. Just add the following dependency to your `pom.xml`:

	<dependency>
		<groupId>info.gehrels</groupId>
		<artifactId>FlockDB-Client</artifactId>
		<version>0.1-SNAPSHOT</version>
	</dependency>

If you do not use Maven as your build and dependency management tool, you may download the library from
[Maven Central](http://search.maven.org/#search|ga|1|flockdb%20client). You then also have to make sure to have all
dependencies in your classpath. At the moment of writing this documentation, these are
* libthrift = 0.8.0
* Google guava >= 13.0-rc2

Other versions may work, but are untested. If you tested one, please feel free to contribute your experiences to this
README file.

How to build it
---------------
If you want to build this library yourself, you will need some prerequisites:
* A Java JDK 7 or higher
* Maven 2.0 or higher
* Thrift 0.8.0 - the path to the Thrift binary must be in your `$PATH`/`%PATH%`, otherwise the Maven Plugin won't find it.

You can build it by entering `mvn package` from inside this directory. The binary `.jar`-file will then be found under
in the `target` directory.

How to use it
-------------
The primary entry point to the FlockDB Client is the class called `info.gehrels.flockDBClient.FlockDB`. You may simply
instantiate it using a the hostName and the port of your running FlockDB server:

	FlockDB myFlockConnection = new FlockDB("localhost", 7915);

### Mutating operations – add, remove, negate and archive
Once you have a `FlockDB` instance at your fingertips, you may use it for all the operations provided by the server. The
first thing you may want to do is adding nodes to your instance:

	myFlockConnection.batchExecution(Priority.Normal)
		.add(1, 2, new Date().getTime(), OUTGOING, 3)
		.add(3, 2, new Date().getTime(), OUTGOING, 4)
		.add(4, 2, new Date().getTime(), OUTGOING, 1)
		.execute();

This will add three edges, each one labeled with the current time to graph no. 2: one from node 1 to node 3, one from node 3
to node 4 and one from node 4 to node 1. Together, they form a triangle. You mach also use batch executions to remove,
negate and archive edges or to add multiple edges at once:

	myFlockConnection.batchExecution(Priority.Normal)
		.add(1, 2, new Date().getTime(), OUTGOING, 3, 4, 5, 6) // Adds 4 edges: 1->3, 1->4, 1->5, 1->6
		.negate(1, 2, OUTGOING, 4, 5) // negates edges 1->4, 1->5
		.archive(3, 2, INCOMING, 1) // archives edge 1->3
		.remove(1, 2, OUTGOING, 6) // removes edge 1->6
		.execute();

### Basic selections
After filling the database you will probably want to query your data. The easiest way to do this is querying for a
single edge:

	Edge edge = myFlockConnection.get(1, 2, 3);

This will return the edge 1->3 of graph no. 2 – or null, if it does not exist. If you are looking for a cheaper way to
test, if an edge exists in the database, you can also call

	boolean edgeExists = myFlockConnection.contains(1, 2, 3);

If you want to retrieve a Node with some Metadata about it, you can call

	Metadata nodeMetadata = myFlockConnection.getMetadata(1,2);

This will return the node metadata (number of edges, et cetera) of node 1 in graph no. 2. There is also a cheaper way to
test, if a node exists:

	boolean nodeExists = myFlockConnection.containsMetadata(1,2);

### More complex edge selections
There are often situations, where you want not to get a single edge, but a whole bunch of edges. For these use cases,
`selectEdges` will be your friend. You may even batch many batch selection queries into one call, leading to less
network io:

 	List<PagedEdgeList> edgeLists = myFlockConnection
 		.selectEdges(1, 2, OUTGOING) // retrieves all outgoing edges of node 1 in graph 2
 		.selectEdges(2, 3, INCOMING, 4, 5, 6) // retrieves the edges 4->2, 5->2, 6->2 in grap 3, if they exist.
 		.selectEdges(8, 9, OUTGOING).withPageSize(20) // retrieves the first 20 outgoing edges from node 8 in graph 9
 		.execute()

The paging options always belong to the last added selection. You may also specify a page offset by giving the id of the
first node of a page:

 	List<PagedEdgeList> edgeLists = myFlockConnection
 		.selectEdges(1, 2, OUTGOING).withPageStartNode(12345)
 		.selectEdges(2, 3, INCOMING).withPageStartNode(23456).withPageSize(20)
 		.execute()

The resulting `PagedEdgeList` instances offer an `Iterator<Edge>` for the current page, `getNextPage()` and
`getPreviousPage()` methods to navigate from page to page and `hasNextPage()` and `hasPreviousPage()` methods. If you
provide no page size, a default page size of `Integer.MAX_VALUE-1` will be used. You should therefore rarely see a
second page by default.

### More complex node selections
FlockDB also supports set arithmetic base queries over incident nodes. You may, for example, want to now, which users
follow person A and person B and are not blocked by person C:

	int FOLLOWS = 1;
	int BLOCKS = 2;
	List<PagedNodeIdList> result = myFlockDBConnection
		.select(
			difference(
				intersect(
					simpleSelection(personAId, FOLLOWS, INCOMING),
					simpleSelection(personBId, FOLLOWS, INCOMING)
				),
				simpleSelection(personCId, BLOCKS, OUTGOING)
			)
		)
		.execute();

As you may guess from the return type, multiple select queries may be batched and the result may be paged in the same
way as described above for the Edge selections.

What you have to be aware of
----------------------------
Even if this code has a quite good unit test coverage, nearly no integration tests have been written to see, if it
really works with a real FlockDB server. So I would not suggest to use this code in a production environment without
a lot of prior testing. If you find any errors or wrong/misleading documentation, please drop me a line or – better –
send me a pull request.