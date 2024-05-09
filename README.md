# CrustyDB

This is the repository for the Academic Handout version of the CrustyDB project.
Please see your handout instructions for more information.

## CrustyDB 1 - Page Milestone

Implement the slotted page structure in the files `src/storage/heapstore/src/page.rs` 
and `src/storage/heapstore/src/heap_page.rs`. 
Please see your handout instructions for more information.

Do not modify any other files in the repository.

## CrustyDB 2 - HeapStore Milestone
Complete the implementation of the Heapstore in the `src/storage/heapstore/src`
crate. The files that you need to modify are:

- `src/storage/heapstore/src/heapfile.rs`
- `src/storage/heapstore/src/heapfileiter.rs`
- `src/storage/heapstore/src/storage_manager.rs`

If you are also implementing a Buffer Pool, you will additionally need to modify
the following files:
- 

Please see your handout instructions for more information.

## CrustyDB 3 - Query Operator Milestone
Complete the implementation of the query operators in the `src/queryexe/src/opiterator`
crate. The files that you need to modify are:

- `src/queryexe/src/opiterator/nested_loop_join.rs`
- `src/queryexe/src/opiterator/hash_join.rs`
- `src/queryexe/src/opiterator/aggregate.rs`


Please see your handout instructions for more information.

## Running and Testing CrustyDB End-to-End

Once you have completed the major milestones of CrustyDB, you can build the
entire database and run a client and server. To build the entire code base, go
to the root of the repository and run the following command:

```bash
cargo build
```

After compiling the database, start a server and a client instance.

To start the crustydb server:

```bash
cargo run --bin server
```

and to start the client:

```bash
cargo run --bin cli-crusty
```

### Client Commands

CrustyDB emulates `psql` (Postgres client) commands.

Command | Functionality
---------|--------------
`\r [DATABABSE]` | cReates a new database, DATABASE
`\c [DATABASE]` | Connects to DATABASE
`\i [PATH] [TABLE_NAME]` | Imports a csv file at PATH and saves it to TABLE_NAME in 
whatever database the client is currently connected to.
`\l` | List the name of all databases present on the server.
`\dt` | List the name of all tables present on the current database.
`\generate [CSV_NAME] [NUMBER_OF_RECORDS]` | Generate a test CSV for a sample schema.
`\reset` | Calls the reset command. This should delete all data and state for all databases on the server
`\shutdown` |  Shuts down the database server cleanly (allows the DB to gracefully exit)

There are other commands you can ignore for this class (register, runFull, runPartial, convert).

The client also handles basic SQL queries.

# An End-to-End Example

Start a server and a client process as described above. You may want to 
enable `DEBUG` or `TRACE` logging when launching the server to see more detailed
information about the code execution happening on the server side in 
response to client requests.

Then, from the client, you can create a database named `testdb`:

```
[crustydb]>> \r testdb 
```

Then, connect to the newly created database:

```
[crustydb]>> \c testdb
```

At this point, you can create a table `test` in the `testdb` database you are
connected to by writing the appropriate SQL command. Let's create a table with 2
Integer columns, which we are going to name `a` and `b`.

```
[crustydb]>> CREATE TABLE test (a INT, b INT, primary key (a));
```

At this point the table exists in the database, but it does not contain any
data. We include a CSV file in the repository (named `e2e-tests/csv/data.csv`)
with some sample data you can import into the newly created table. You can do
that by doing:

```
[crustydb]>> \i <PATH>/data.csv test
```

Note that you need to replace PATH with the path to the repository where the
`data.csv` file lives.

After importing the data, you can run basic SQL queries on the table. For
example:

```
[crustydb]>> SELECT a FROM test;
```

or:

```
[crustydb]>> SELECT sum(a), sum(b) FROM test;
```

As you follow through with this end-to-end example, we encourage you to take a
look at the log messages emitted by the server while running the server with
debug logging enabled. You can search for specific log messages in the code: that is an excellent way of understanding the lifecycle of query execution in crustydb.
