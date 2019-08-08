# 5300-Antelope

**Joshua Halbert, Somya Kajla**  
*Seattle University, CPSC5300, Summer 2019*

### Assignment Requirements

This release fulfills the requirements of **Milestone 6** by doing the following:

1. Implements all required functions of btree.cpp.
2. Executes all tests successfully (see output below and try running `test` at the `SQL>` prompt.
3. Includes all required elements from Milestone 5, all functioning successfully.

### Compiling and Usage

- To compile, run `make` in the file directory.
	- Optionally, run `make clean` then `make` to rebuild all files from scratch.
- To run after compliation, run `./sql5300 *writeable directory relative to executable*`
	- *relative to the executable* means another directory in the same directory as the project files.
	- For example, after compiling, you might run `mkdir data` followed by `./sql5300 data`.
- Given the `SQL>` prompt, you may enter `test`, a SQL statement, or `quit`.
- Between tests, you may wish to delete the contents of your data directory to avoid errors. To preserve data between tests, this is not done automatically.

### Test Results

Current version is passing all the tests in the Milestone 6 requirements, as shown:

	[halbertj@cs1 sql5300]$ ./sql5300 data
	(sql5300: running with database environment at data)
	SQL> test
	test_heap_storage: 
	create ok
	drop ok
	create_if_not_exsts ok
	insert ok
	select/project ok 1
	many inserts/select/projects ok
	del ok
	test_heap_storage: ok
	over 10000 successful inserts
	btree index create ok
	Should be 12: 12
	Should be 88: 88
	Handle size for no key found should be 0: Handle size: 0
	many selects/projects/and lookups ok
	drop index ok
	test_btree: ok
	SQL> create table foo (id int, data text)
	CREATE TABLE foo (id INT, data TEXT)
	created foo
	SQL> insert into foo values (1,"one");insert into foo values(2,"two"); insert into foo values (2, "another two")
	INSERT INTO foo VALUES (1, "one")
	successfully inserted 1 row into foo
	INSERT INTO foo VALUES (2, "two")
	successfully inserted 1 row into foo
	INSERT INTO foo VALUES (2, "another two")
	successfully inserted 1 row into foo
	SQL> select * from foo
	SELECT * FROM foo
	id data 
	+----------+----------+
	1 "one" 
	2 "two" 
	2 "another two" 
	successfully returned 3 rows
	SQL> create index fxx on foo (id)
	CREATE INDEX fxx ON foo USING BTREE (id)
	Error: DbRelationError: Duplicate keys are not allowed in unique index
	SQL> show index from foo
	SHOW INDEX FROM foo
	table_name index_name column_name seq_in_index index_type is_unique 
	+----------+----------+----------+----------+----------+----------+
	successfully returned 0 rows
	SQL> delete from foo where data = "two"
	DELETE FROM foo WHERE data = "two"
	successfully deleted 1 rows from foo
	SQL> select * from foo
	SELECT * FROM foo
	id data 
	+----------+----------+
	1 "one" 
	2 "another two" 
	successfully returned 2 rows
	SQL> create index fxx on foo (id)
	CREATE INDEX fxx ON foo USING BTREE (id)
	created index fxx
	SQL> show index from foo
	SHOW INDEX FROM foo
	table_name index_name column_name seq_in_index index_type is_unique 
	+----------+----------+----------+----------+----------+----------+
	"foo" "fxx" "id" 1 "BTREE" true 
	successfully returned 1 rows
	SQL> insert into foo values (4,"four")
	INSERT INTO foo VALUES (4, "four")
	successfully inserted 1 row into foo and 1 indices
	SQL> select * from foo
	SELECT * FROM foo
	id data 
	+----------+----------+
	1 "one" 
	2 "another two" 
	4 "four" 
	successfully returned 3 rows
	SQL> quit
	[halbertj@cs1 sql5300]$ 

