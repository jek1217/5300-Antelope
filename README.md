# 5300-Antelope

**Joshua Halbert, Somya Kajla**  
*Seattle University, CPSC5300, Summer 2019*

### Assignment Requirements

This file fulfills the requirements of **Milestone 5** by doing the following:

1. Implements INSERT successfully, including inserting into indices
2. Implements SELECT * FROM table using EvalPlan successfully
3. Implements more complicated SELECTs (supporting **=** and **AND**)
4. Implements DELETE using EvalPlan successfully, including deleting from indices

### Compiling and Usage

- To compile, run `make` in the file directory.
	- Optionally, run `make clean` then `make` to rebuild all files from scratch.
- To run after compliation, run `./sql5300 *writeable directory relative to executable*`
	- *relative to the executable* means another directory in the same directory as the project files.
	- For example, after compiling, you might run `mkdir data` followed by `./sql5300 data`.
- Given the `SQL>` prompt, you may enter a SQL statement or `quit`, which will close the database and end the program.

### Test Results

Current version is passing all the tests in the Milestone 5 requirements, as shown:

	[halbertj@cs1 sql5300]$ ./sql5300 data
	(sql5300: running with database environment at data)
	SQL> show tables
	SHOW TABLES
	table_name 
	+----------+
	"goober" 
	successfully returned 1 rows
	SQL> create table foo (id int, data text)
	CREATE TABLE foo (id INT, data TEXT)
	created foo
	SQL> show tables
	SHOW TABLES
	table_name 
	+----------+
	"goober" 
	"foo" 
	successfully returned 2 rows
	SQL> show columns from foo
	SHOW COLUMNS FROM foo
	table_name column_name data_type 
	+----------+----------+----------+
	"foo" "id" "INT" 
	"foo" "data" "TEXT" 
	successfully returned 2 rows
	SQL> create index fx on foo (id)
	CREATE INDEX fx ON foo USING BTREE (id)
	created index fx
	SQL> create index fz on foo (data)
	CREATE INDEX fz ON foo USING BTREE (data)
	created index fz
	SQL> show index from foo 
	SHOW INDEX FROM foo
	table_name index_name column_name seq_in_index index_type is_unique 
	+----------+----------+----------+----------+----------+----------+
	"foo" "fx" "id" 1 "BTREE" true 
	"foo" "fz" "data" 1 "BTREE" true 
	successfully returned 2 rows
	SQL> insert into foo (id, data) values (1,"one")
	INSERT INTO foo (id, data) VALUES (1, "one")
	successfully inserted 1 row into foo and 2 indices
	SQL> select * from foo
	SELECT * FROM foo
	id data 
	+----------+----------+
	1 "one" 
	successfully returned 1 rows
	SQL> insert into foo values (2, "Two"); insert into foo values (3, "Three"); insert into foo values (99, "wowzers, Penny!!")
	INSERT INTO foo VALUES (2, "Two")
	successfully inserted 1 row into foo and 2 indices
	INSERT INTO foo VALUES (3, "Three")
	successfully inserted 1 row into foo and 2 indices
	INSERT INTO foo VALUES (99, "wowzers, Penny!!")
	successfully inserted 1 row into foo and 2 indices
	SQL> select * from foo
	SELECT * FROM foo
	id data 
	+----------+----------+
	1 "one" 
	2 "Two" 
	3 "Three" 
	99 "wowzers, Penny!!" 
	successfully returned 4 rows
	SQL> select * from foo where id=3
	SELECT * FROM foo WHERE id = 3
	id data 
	+----------+----------+
	3 "Three" 
	successfully returned 1 rows
	SQL> select * from foo where id=1 and data="one"
	SELECT * FROM foo WHERE id = 1 AND data = "one"
	id data 
	+----------+----------+
	1 "one" 
	successfully returned 1 rows
	SQL> select * from foo where id=99 and data="nine"
	SELECT * FROM foo WHERE id = 99 AND data = "nine"
	id data 
	+----------+----------+
	successfully returned 0 rows
	SQL> select id from foo
	SELECT id FROM foo
	id 
	+----------+
	1 
	2 
	3 
	99 
	successfully returned 4 rows
	SQL> select data from foo where id=1
	SELECT data FROM foo WHERE id = 1
	data 
	+----------+
	"one" 
	successfully returned 1 rows
	SQL> delete from foo where id=1
	DELETE FROM foo WHERE id = 1
	successfully deleted 1 rows from foo and 2 indices
	SQL> select * from foo
	SELECT * FROM foo
	id data 
	+----------+----------+
	2 "Two" 
	3 "Three" 
	99 "wowzers, Penny!!" 
	successfully returned 3 rows
	SQL> delete from foo
	DELETE FROM foo
	successfully deleted 3 rows from foo and 2 indices
	SQL> select * from foo
	SELECT * FROM foo
	id data 
	+----------+----------+
	successfully returned 0 rows
	SQL> insert into foo values (2, "Two"); insert into foo values (3, "Three"); insert into foo values (99, "wowzers, Penny!!")
	INSERT INTO foo VALUES (2, "Two")
	successfully inserted 1 row into foo and 2 indices
	INSERT INTO foo VALUES (3, "Three")
	successfully inserted 1 row into foo and 2 indices
	INSERT INTO foo VALUES (99, "wowzers, Penny!!")
	successfully inserted 1 row into foo and 2 indices
	SQL> select * from foo
	SELECT * FROM foo
	id data 
	+----------+----------+
	2 "Two" 
	3 "Three" 
	99 "wowzers, Penny!!" 
	successfully returned 3 rows
	SQL> drop index fz from foo
	DROP INDEX fz FROM foo
	dropped index fz
	SQL> show index from foo
	SHOW INDEX FROM foo
	table_name index_name column_name seq_in_index index_type is_unique 
	+----------+----------+----------+----------+----------+----------+
	"foo" "fx" "id" 1 "BTREE" true 
	successfully returned 1 rows
	SQL> insert into foo (id) VALUES (100)
	INSERT INTO foo (id) VALUES (100)
	Error: DbRelationError: don't know how to handle NULLs, defaults, etc. yet
	SQL> select * from foo
	SELECT * FROM foo
	id data 
	+----------+----------+
	2 "Two" 
	3 "Three" 
	99 "wowzers, Penny!!" 
	successfully returned 3 rows
	SQL> drop table foo
	DROP TABLE foo
	dropped foo
	SQL> show tables
	SHOW TABLES
	table_name 
	+----------+
	"goober" 
	successfully returned 1 rows
	SQL> quit
	[halbertj@cs1 sql5300]$
