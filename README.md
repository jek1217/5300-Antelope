# 5300-Antelope


## Sprint Verano: Vishakha Bhavsar and Virmel Gacad

Milestone 1: Skeleton

  -Sets up Berkeley DB environment
  -Uses SQLParserResult
  -sql5300 is the main driver program, being the default makefile target
  
  
  
Milestone 2: Rudimentary Storage Engine


  We have gotten mixed results with output/testing in regards to Milestone 2. Virmel has gotten the program to compile but
  reaches a library shared error when trying to run it. Vishakha has gotten to compile the program, run the program, and
  complete the test_heap_storage() test function. We have done all our work solely on CS1, so we do not know if our program works
  outside of CS1 as well.
  
  
  Our files include:
  
  -storage_engine.h, which was given to us
  -heap_storage.h
  -heap_storage.cpp
  -sql5300.cpp
  -Makefile
  
  
  Quirks:
  
  -Memory management could be looked into. 
  -Random quick-thought comments could be found throughout.
  
  
## Spring Ontono: Jake and Maggie
Milestones 3 and 4

We submitted fixes for the issues noted above in milestones 1 and 2.
  
Main methods to look at are as follows and exist in the SQLExec.cpp file:

 - create_table: creates table if it doesn't already exist and has a valid name
 - drop_table: drops a table on the condition that it already exists and removes  references in schema tables
 - show_table: shows tables  in database excluding our schema tables (_tables, _columns, _indices)
 - show_columns: shows the columns of a particular table determined by the user-proffered SQL statement
 - create_index: creates an index if it doesn't already exist and has a valid name
 - drop_index: drops an index on the condition that it already exists
 - show_index: shows the index(es) associated with with the following information:
 - - table name
 - - index name
 - - column name
 - - sequence in the index
 - - index type
 - - is unique
 
Directions to run:

In the 5300-Antelope directory enter the following commands:

1. make
2. mkdir data
3. ./sql5300 data
 
Directions to clean:

In the 5300-Antelope directory enter the following commands:

1. rm -rf data
2. make clean

