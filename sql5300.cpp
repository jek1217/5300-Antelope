

//Includes


#include <iostream>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <db_cxx.h>
#include "SQLParser.h"
#include "sqlhelper.h"


using namespace std;
using namespace hsql;


int main(int argc, char* argv[]){


  bool continue = true;
  string query;
  
  if(argc != 2){

    cout << "Error: Incorrect input for command line" << endl;
    return -1;
    
  }

  char* envHome = argv[1];
  DbEnv *myEnv(0);

  try{
    myEnv->open(envHome, DB_CREATE | DB_INIT_MPOOL, 0);
  }
  catch(DbException &e){
    cout << "Error while opening database environment: " <<
      envHome << endl;
    cout << e.what() << endl;
    exit(-1);
  }
  catch(exception &e){
    cout << "Error while opening database environment: " <<
      envHome << endl;
    cout << e.what() << endl;
    exit(-1);
  }


  //Confirm that the database is working now

  cout << "( " << argv[0] << ": running with database environment: " << envHome << ")" << endl;

  //Querying from user

  while(continue){

    cout << "SQL>";
    getline(cin, query);


    //Error checking here

    if(ToLowerCase(query) == "quit"){
      continue = false;
    }
    else{
      SQLParserResult* result = parseSQLString(query)

    
