//Includes
#include <iostream>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>
#include <db_cxx.h>
#include "SQLParser.h"
#include "sqlhelper.h"
#include "SQLParserResult.h"

#include "heap_storage.h"

//#include "heap_storage.cpp"


DbEnv* _DB_ENV;

using namespace std;
using namespace hsql;

string handleTableNameRef(TableRef* t);
string handleExpr(Expr* expr);
string execute(const SQLStatement* st);

int main(int argc, char* argv[]){

  bool cont = true;
  string que;

  if(argc != 2){
    cout << "Error: Incorrect input for command line" << endl;
    return -1;
  }

  char* envHome = argv[1];
  DbEnv env(0U);
  env.set_message_stream(&cout);
  env.set_error_stream(&cerr);

  cout << "Connecting to database "<< envHome << endl;

  try{
    env.open(envHome, DB_CREATE | DB_INIT_MPOOL, 0);
  }
  catch(DbException &e){
    cout << "Error while opening database environment: " << envHome << endl;
    cout << e.what() << endl;
    exit(-1);
  }
  catch(exception &e){
    cout << "Error while opening database environment: " << envHome << endl;
    cout << e.what() << endl;
    exit(-1);
  }

  //Confirm that the database is working now
  cout << "( " << argv[0] << ": running with database environment: " << envHome << ")" << endl;

  
  //Global database initialized
  _DB_ENV = &env;
  
  //Querying from user
  while(cont){
    cout << "SQL>";
    getline(cin, que);
    //Error checking here
    if(que.length() == 0){
      continue;
    }
    if(que == "quit"){
      cont = false;
    }
    else if(que == "test"){
      cout << "Now testing the test_heap_storage() function" << endl;
      bool result = test_heap_storage();
      if(result){
        cout << "Passed the tests!" << endl;
      }
      else{
        cout << "Did not pass all the tests" << endl;
      }
    }
    
    else{
      SQLParserResult* result = SQLParser::parseSQLString(que);

      if (!result->isValid() || result->size() == 0) {
        cout << "Invalid SQL: \"" << que << "\"" << endl;
        continue;
      } else {
        const SQLStatement* statement = result->getStatement(0);
        cout << "Parsed Statement = \"" << execute(statement) << "\"" << endl;
      }
    }
  }
  //Closing out DB environment

  cout << "Closing out DB environment " << envHome << endl;

  try{
    env.close(0);
  }
  catch(DbException &error){
    cout << "Error while trying to close database environment: " << envHome << endl;
    cout << error.what() << endl;
    exit(-1);
  }
  catch(exception &error){
    cout << "Error while trying to close database environment: " << envHome << endl;
    cout << error.what() << endl;
    exit(-1);
  }

  //DB environment should be closed.
  

  return 0;
}

string execute(const SQLStatement* st) {
  string qstr = "";
  if(kStmtSelect == st->type()){
    qstr = qstr + "SELECT ";

    const SelectStatement* slst = (SelectStatement*) st;

    //handle distict keyword
    if(slst->selectDistinct == true){
      qstr += "DISTINCT ";
    }
    //handle select list expressions
    for(Expr* expr : *slst->selectList){
      qstr += handleExpr(expr);
      qstr += ", ";
    }
    //Remove extra comma
    qstr = qstr.substr(0, qstr.length() -2);

    //handle from expression
    TableRef* fromTable = slst->fromTable;
    qstr += "FROM ";
    if(fromTable->schema != NULL){
      qstr += fromTable->schema;
      qstr += ".";
    }
    // handle different TableRefTypes
    if(fromTable->type == kTableName){
      qstr += handleTableNameRef(fromTable);
    }else if(fromTable->type == kTableJoin){
      //handle left
      qstr += handleTableNameRef(fromTable->join->left);

      //handle join type
      switch(fromTable->join->type){
      case kJoinCross:
      case kJoinInner:
        qstr += " JOIN ";
        break;
      case kJoinOuter:
      case kJoinLeftOuter:
      case kJoinLeft:
        qstr += " LEFT JOIN ";
        break;
      case kJoinRightOuter:
      case kJoinRight:
        qstr += " RIGHT JOIN ";
        break;
      case kJoinNatural:
        qstr += " NATURAL JOIN ";
        break;
      }

      //handle right
      qstr += handleTableNameRef(fromTable->join->right);

      //handle join condition
      qstr += "ON ";
      qstr += handleExpr(fromTable->join->condition);
    }

    //handle where clause
    if(slst->whereClause != NULL){
      Expr* whereClause = slst->whereClause;
      qstr += "WHERE ";
      qstr += handleExpr(whereClause);
    }
  }
  return qstr;
}

string handleExpr(Expr* expr){
  string qstr="";
  switch(expr->type){
  case kExprStar:
    qstr += "* "; break;
  case kExprSelect:
    qstr += "kExprSelect "; break;
  case kExprLiteralString:
    qstr += expr->name;
    qstr += " ";break;
  case kExprColumnRef:
    if (expr->table != NULL)
      qstr += string(expr->table) + ".";
    qstr += expr->name;
    qstr += " ";
    break;
  case kExprLiteralInt:
    cout<< "Integer Expression"<<endl;
    qstr += to_string(expr->ival);
    qstr += " ";
    break;
  case kExprOperator:
    qstr += handleExpr(expr->expr);
    qstr += " ";

    switch(expr->opType){
    case Expr::SIMPLE_OP:
      qstr += expr->opChar;
      break;
    case Expr::AND:
      qstr += "AND";
      break;
    case Expr::OR:
      qstr += "OR";
      break;
    default:
      qstr += "? ";break;
    }
    qstr += " ";

    if(expr->expr2!=NULL)
      qstr += handleExpr(expr->expr2);
    break;
  default:
    qstr += "? ";break;
  }
  if (expr->alias != NULL){
    qstr += " AS ";
    qstr += expr->alias;
    qstr += " ";
  }
  return qstr;
}
string handleTableNameRef(TableRef* t){
  string s ="";
  s += t->name;
  s += " ";
  if(t->alias != NULL){
    s += "AS ";
    s += t->alias;
    s += " ";
  }
  return s;
}
