//Completed Milestone 5 by Joshua Halbert
//still needs cleaning and commenting before delivery
//all tests pass

/**
 * @file SQLExec.cpp - implementation of SQLExec class 
 * @author Kevin Lundeen, Jake Ladera, Jiaxin Dong
 * @see "Seattle University, CPSC5300, Summer 2018"
 */
#include <algorithm>
#include "SQLExec.h"
#include "EvalPlan.h"
#include <sstream>

using namespace std;
using namespace hsql;

Tables* SQLExec::tables = nullptr;
Indices* SQLExec::indices = nullptr;

// prints results to screen
ostream &operator<<(ostream &out, const QueryResult &qres) {
    if (qres.column_names != nullptr) {
        for (auto const &column_name: *qres.column_names)
            out << column_name << " ";
        out << endl << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (auto const &row: *qres.rows) {
            for (auto const &column_name: *qres.column_names) {
                Value value = row->at(column_name);
                switch (value.data_type) {
                    case ColumnAttribute::INT:
                        out << value.n;
                        break;
                    case ColumnAttribute::TEXT:
                        out << "\"" << value.s << "\"";
                        break;
					case ColumnAttribute::BOOLEAN:
						out << (value.n == 0 ? "false" : "true");
						break;
                    default:
                        out << "???";
                }
                out << " ";
            }
            out << endl;
        }
    }
    out << qres.message;
    return out;
}

// cleans up query
QueryResult::~QueryResult() {
    if (column_names != nullptr)
        delete column_names;
    if (column_attributes != nullptr)
        delete column_attributes;
    if (rows != nullptr) {
        for (auto row: *rows)
            delete row;
        delete rows;
    }
}

// executes a sql statement, throws if not supported
QueryResult *SQLExec::execute(const SQLStatement *statement) throw(SQLExecError) {
    // initialize _tables table, if not yet present
    if (SQLExec::tables == nullptr) {
        SQLExec::tables = new Tables();
		SQLExec::indices = new Indices();
	}

    try {
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement *) statement);
            case kStmtDrop:
                return drop((const DropStatement *) statement);
            case kStmtShow:
                return show((const ShowStatement *) statement);
            case kStmtInsert:
                return insert((const InsertStatement *) statement);
            case kStmtDelete:
                return del((const DeleteStatement *) statement);
            case kStmtSelect:
                return select((const SelectStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError& e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

// sets input column attribute to input data type
void SQLExec::column_definition(const ColumnDefinition *col, Identifier& column_name,
                                ColumnAttribute& column_attribute) {
    column_name = col->name;
    switch (col->type) {
        case ColumnDefinition::INT:
            column_attribute.set_data_type(ColumnAttribute::INT);
            break;
        case ColumnDefinition::TEXT:
            column_attribute.set_data_type(ColumnAttribute::TEXT);
            break;
        case ColumnDefinition::DOUBLE:
        default:
            throw SQLExecError("unrecognized data type");
    }
}

// creates table or index
QueryResult *SQLExec::create(const CreateStatement *statement) {
    switch(statement->type) {
        case CreateStatement::kTable:
            return create_table(statement);
        case CreateStatement::kIndex:
            return create_index(statement);
        default:
            return new QueryResult("Only CREATE TABLE and CREATE INDEX are implemented");
    }
}
 
// creates a table
// input: sql statement with table and columns
// output: result from querying
QueryResult *SQLExec::create_table(const CreateStatement *statement) {
    Identifier table_name = statement->tableName;
    ColumnNames column_names;
    ColumnAttributes column_attributes;
    Identifier column_name;
    ColumnAttribute column_attribute;
    for (ColumnDefinition *col : *statement->columns) {
        column_definition(col, column_name, column_attribute);
        column_names.push_back(column_name);
        column_attributes.push_back(column_attribute);
    }

    // Add to schema: _tables and _columns
    ValueDict row;
    row["table_name"] = table_name;
    Handle t_handle = SQLExec::tables->insert(&row);  // Insert into _tables
    try {
        Handles c_handles;
        DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
        try {
            for (uint i = 0; i < column_names.size(); i++) {
                row["column_name"] = column_names[i];
                row["data_type"] = Value(column_attributes[i].get_data_type() == ColumnAttribute::INT ? "INT" : "TEXT");
                c_handles.push_back(columns.insert(&row));  // Insert into _columns
            }

            // Finally, actually create the relation
            DbRelation& table = SQLExec::tables->get_table(table_name);
            if (statement->ifNotExists)
                table.create_if_not_exists();
            else
                table.create();

        } catch (exception& e) {
            // attempt to remove from _columns
            try {
                for (auto const &handle: c_handles)
                    columns.del(handle);
            } catch (...) {}
            throw;
        }

    } catch (exception& e) {
        try {
            // attempt to remove from _tables
            SQLExec::tables->del(t_handle);
        } catch (...) {}
        throw;
    }
    return new QueryResult("created " + table_name);
}

// creates index for a table
// input: sql statement with index and table
// output: result from querying
QueryResult *SQLExec::create_index(const CreateStatement *statement) {
    Identifier table_name = statement->tableName;
    Identifier index_name = statement->indexName;
    Identifier index_type;
    bool is_unique;

    Handles columnHandles;
    try {
        index_type = statement->indexType;
    } catch (exception &e) {
        index_type = "BTREE";
    }
    
    if (index_type == "BTREE") {
        is_unique = true;
    } else {
        is_unique = false;
    }
    ValueDict row;
    row["table_name"] = table_name;
    row["index_name"] = index_name;
    row["seq_in_index"] = 0;
    row["index_type"] = index_type;
    row["is_unique"] = is_unique;
    try {
        for (auto const &col : *statement->indexColumns) {
            row["seq_in_index"].n += 1;
            row["column_name"] = string(col);
            columnHandles.push_back(SQLExec::indices->insert(&row));
        }
        DbIndex &index = SQLExec::indices->get_index(table_name, index_name);
        index.create();
    } catch (exception &e) {
        for (auto const &handle : columnHandles) {
            SQLExec::indices->del(handle);
        }
        throw;
    }
    return new QueryResult("Created index " + index_name);
}

// DROP ...
QueryResult *SQLExec::drop(const DropStatement *statement) {
    switch(statement->type) {
        case DropStatement::kTable:
            return drop_table(statement);
        case DropStatement::kIndex:
            return drop_index(statement);
        default:
            return new QueryResult("Only DROP TABLE and CREATE INDEX are implemented");
    }
}
 
// drops table
// input: sql statement
// output: result from querying
QueryResult *SQLExec::drop_table(const DropStatement *statement) {
    Identifier table_name = statement->name;
    if (table_name == Tables::TABLE_NAME || table_name == Columns::TABLE_NAME || table_name == Indices::TABLE_NAME)
        throw SQLExecError("cannot drop a schema table");

    ValueDict where;
    where["table_name"] = Value(table_name);

    // get the table
    DbRelation& table = SQLExec::tables->get_table(table_name);

    // drop the indexes
    Handles* indexHandles = indices->select(&where);
    for (auto const& handle: *indexHandles) {
        indices->del(handle);
    }
    delete indexHandles;

    // remove from _columns schema
    DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
    Handles* handles = columns.select(&where);
    for (auto const& handle: *handles)
        columns.del(handle);
    delete handles;

    // remove table
    table.drop();

    // finally, remove from _tables schema
    SQLExec::tables->del(*SQLExec::tables->select(&where)->begin()); // expect only one row from select

    return new QueryResult(string("dropped ") + table_name);
}

// drops index from table
// input: sql statement
// output: result from querying
QueryResult *SQLExec::drop_index(const DropStatement *statement) {
    Identifier table_name = statement->name;
    Identifier index_name = statement->indexName;

    DbIndex& index = indices->get_index(table_name, index_name);
    index.drop();

    ValueDict where;
    where["table_name"] = Value(table_name);
    where["index_name"] = Value(index_name);

    Handles* handles = indices->select(&where);
    for (auto const& handle: *handles) {
        indices->del(handle);
    }
    bool notFound = handles->size() == 0;
    delete handles;
    
    if (notFound) {
        return new QueryResult(string("index not found: " + index_name));
    }
    return new QueryResult(string("dropped index ") + index_name);
}

// shows tables, columns, or index if input sql is valid
// input: sql statement
// output: result from querying
QueryResult *SQLExec::show(const ShowStatement *statement) {
    switch (statement->type) {
        case ShowStatement::kTables:
            return show_tables();
        case ShowStatement::kColumns:
            return show_columns(statement);
        case ShowStatement::kIndex:
            return show_index(statement);
        default:
            throw SQLExecError("unrecognized SHOW type");
    }
}

// shows all indexes for a table
// input: sql statement
// output: result from querying
QueryResult *SQLExec::show_index(const ShowStatement *statement) {
    SQLExec::indices = new Indices();    

    ColumnNames* column_names = new ColumnNames();
    column_names->push_back("table_name");
    column_names->push_back("index_name");
    column_names->push_back("column_name");
    column_names->push_back("seq_in_index");
    column_names->push_back("index_type");
    column_names->push_back("is_unique");

    ValueDict where;
    where["table_name"] = Value(statement->tableName);
    Handles* handles = indices->select(&where);
    u_long n = handles->size();

    ValueDicts* rows = new ValueDicts;
    for (auto const& handle: *handles) {
        ValueDict* row = indices->project(handle, column_names);
        rows->push_back(row);
    }

    delete handles;
    return new QueryResult(column_names, nullptr, rows,
                           "successfully returned " + to_string(n) + " rows");
}

// shows all tables in database
// input: none
// output: result from querying
QueryResult *SQLExec::show_tables() {
    ColumnNames* column_names = new ColumnNames;
    column_names->push_back("table_name");

    ColumnAttributes* column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    Handles* handles = SQLExec::tables->select();
    u_long n = handles->size() - 3;

    ValueDicts* rows = new ValueDicts;
    for (auto const& handle: *handles) {
        ValueDict* row = SQLExec::tables->project(handle, column_names);
        Identifier table_name = row->at("table_name").s;
        if (table_name != Tables::TABLE_NAME && table_name != Columns::TABLE_NAME && table_name != Indices::TABLE_NAME)
            rows->push_back(row);
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows,
                           "successfully returned " + to_string(n) + " rows");
}

// shows the columns for a table
// input: sql statement
// output: result from querying
QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);

    ColumnNames* column_names = new ColumnNames;
    column_names->push_back("table_name");
    column_names->push_back("column_name");
    column_names->push_back("data_type");

    ColumnAttributes* column_attributes = new ColumnAttributes;
    column_attributes->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    ValueDict where;
    where["table_name"] = Value(statement->tableName);
    Handles* handles = columns.select(&where);
    u_long n = handles->size();

    ValueDicts* rows = new ValueDicts;
    for (auto const& handle: *handles) {
        ValueDict* row = columns.project(handle, column_names);
        rows->push_back(row);
    }
    delete handles;
    return new QueryResult(column_names, column_attributes, rows,
                           "successfully returned " + to_string(n) + " rows");
}


QueryResult *SQLExec::insert(const InsertStatement *statement) {
    //construct the ValueDict row
    //insert the ValueDict row
    //also add to any indices
    //useful methods are get_table, get_index_names, and get_index
    //column order may differ from other in table def.
    
    //get the table
    DbRelation& table = SQLExec::tables->get_table(statement->tableName);
   
    //get the columns FIXME what if there are none given?
    ColumnNames column_names;
    
    //handle if no columns given (pretend default order)
    if(statement->columns == nullptr) {
        column_names = table.get_column_names();
    }
    else {
        for (auto const &column: *statement->columns) {
            column_names.push_back(column);
        }
    }
    
    //get the values
    ValueDict values;
    int valCount = 0;
    for (Expr *expr : *statement->values) {
        
        //either ival or name
        Value val;
        switch(expr->type) {
            case(kExprLiteralString):
                val = Value(expr->name);
                break;
            case(kExprLiteralInt):
                val = Value(expr->ival);
                break;
            default:
                throw SQLExecError("unrecognized INSERT data type");
        }
        values[to_string(valCount)] = val;
        valCount++;
    }
    
    //build the row
    ValueDict row;
    for(u_int i = 0; i < column_names.size(); i++) {
        row[column_names.at(i)] = values[to_string(i)];
    }
    
    //insert the row
    Handle t_handle = table.insert(&row);
    
    string suffix = "";
    stringstream ss;
    IndexNames index_names = indices->get_index_names(statement->tableName);
    for(u_int i = 0; i < index_names.size(); i++) {
        DbIndex& index = indices->get_index(statement->tableName, index_names[i]);
        index.insert(t_handle);
        if(i + 1 >= index_names.size()) {
            ss << i+1;
            suffix = " and " + ss.str() + " indices";
        }
    }
    
    return new QueryResult("successfully inserted 1 row into " + string(statement->tableName) + suffix);  // FIXME
}

ValueDict* get_where_conjunction(const Expr *expr) {
    //recursively pull out ANDs and =s
    //build a ValueDict where
    
    ValueDict *where = new ValueDict();
    
    switch(expr->opType) {
        case Expr::SIMPLE_OP: {
            if(expr->opChar == '=') {
                if(expr->expr2->type == kExprLiteralInt)
                    where->emplace(string(expr->expr->name), Value(expr->expr2->ival));
                else if (expr->expr2->type == kExprLiteralString)
                    where->emplace(string(expr->expr->name), Value(expr->expr2->name));
                else
                    throw SQLExecError("unrecognized literal type");
            }
            else
                throw SQLExecError("unrecognized operation type");
            break;
        }
        case Expr::AND: {
            //get both expressions, recurse
            ValueDict* where2 = get_where_conjunction(expr->expr);
            ValueDict* where3 = get_where_conjunction(expr->expr2);
            where->insert(where2->begin(), where2->end());
            where->insert(where3->begin(), where3->end());
            delete where2;
            delete where3;
            break;
        }
        default:
            throw SQLExecError("unrecognized operation type");
            break;
    }
    return where;
}

QueryResult *SQLExec::del(const DeleteStatement *statement) {
    
    //get the table
    DbRelation& table = SQLExec::tables->get_table(statement->tableName);
    
    //start base of plan at a tablescan
    EvalPlan *plan = new EvalPlan(table);
    
    //enclose it in a Select if we have a where clause
    if (statement->expr != nullptr)
        plan = new EvalPlan(get_where_conjunction(statement->expr), plan);
    
    EvalPlan *optimized = plan->optimize();
    EvalPipeline pipeline = optimized->pipeline();
    
    //get the indices and handles 
    IndexNames index_names = indices->get_index_names(statement->tableName);
    Handles *handles = pipeline.second;
    
    stringstream ss;
    string suffix = "";
    
    //delete all handles from each index
    for(u_int i = 0; i < index_names.size(); i++) {
        DbIndex& index = indices->get_index(statement->tableName, index_names[i]);
        for (auto const& handle : *handles) {
            index.del(handle);
        }
        if(i + 1 >= index_names.size()) {
            ss << i+1;
            suffix = " and " + ss.str() + " indices";
        }
    }
    
    //delete all handles from the table
    u_int rowCount = 0;
    for (auto const& handle : *handles) {
        table.del(handle);
        rowCount++;
    }
    
    ss.str("");
    ss << rowCount;
    
    return new QueryResult("successfully deleted " + ss.str() + " rows from " + string(statement->tableName) + suffix);
}

QueryResult *SQLExec::select(const SelectStatement *statement) {
    
    //get the table
    DbRelation& table = SQLExec::tables->get_table(statement->fromTable->name);
    
    //start base of plan at a tablescan
    EvalPlan *plan = new EvalPlan(table);
    
    //enclose it in a Select if we have a where clause
    if (statement->whereClause != nullptr)
        plan = new EvalPlan(get_where_conjunction(statement->whereClause), plan);
    
    //determine column names
    ColumnNames *column_names = new ColumnNames;
    for (Expr *expr : *statement->selectList) {
        if(expr->type == kExprStar)
            *column_names = table.get_column_names();
        else
            column_names->push_back(expr->name);
    }
    
    //wrap in a project (not using projectall b/c not needed the way columns are done)
    plan = new EvalPlan(column_names, plan);
        
    //optimize the plan and evaluate the optimized plan
    EvalPlan *optimized = plan->optimize();
    ValueDicts *rows = optimized->evaluate();
    
    u_int rowNum = rows->size();
    stringstream ss;
    ss << rowNum;
    
    //get column attributes for returning in queryResult
    ColumnAttributes *column_attributes = table.get_column_attributes(*column_names);
    
    return new QueryResult(column_names, column_attributes, rows, "successfully returned " + ss.str() + " rows");
}
