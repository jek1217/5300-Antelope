//Includes
#include <iostream>
#include <cstring>
#include <stdlib.h>
#include <stdio.h>
#include "heap_storage.h"
typedef u_int16_t u16;
using namespace std;

//Slotted Page section
SlottedPage :: SlottedPage(Dbt &block, BlockID block_id, bool is_new)
	: DbBlock(block, block_id, is_new){
		//Handling new block
		if(is_new){
			this->num_records = 0;
			this->end_free = DbBlock::BLOCK_SZ - 1;
			put_header();
		}
		else{
			//Passing in size and location into get_header. 
			get_header(this->num_records, this->end_free);
		}
	}

RecordID SlottedPage :: add(const Dbt* data) throw(DbBlockNoRoomError){
	if(!has_room(data->get_size())){
		throw DbBlockNoRoomError("Not enough room for new record");
	}
	this->num_records++;
	uint16_t id = this->num_records;
	uint16_t size = (uint16_t)data->get_size();
	this->end_free -=  size;
	uint16_t location = this->end_free + 1;
	
	//Update
	put_header();
	//New addition
	put_header(id, size, location);
	memcpy(this->address(location), data->get_data(), size);
	return id;
}

Dbt* SlottedPage :: get(RecordID record_id){
	uint16_t size;
	uint16_t location;

	get_header(size, location, record_id);

	if(location == 0){
		return nullptr;
	}
	//char* rec = new char[size];
    	//memcpy(rec, this->address(location), size);
    	//Dbt* returningRecord = new Dbt(rec, size);
	Dbt* returningRecord = new Dbt(this->address(location), size);
	return returningRecord;
}

void SlottedPage :: put(RecordID record_id, const Dbt &data) throw(DbBlockNoRoomError){
	uint16_t size;
	uint16_t location;

	get_header(size, location, record_id);

	//check size
	uint16_t check = (u16) data.get_size();
	if(check > size){
		uint16_t excess = check - size;
		if(!has_room(excess)){
			throw DbBlockNoRoomError("not enough room for new record");
		}
		//I think this is correct? Could possibly switch the two here? tricky
		slide(location + check, location + size);
		memcpy(this->address(location-excess), data.get_data(), check);
	}
	else{
		//This happens if size has not changed/increased
		memcpy(this->address(location), data.get_data(), check);
		slide(location + check, location + size);
	}
	//Updates
	get_header(size, location, record_id);
	put_header(record_id, check, location);
}

void SlottedPage :: del(RecordID record_id){
	uint16_t size;
	uint16_t location;
	get_header(size, location, record_id);
	put_header(record_id,0,0);
	slide(location, location + size);
}

RecordIDs* SlottedPage :: ids(void){
	uint16_t size = 0;
	uint16_t location = 0;
	uint16_t recordNum = this->num_records;

	RecordIDs* recordIDVector = new RecordIDs();

	//cout<<"recordNum="<<recordNum<<endl;

	for(int i = 1; i <= recordNum; i++){
		get_header(size, location, i);
		if(location != 0){
			recordIDVector->push_back(i);
			//cout<<"pushed "<<i<<" records"<<endl;
		}
	}
	return recordIDVector;
}

void SlottedPage :: put_n(uint16_t offset, uint16_t n){
	//Fix later? Is this Correct?
	*(uint16_t*)this->address(offset) = n;
}

void SlottedPage :: put_header(RecordID id, uint16_t size, uint16_t location){
	if(id == 0){
		size = this->num_records;
		location = this->end_free;
	}
	put_n(4 * id, size);
	put_n(4 * id + 2, location);
}

void SlottedPage :: get_header(uint16_t &size, uint16_t &location, RecordID id){
	/*if(id == 0){
		//Update
		size = this->num_records;
		location = this->end_free;
	}
	else{
		put_n(id *4, size); 
		put_n(id *4 + 2, location);
	}*/
    size = this->get_n(4 * id);
    location = this->get_n(4 * id + 2);
}

bool SlottedPage :: has_room(uint16_t size){
	/*uint16_t remainingRoom = end_free - ((num_records + 1) * 4);
	//CHECK IF THIS IS CORRECT
	return (remainingRoom >= size);
	*/
    u16 available = this->end_free - (this->num_records + 1) * 4;
    return size <= available;
}

void SlottedPage :: slide(uint16_t start, uint16_t end){
u16 shift = end - start;
    if (shift == 0) {
        return;
    }
    void* from = this->address((u16)this->end_free + 1);
    void* to = this ->address((u16)this ->end_free + 1 + shift);
    uint sizeOfPage = (start - this->end_free + 1);
    memmove(to, from, sizeOfPage);

    u16 loc, size;

    //iterating through the records and put the values accordingly
    for(int i = 1; i <= this->num_records; i++){
        this->get_header(size, loc, i);
        if (loc <= start) {
            loc += shift;
            this->put_header(i, size, loc);
        }
    }
    this->end_free += shift;
    this->put_header();
}

uint16_t SlottedPage :: get_n(uint16_t offset){
	return *(uint16_t*)address(offset);
}

void* SlottedPage :: address(uint16_t offset){
	return (void*)((char*)this->block.get_data() + offset);
}

//HeapFile Section now

	void HeapFile :: create(){
	   this->db_open(DB_CREATE | DB_EXCL);
	   DbBlock * block = this->get_new();
	   this->put(block);	
	   delete block;
	}	
	void HeapFile :: open(){
	   this->db_open();
	   //this->block_size = this->stat['re_len'];
	}
	void HeapFile :: close(){
	   this->db.close(0);
	   this->closed= true;	   
	}
	
	void HeapFile:: drop(){
		this->close();
		remove(this->dbfilename.c_str());
	}
	SlottedPage* HeapFile :: get_new(){
		char block[DbBlock::BLOCK_SZ];
    		std::memset(block, 0, sizeof(block));
    		Dbt data(block, sizeof(block));

    		int block_id = ++this->last;
    		Dbt key(&block_id, sizeof(block_id));

    		// write out an empty block and read it back in so Berkeley DB is managing the memory
        	SlottedPage* page = new SlottedPage(data, this->last, true);
             	this->db.put(nullptr, &key, &data, 0); // write it out with initialization applied
                this->db.get(nullptr, &key, &data, 0);
                return page;
	}

	SlottedPage* HeapFile :: get(BlockID block_id){
		char block[DbBlock::BLOCK_SZ];
		Dbt rdata(block, sizeof(block));

		Dbt key(&block_id, sizeof(block_id));
		this->db.get(nullptr, &key, &rdata, 0); //read the given block id
		SlottedPage* page= new SlottedPage(rdata,block_id);
		return page;
	}
	void HeapFile :: put(DbBlock* block){
		BlockID block_id= block->get_block_id();
		void* block_data= block->get_data();
		//int block_id = ++this->last;
                Dbt key(&block_id, sizeof(block_id));
		Dbt data(block_data, DbBlock::BLOCK_SZ);
		this->db.put(nullptr, &key, &data, 0);
	}
	BlockIDs* HeapFile :: block_ids(){
		BlockIDs* ids = new BlockIDs;
		for(BlockID i=1; i < this->last+1; i++){
			ids->push_back(i);
		}
		return ids;
	}
	
	void HeapFile :: db_open(uint flags){
		if(!this->closed){
			return;
		}
		this->db.set_re_len(DbBlock::BLOCK_SZ);	
		//this->db=db(_DB_ENV,0);
			const char* dbEnvHome=nullptr;
			_DB_ENV->get_home(&dbEnvHome);
			//string dbEnvHomeStr(dbEnvHome);
			//this->dbfilename= dbEnvHomeStr.c_str() + this->name.c_str()+ ".db";
			char str[80];
    			strcpy(str, dbEnvHome);
    			strcat(str, "/");
    			strcat(str, this->name.c_str());
    			strcat(str, ".db");
    			this->dbfilename = str;			

			cout<<"dbfilename="<<this->dbfilename<<endl;
			//this->dbfilename= this->name + ".db"; 
			
			this->db.open(nullptr, this->dbfilename.c_str(), nullptr, DB_RECNO, flags,0);
			DB_BTREE_STAT stat;
			this->db.stat(nullptr, &stat, DB_FAST_STAT);
			this->last = stat.bt_ndata;
			this->closed = false; 
		
	}

//HeapTable Section now
//Identifier == string as a typedef
//ColumnNames == vector of Identifiers
//ColumnAttributes == vector of ColumnAttribute, which is a class that holds an int and text

HeapTable :: HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes)
  : DbRelation(table_name, column_names, column_attributes), file(table_name){
	
}

void HeapTable :: create(){
  this->file.create();
}

//Not sure how to go about this one
void HeapTable :: create_if_not_exists(){
  try{
    this->file.open();
  }
  catch(exception &e){
    this->create();
  }
}

void HeapTable :: drop(){
  this->file.drop();
}

void HeapTable :: open(){
  this->file.open();
}

void HeapTable :: close(){
  this->file.close();
}

Handle HeapTable :: insert(const ValueDict* row){
  this->open();
  return this->append(this->validate(row));
}

//HeapTable implementation for Milestone2 requires: create, create_if_not_exists, open, close, and drop + insert
void HeapTable :: update(const Handle handle, const ValueDict* new_values){
  throw DbRelationError("Update is not supported yet");
}

void HeapTable :: del(const Handle handle){
    this->open();
    SlottedPage* victim= this->file.get(handle.first);
    victim->del(handle.second);
    this->file.put(victim);
    delete victim;
}

Handles* HeapTable :: select(){
  Handles* handles = new Handles();
  BlockIDs* block_ids = file.block_ids();
  for (auto const& block_id : *block_ids){    
    SlottedPage* block = file.get(block_id);
    RecordIDs* record_ids = block->ids();
    cout<<"record_ids size="<<record_ids->size()<<endl;
    //int i=0;
    for (auto const& record_id: *record_ids){
      handles->push_back(Handle(block_id, record_id));
      //cout<<"pushed "<<++i<<" hanldes"<<endl;
    }
    delete record_ids;
    delete block;
  }
  delete block_ids;
  return handles;
}


Handles* HeapTable :: select(const ValueDict* where){
    Handles* h = nullptr;
    return h;
}


ValueDict* HeapTable :: project(Handle handle){
    ColumnNames* v = &this->column_names;
    return project(handle, v);
}

ValueDict* HeapTable :: project(Handle handle, const ColumnNames* column_names){
    SlottedPage* block = this->file.get(handle.first);
    Dbt* data = block->get(handle.second);
    ValueDict* row = unmarshal(data);
    delete data;
    delete block;

    if (column_names->empty()) {
        return row;
    } else {
        ValueDict* result = new ValueDict();
        for(auto const& column_name:*column_names){
           if (row->find(column_name) == row->end()){
              throw DbRelationError("table does not have column named '" + column_name + "'");
           }
           (*result)[column_name] = (*row)[column_name];
        }
    cout << "result retrieved" << std::endl;
        return result;
    }
}

ValueDict* HeapTable :: validate(const ValueDict* row){
  //ValueDict == map of <Identifier, value> where identifier is a string
  //uint16_t col_num = 0;
  ValueDict* returnRow = new ValueDict();
  
  for(auto const& column_name : this->column_names){
    //ColumnAttribute ca = this->column_attributes[col_num++];
    ValueDict::const_iterator column = row->find(column_name);
    //match
    if(column == row->end()){
      throw DbRelationError("Error: not fully functioning yet");
    }
    else{
      Value value = row->at(column_name);
      (*returnRow)[column_name] = value;
    }
  }
  return returnRow;
}

Handle HeapTable :: append(const ValueDict* row){
  //Handle handle;
  RecordID record_id;
  Dbt* data = marshal(row);
  uint32_t last_id = this->file.get_last_block_id();
  
  SlottedPage* chosenBlock = this->file.get(last_id);

  try{
    record_id = chosenBlock->add(data);
  }
  catch(exception &e){
    chosenBlock = this->file.get_new();
    record_id = chosenBlock->add(data);
  }
  
  this->file.put(chosenBlock);

  //Is this correct?
  //handle.first = this->file.get_last_block_id();
  //handle.second = record_id;

  //return handle;
  return Handle(this->file.get_last_block_id(), record_id);
}

Dbt* HeapTable :: marshal(const ValueDict* row){
  
  char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
  uint16_t offset = 0;
  uint16_t col_num = 0;
  for (auto const& column_name: this->column_names) {
    ColumnAttribute ca = this->column_attributes[col_num++];
    ValueDict::const_iterator column = row->find(column_name);
    Value value = column->second;
    if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
      *(uint32_t*) (bytes + offset) = value.n;
      offset += sizeof(uint32_t);
    } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
      uint size = value.s.length();
      *(uint16_t*) (bytes + offset) = size;
      offset += sizeof(uint16_t);
      memcpy(bytes+offset, value.s.c_str(), size); // assume ascii for now
      offset += size;
    } else {
      throw DbRelationError("Only know how to marshal INT and TEXT");
    }
  }
  char *right_size_bytes = new char[offset];
  memcpy(right_size_bytes, bytes, offset);
  delete[] bytes;
  Dbt *data = new Dbt(right_size_bytes, offset);
  return data;
}

ValueDict* HeapTable :: unmarshal(Dbt* data){
char* bytes = (char*) data->get_data();
    ValueDict* row = new ValueDict();
    unsigned offset = 0;
    unsigned colNum = 0;
    for (auto const& col : this->column_names) {
        Value val;
        ColumnAttribute myAttr = this->column_attributes[colNum++];
        val.data_type = myAttr.get_data_type();
        if (myAttr.get_data_type() == ColumnAttribute::DataType::INT) {
            // read in INT from data's bytes
	    val.n = *(int32_t*)(bytes + offset);
            val.data_type = ColumnAttribute::DataType::INT;
            offset += sizeof(int32_t);
        }
        else if (myAttr.get_data_type() == ColumnAttribute::DataType::TEXT) {
	    u_int16_t size = *(u_int16_t*)(bytes + offset);
            offset += sizeof(u_int16_t);
            char buffer[DbBlock::BLOCK_SZ];
            memcpy(buffer, bytes + offset, size);
            buffer[size] = '\0';
            val.s = std::string(buffer);
            val.data_type = ColumnAttribute::DataType::TEXT;
            offset += size;	
        }
        else {
            throw DbRelationError(
                    "Not able to unmarshal - only INT and TEXT types are supported");
        }
        (*row)[col] = val;
    }
    return row;
}

// test function -- returns true if all tests pass
bool test_heap_storage() {
  ColumnNames column_names;
  column_names.push_back("a");
  column_names.push_back("b");
  
  ColumnAttributes column_attributes;
  
  ColumnAttribute ca(ColumnAttribute::INT);
  column_attributes.push_back(ca);
  
  ca.set_data_type(ColumnAttribute::TEXT);
  column_attributes.push_back(ca);

  HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
  table1.create();
  std::cout << "create ok" << std::endl;
  
  table1.drop();  // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
  std::cout << "drop ok" << std::endl;

  HeapTable table("_test_data_cpp", column_names, column_attributes);
  table.create_if_not_exists();
  std::cout << "create_if_not_exsts ok" << std::endl;

  ValueDict row;
  row["a"] = Value(12);
  row["b"] = Value("Hello!");
  
  std::cout << "try insert" << std::endl;
  table.insert(&row);
  std::cout << "insert ok" << std::endl;
  
  Handles* handles = table.select();
  std::cout << "select ok " << handles->size() << std::endl;
  
  ValueDict *result = table.project((*handles)[0]);
  std::cout << "project ok" << std::endl;
  
  Value value = (*result)["a"];
  //std::cout << "a=" <<value.n<< std::endl;
  if (value.n != 12)
    return false;
  
  value = (*result)["b"];
  //std::cout << "b=" <<value.s<< std::endl;
  if (value.s != "Hello!")
    return false;
  
  table.drop();

  return true;
}
