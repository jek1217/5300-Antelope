
//Includes
#include <iostream>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "heap_storage.h"

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

  uint16_t size = 0;
  uint16_t location = 0;

  get_header(size, location, record_id);

  if(location == 0){
       return NULL;
  }
  Dbt* returningRecord = new Dbt(this->address(location), size);
  return returningRecord;
}

void SlottedPage :: put(RecordID record_id, const Dbt &data) throw(DbBlockNoRoomError){

  
  uint16_t size = 0;
  uint16_t location = 0;

  get_header(size, location, record_id);

  //check size

  uint16_t check = data.get_size();

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

  uint16_t size = 0;
  uint16_t location = 0;
  get_header(size, location, record_id);
  put_header(record_id);
  slide(location, location + size);
}

RecordIDs* SlottedPage :: ids(void){

  uint16_t size = 0;
  uint16_t location = 0;
  uint16_t recordNum = this->num_records;

  RecordIDs* recordIDVector = new RecordIDs(recordNum);

  for(int i = 0; i < recordNum + 1; i++){
     get_header(size, location, i);
     if(location != 0){
        recordIDVector->push_back(i);
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
     put_n(id * 4, size);
     put_n(id * 4 + 2, location);
  }

void SlottedPage :: get_header(uint16_t &size, uint16_t &location, RecordID id){
    if(id == 0){
       //Update
       size = this->num_records;
       location = this->end_free;
    }
    else{
	put_n(id *4, size); 
	put_n(id *4 + 2, location);
    }
}

bool SlottedPage :: has_room(uint16_t size){
    uint16_t remainingRoom = end_free - ((num_records + 1) * 4);
    //CHECK IF THIS IS CORRECT
    return (remainingRoom >= size);
}

void SlottedPage :: slide(uint16_t start, uint16_t end){

  uint16_t difference = end - start;

  memcpy(address(end_free + difference + 1), address(end_free + 1), difference);


  RecordIDs* recordIDVector = ids();

  for(RecordID recordID : recordIDVector){

    uint16_t size = 0;
    uint16_t location = 0;

    get_header(size, location, recordID);
    if(start >= location){

      location += difference;
      put_header(recordID, size, location);
    }
  }


  end_free += difference;
  put_header();
}


uint16_t SlottedPage :: get_n(uint16_t offset){

  return *(uint16_t*)address(offset);
}

void* SlottedPage :: address(uint16_t offset){

  return (void*)((char*)this->block.get_data() + offset);
}

//HeapFile Section now

HeapFile :: HeapFile(string name) : DbFile(name){

	void HeapFile :: create(){

	}	
	void HeapFile :: drop(){


	}
	void HeapFile :: open(){
	}
	void HeapFle :: close(){
	}
	
	SlottedPage* HeapFile :: get_new(){
	}

	SlottedPage* HeapFile :: get(BlockID block_id){

	}
	void HeapFile :: put(DbBlock* block){
	
	}
	BlockIDs* HeapFile :: block_ids(){
	
	}
	/*uint32_t HeapFile :: get_last_block_id(){
	    return last;
        }*/
   }
	



//HeapTable Section now

//Identifier == string as a typedef
//ColumnNames == vector of Identifiers
//ColumnAttributes == vector of ColumnAttribute, which is a class that holds an int and text

HeapTable :: HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes)
  : DbRelation(table_name, column_names, column_attributes){

  this->file = new HeapFile(table_name);

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
    this->file.create();
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

  open();
  return append(validate(row));
}




//HeapTable implementation for Milestone2 requires: create, create_if_not_exists, open, close, and drop + insert

void HeapTable :: update(const Handle handle, const ValueDict* new_values){

}

void HeapTable :: del(const Handle handle){

}

Handles* HeapTable :: select(){
  Handles* handles = new Handles();
  BlockIDs* block_ids = file.block_ids();
  for (auto const& block_id : *block_ids){    
    SlottedPage* block = file.get(block_id);
    RecordIDs* record_ids = block->ids();
    for (auto const& record_id: *record_ids)
      handles->push_back(Handle(block_id, record_id));
    delete record_ids;
    delete block;
  }
  delete block_ids;
  return handles;
}


Handles* HeapTable :: select(const ValueDict* where){
  Handles* handles = new Handles();
  BlockIDs* block_ids = file.block_ids();
  for (auto const& block_id: *block_ids) {
    SlottedPage* block = file.get(block_id);
    RecordIDs* record_ids = block->ids();
    for (auto const& record_id : *record_ids)
      handles->push_back(Handle(block_id, record_id));
    delete record_ids;
    delete block;
  }
  delete block_ids;
  return handles;
}


ValueDict* HeapTable :: project(Handle handle){

  return NULL;
}

ValueDict* HeapTable :: project(Handle handle, const ColumnNames* column_names){

  ValueDict* returnRow = new ValueDict();
  return returnRow;
}

ValueDict* HeapTable :: validate(const ValueDict* row){


  //ValueDict == map of <Identifier, value> where identifier is a string

  uint16_t col_num = 0;
  ValueDict* returnRow = {};
  
  for(auto const& column_name : this->column_names){
    ColumnAttribute ca = this->column_attributes[col_num++];
    ValueDict::const_iterator column = row->find(column_name);
    //match
    if(column == row->end()){
      throw DbRelationError("Error: not fully functioning yet");
    }
    else{
      Value value = row->at(column_name);
      returnRow->at(column_name) = value;
    }
  }
  
    
  return returnRow;
}

Handle HeapTable :: append(const ValueDict* row){

  Handle handle;
  RecordID record_id;

  Dbt* data = marshal(row);

  uint32_t last_id = this->file.get_last_block_id();
  SlottedPage* chosenBlock = this->file.get(last_id);



  try{
    record_id = chosenBlock->add(data);
  }
  catch(exception &e){

    chosenBlock = file.get_new();
    record_id = chosenBlock->add(data);

  }


  
  file.put(chosenBlock);

  //Is this correct?
  
  handle.first = file.get_last_block_id();
  handle.second = record_id;

  return handle;
  
  
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

  return NULL;
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
  if (value.n != 12)
    return false;
  value = (*result)["b"];
  if (value.s != "Hello!")
    return false;
  table.drop();

  return true;
}






 
