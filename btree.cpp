#include "btree.h"

BTreeIndex::BTreeIndex(DbRelation& relation, Identifier name, ColumnNames key_columns, bool unique)
    : DbIndex(relation, name, key_columns, unique),
    closed(true),
    stat(nullptr),
    root(nullptr),
    file(relation.get_table_name() + "-" + name),
    key_profile() {
        if (!unique)
            throw DbRelationError("BTree index must have unique key");
        build_key_profile();
    }

BTreeIndex::~BTreeIndex() {
        delete stat;
        delete root;
}

// Create the index.
void BTreeIndex::create() {
        
        this->file.create();
        this->stat = new BTreeStat(this->file, BTreeIndex::STAT, BTreeIndex::STAT + 1, this->key_profile);
        this->root = new BTreeLeaf(this->file, this->stat->get_root_id(), this->key_profile, true);
        this->closed = false;
        Handles* handles = this->relation.select();
        
        for(u_int i = 0; i < handles->size(); i++) {
                this->insert(handles->at(i));
        }
        delete handles;
}

// Drop the index.
void BTreeIndex::drop() {
    this->file.drop();
}

// Open existing index. Enables: lookup, range, insert, delete, update.
void BTreeIndex::open() {
    
    if(this->closed) {
        this->file.open();
        this->stat = new BTreeStat(this->file, BTreeIndex::STAT, this->key_profile);
        if(this->stat->get_height() == 1)
            this->root = new BTreeLeaf(this->file, 
                                           this->stat->get_root_id(), this->key_profile, false);
        else
            this->root = new BTreeInterior(this->file, 
                                               this->stat->get_root_id(), this->key_profile, false);
        this->closed = false;
    } 
}

// Closes the index. Disables: lookup, range, insert, delete, update.
void BTreeIndex::close() {

    this->file.close();
    delete this->stat;
    this->stat = nullptr;
    delete this->root;
    this->root = nullptr;
    this->closed = true;
}

// Find all the rows whose columns are equal to key. Assumes key is a dictionary whose keys are the column
// names in the index. Returns a list of row handles.
Handles* BTreeIndex::lookup(ValueDict* key_dict) const {
    Handles* ret = this->_lookup(this->root, this->stat->get_height(), this->tkey(key_dict));
    return ret;
}

Handles* BTreeIndex::_lookup(BTreeNode *node, uint height, const KeyValue* key) const {
    Handles* ret = new Handles;
    BTreeLeaf* btl = nullptr;
    BTreeInterior* bti = nullptr;
    if(dynamic_cast<BTreeLeaf*>(node)) {
        btl = (BTreeLeaf*)node;
        try { ret->push_back(btl->find_eq(key)); }
        catch (...) { return ret; }
        return ret;

    }
    else {
        bti = (BTreeInterior*)node;
        return this->_lookup(bti->find(key, height), height - 1, key);
    }
}
        

Handles* BTreeIndex::range(ValueDict* min_key, ValueDict* max_key) const {
    throw DbRelationError("Don't know how to do a range query on Btree index yet");
}


void BTreeIndex::insert(Handle handle) {

    this->open();
    KeyValue* tkey = this->tkey(this->relation.project(handle));

    Insertion split_root = this->_insert(this->root, this->stat->get_height(), tkey, handle);

    if(!BTreeNode::insertion_is_none(split_root)) {
        BTreeInterior* nroot = new BTreeInterior(this->file, 0, this->key_profile, true);
        nroot->set_first(this->root->get_id());
        nroot->insert(&split_root.second, split_root.first);
        nroot->save();
        this->stat->set_root_id(nroot->get_id());
        this->stat->set_height(this->stat->get_height() + 1);
        this->stat->save();
        delete this->root;
        this->root = nroot;
    }
    delete tkey;
}

Insertion BTreeIndex::_insert(BTreeNode *node, uint height, const KeyValue* key, Handle handle) {
        
    //test inserts
    //create table foon (a int, b text);create index fooan on foon (a);create index fooab on foo (a,b)
    //insert into foon (a, b) values (1, "one")
        
    BTreeLeaf* btl = nullptr;
    BTreeInterior* bti = nullptr;
    Insertion ret;
    if(dynamic_cast<BTreeLeaf*>(node)) {
        btl = (BTreeLeaf*)node;
        ret = btl->insert(key, handle);
        btl->save();
        return ret;
    }
    else {
        bti = (BTreeInterior*)node;
        Insertion new_kid = this->_insert(bti->find(key, height), height - 1, key, handle);
        if(!BTreeNode::insertion_is_none(new_kid)) {
            ret = bti->insert(&new_kid.second, new_kid.first);
        }
        return ret;
    }
}

void BTreeIndex::del(Handle handle) {
    throw DbRelationError("Don't know how to delete from a BTree index yet");
}

KeyValue *BTreeIndex::tkey(const ValueDict *key) const {
        
    KeyValue* kv = new KeyValue;

    for(u_int i = 0; i < this->key_columns.size(); i++) {
        kv->push_back(key->at(key_columns[i]));
    }
	return kv;
}

void BTreeIndex::build_key_profile() {
    ColumnNames cn = this->relation.get_column_names();
    ColumnAttributes ca = this->relation.get_column_attributes();
    std::map<Identifier,ColumnAttribute> mp;

    for(u_int i = 0; i < cn.size(); i++) {
        mp[cn[i]] = ca[i];
    }

    KeyProfile kp;
    for(u_int i = 0; i < this->key_columns.size(); i++) {
        kp.push_back(mp[this->key_columns[i]].get_data_type());
    }
    this->key_profile = kp;
}

bool test_btree() {
        
    ColumnAttribute t = ColumnAttribute(ColumnAttribute::DataType::TEXT);
    ColumnAttribute i = ColumnAttribute(ColumnAttribute::DataType::INT);
    ColumnNames cn;
    cn.push_back("a");
    cn.push_back("b");
    ColumnAttributes ca;
    ca.push_back(i);
    ca.push_back(i);
    HeapTable table("goober", cn, ca);
    table.create();
    ValueDict row1;
    ValueDict row2;
    row1["a"] = Value(12);
    row1["b"] = Value(99);
    row2["a"] = Value(88);
    row2["b"] = Value(101);
    table.insert(&row1);
    table.insert(&row2);
    for (int i = 0; i < 10000; i++) {
        ValueDict row;
        row["a"] = Value(i+100);
        row["b"] = Value(-1 * i);
        table.insert(&row);
    }
    std::cout << "over 10000 successful inserts\n";
    
    ColumnNames kc;
    kc.push_back("a");
    BTreeIndex* bt = new BTreeIndex(table, "goober_index", kc, true);
    bt->create();
    printf("btree index create ok\n");
    ValueDict kd1;
    kd1["a"] = 12;
    ValueDict kd2;
    kd2["a"] = 88;
    ValueDict kd3;
    kd3["a"] = 6;

    ValueDict* result = table.project(bt->lookup(&kd1)->at(0)); //expect 1 handle
    std::cout << "Should be 12: " << result->at("a").n << std::endl;
    if(result->at("a").n != 12)
        return false;
    delete result;

    result = table.project(bt->lookup(&kd2)->at(0)); //expect 1 handle
    std::cout << "Should be 88: " << result->at("a").n << std::endl;  
    if(result->at("a").n != 88)
        return false;
    delete result;

    std::cout << "Handle size for no key found should be 0: Handle size: " << bt->lookup(&kd3)->size() << std::endl;
    if(bt->lookup(&kd3)->size() != 0)
        return false;

    for(u_int j = 0; j < 10; j++) {
        for(int i = 0; i < 1000; i++) {
            ValueDict q;
            q["a"] = i+100;
            result = table.project(bt->lookup(&q)->at(0)); //expect 1 handle
            if(result->at("b").n != -i)
                return false;
            delete result;
        }
    }
    std::cout << "many selects/projects/and lookups ok\n";
    bt->drop();
    std::cout << "drop index ok\n";
    delete bt;
    return true;
}
