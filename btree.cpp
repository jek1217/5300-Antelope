#include "btree.h"

#include <sstream> //FIXME

BTreeIndex::BTreeIndex(DbRelation& relation, Identifier name, ColumnNames key_columns, bool unique)
        : DbIndex(relation, name, key_columns, unique),
          closed(true),
          stat(nullptr),
          root(nullptr),
          file(relation.get_table_name() + "-" + name),
          key_profile() {
    if (!unique)
        throw DbRelationError("BTree index must have unique key");
	// FIXME - what else?!
}

BTreeIndex::~BTreeIndex() {
	// FIXME - free up stuff
        delete stat;
        delete root;
        
}

// Create the index.
void BTreeIndex::create() {
        /*
        """ Create the index. """
        self.file.create()
        self.stat = _BTreeStat(self.file, self.STAT, new_root=self.STAT + 1, key_profile=self.key_profile)
        self.root = _BTreeLeaf(self.file, self.stat.root_id, self.key_profile, create=True)
        self.closed = False

        # now build the index! -- add every row from relation into index
        self.file.begin_write()
        for handle in self.relation.select():
            self.insert(handle)
        self.file.end_write()
        */
        
        
        
        this->file.create();
        this->stat = new BTreeStat(this->file, BTreeIndex::STAT, BTreeIndex::STAT + 1, this->key_profile);
                //(HeapFile &file, BlockID stat_id, BlockID new_root, const KeyProfile& key_profile)
        this->root = new BTreeLeaf(this->file, this->stat->get_root_id(), this->key_profile, true);
                //(HeapFile &file, BlockID block_id, const KeyProfile& key_profile, bool create);
        this->closed = false;
        
        
        //build the index, add every row from relation into index
        //this->file.begin_write();
        //don't need to lock...
        Handles* handles = this->relation.select();
        
        
        for(u_int i = 0; i < handles->size(); i++) {
                this->insert(handles->at(i));
        }
        
        //throw DbRelationError("CREATE BTREE INDEX"); reaches this WORKING...

}

// Drop the index.
void BTreeIndex::drop() {
        /*
        """ Drop the index. """
        self.file.delete()
        */
        
        this->file.drop();
        
	// WORKING...
        
        
        
}

// Open existing index. Enables: lookup, range, insert, delete, update.
void BTreeIndex::open() {
        
        /*
        """ Open existing index. Enables: lookup, [range if supported], insert, delete, update. """
        if self.closed:
            self.file.open()
            self.stat = _BTreeStat(self.file, self.STAT)
            if self.stat.height == 1:
                self.root = _BTreeLeaf(self.file, self.stat.root_id, self.key_profile)
            else:
                self.root = _BTreeInterior(self.file, self.stat.root_id, self.key_profile)
            self.closed = False
            */
        
        if(this->closed) {
                this->file.open();
                this->stat = new BTreeStat(this->file, BTreeIndex::STAT, this->key_profile); //fixme?
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
        /*
         """ Closes the index. Disables: lookup, [range if supported], insert, delete, update. """
        self.file.close()
        self.stat = self.root = None
        self.closed = True
        */
        
        this->file.close();
        this->stat = nullptr;
        this->root = nullptr;
        this->closed = true;
        
        
        throw DbRelationError("CLOSE BTREE INDEX COMPLETE");

}

// Find all the rows whose columns are equal to key. Assumes key is a dictionary whose keys are the column
// names in the index. Returns a list of row handles.
Handles* BTreeIndex::lookup(ValueDict* key_dict) const {
        
        
        Handles* ret = this->_lookup(this->root, this->stat->get_height(), this->tkey(key_dict));
        //throw DbRelationError("DONE WITH LOOKUP...");
        return ret;
        
        
}

Handles* BTreeIndex::_lookup(BTreeNode *node, uint height, const KeyValue* key) const {
        
        /*

        def _lookup(self, node, depth, tkey):
                """ Recursive lookup. """
                if isinstance(node, _BTreeLeaf):  # base case: a leaf node
                    handle = node.find_eq(tkey)
                    return [handle] if handle is not None else []
                else:
                    return self._lookup(node.find(tkey, depth), depth - 1, tkey)  # recursive case: go down one level

        */

        
        Handles* ret = new Handles;
        BTreeLeaf* btl = nullptr;
        BTreeInterior* bti = nullptr;
        if(dynamic_cast<BTreeLeaf*>(node)) {
                printf("LOOKING THRU LEAF NODE\n");
                btl = (BTreeLeaf*)node;
                
                for(u_int i = 0; i < key->size(); i++) {
                        std::stringstream ss;
                        ss << "key value: " << key->at(i).n << "\n";
                        printf(ss.str().c_str());
                }
                try { ret->push_back(btl->find_eq(key)); }
                catch (...) { return ret; }
                //throw DbRelationError("LOOKUP BTREE LEAF NODE COMPLETE");
                return ret;
                        
        }
        else {
                bti = (BTreeInterior*)node;
                printf("LOOKING THRU INTERIOR NODE\n");
                return this->_lookup(bti->find(key, height), height - 1, key); //added this->
        }
}

        
        

Handles* BTreeIndex::range(ValueDict* min_key, ValueDict* max_key) const {
    throw DbRelationError("Don't know how to do a range query on Btree index yet");
}

// Insert a row with the given handle. Row must exist in relation already.
void BTreeIndex::insert(Handle handle) {
        
        /*
        """ Insert a row with the given handle. Row must exist in relation already. """
        tkey = self._tkey(self.relation.project(handle, self.key))

        split_root = self._insert(self.root, self.stat.height, tkey, handle)

        # if we split the root grow the tree up one level
        if split_root is not None:
            rroot, boundary = split_root
            root = _BTreeInterior(self.file, 0, self.key_profile, create=True)
            root.first = self.root.id
            root.insert(boundary, rroot.id)
            root.save()
            self.stat.root_id = root.id
            self.stat.height += 1
            self.stat.save()
            self.root = root
        */
        
        
        KeyValue* tkey = this->tkey(this->relation.project(handle, &this->key_columns));
        
        Insertion split_root = this->_insert(this->root, this->stat->get_height(), tkey, handle); 
        
        
        if(!BTreeNode::insertion_is_none(split_root)) {
                BTreeInterior* root = new BTreeInterior(this->file, 0, this->key_profile, true);
                root->set_first(this->root->get_id());
                root->insert(&split_root.second, split_root.first);
                root->save();
                this->stat->set_root_id(root->get_id());
                this->stat->set_height(this->stat->get_height() + 1);
                this->stat->save();
                this->root = root;
                printf("SPLIT THE ROOT\n");
        }
        
        //throw DbRelationError("BOTTOM OF INSERT");
	// FIXME
}

Insertion BTreeIndex::_insert(BTreeNode *node, uint height, const KeyValue* key, Handle handle) {
        
        /*
        """ Recursive insert. If a split happens at this level, return the (new node, boundary) of the split. """
        if isinstance(node, _BTreeLeaf):  # base case: a leaf node
            try:
                node.insert(tkey, handle)
                node.save()
                return None
            except ValueError:
                return self._split_leaf(node, tkey, handle)
        else:
            new_kid = self._insert(node.find(tkey, depth), depth - 1, tkey, handle)  # recursive case
            if new_kid is not None:
                nnode, boundary = new_kid
                try:
                    node.insert(boundary, nnode.id)
                    node.save()
                    return None
                except ValueError:
                    return self._split_node(node, boundary, nnode.id)
        */
        
        //test inserts
        //create table foo (a int, b text);create index fooa on foo (a);create index fooab on foo (a,b)
        //insert into foo (a, b) values (1, "one")
        
        BTreeLeaf* btl = nullptr;
        BTreeInterior* bti = nullptr;
        Insertion ret;
        if(dynamic_cast<BTreeLeaf*>(node)) {
                printf("IN LEAF NODE _insert!\n");
                btl = (BTreeLeaf*)node;
                ret = btl->insert(key, handle);
                if(BTreeNode::insertion_is_none(ret))
                        btl->save();
                else
                        printf("LEAF INSERTION IS NOT NONE!\n");
                return ret;
        }
        else {
                bti = (BTreeInterior*)node;
                printf("IN INTERIOR NODE _insert!\n");
                Insertion new_kid = this->_insert(bti->find(key, height), height - 1, key, handle);
                if(!BTreeNode::insertion_is_none(new_kid)) {
                        printf("INTERIOR INSERTION IS NOT NONE\n");
                        ret = bti->insert(&new_kid.second, new_kid.first); //fixme?
                        if(BTreeNode::insertion_is_none(ret))
                                bti->save();
                        return ret;
                }
                
                else
                        return BTreeNode::insertion_none();
        }
        
        
}



void BTreeIndex::del(Handle handle) {
    throw DbRelationError("Don't know how to delete from a BTree index yet");
}

KeyValue *BTreeIndex::tkey(const ValueDict *key) const {
        
        /*
        """ Transform a key dictionary into a tuple in the correct order. """
        return tuple(key[column_name] for column_name in self.key)
        */
        
        //column name, value
        
        //from a valuedict* to a to a vector<Values>*
        

        KeyValue* kv = new KeyValue;
        
        for(u_int i = 0; i < this->key_columns.size(); i++) {
                //printf("before");
                kv->push_back(key->at(key_columns[i]));
                //printf("after");
        }
        
        //throw DbRelationError("in tkey"); //works
        
	return kv;
}

bool test_btree() {
        /*
        
        class TestBTreeIndex(unittest.TestCase):
    def setUp(self):
        dbenv = os.path.expanduser('~/.dbtests')
        if not os.path.exists(dbenv):
            os.makedirs(dbenv)
        for file in os.listdir(dbenv):
            os.remove(os.path.join(dbenv, file))
        initialize(dbenv)

    def testHashIndex(self):
        table = HeapTable('foo', ['a', 'b'], {'a': {'data_type': 'INT'}, 'b': {'data_type': 'INT'}})
        table.create()
        row1 = {'a': 12, 'b': 99}
        row2 = {'a': 88, 'b': 101}
        table.insert(row1)
        table.insert(row2)
        for i in range(1000):
            row = {'a': i+100, 'b': -i}
            table.insert(row)
        index = BTreeIndex(table, 'fooindex', ['a'], unique=True)
        index.create()
        result = [table.project(handle) for handle in index.lookup({'a': 12})]
        self.assertEqual(result, [row1])
        result = [table.project(handle) for handle in index.lookup({'a': 88})]
        self.assertEqual(result, [row2])
        result = [table.project(handle) for handle in index.lookup({'a': 6})]
        self.assertEqual(result, [])

        for j in range(10):
            for i in range(1000):
                result = [table.project(handle) for handle in index.lookup({'a': i+100})]
                self.assertEqual(result, [{'a': i+100, 'b': -i}])

        # FIXME: other things to test: delete, multiple keys
        
        */
        
        //HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes ) :
        
        ColumnAttribute t = ColumnAttribute(ColumnAttribute::DataType::TEXT);
        ColumnAttribute i = ColumnAttribute(ColumnAttribute::DataType::INT);
        ColumnNames cn;
        cn.push_back("a");
        cn.push_back("b");
        ColumnAttributes ca;
        ca.push_back(i);
        ca.push_back(i);
        HeapTable table("foo", cn, ca);
        table.create();
        ValueDict row1;
        ValueDict row2;
        row1["a"] = Value(12);
        row1["b"] = Value(99);
        row2["a"] = Value(88);
        row2["b"] = Value(101);
        table.insert(&row1);
        table.insert(&row2);
        
        
        for (int i = 0; i < 300; i++) {
                ValueDict row;
                row["a"] = Value(i+100);
                row["b"] = Value(-1 * i);
                table.insert(&row);
                
        }
        
        
        
        /*
        index = BTreeIndex(table, 'fooindex', ['a'], unique=True)
        index.create()
        result = [table.project(handle) for handle in index.lookup({'a': 12})]
        self.assertEqual(result, [row1])
        result = [table.project(handle) for handle in index.lookup({'a': 88})]
        self.assertEqual(result, [row2])
        result = [table.project(handle) for handle in index.lookup({'a': 6})]
        self.assertEqual(result, [])

        for j in range(10):
            for i in range(1000):
                result = [table.project(handle) for handle in index.lookup({'a': i+100})]
                self.assertEqual(result, [{'a': i+100, 'b': -i}])

        # FIXME: other things to test: delete, multiple keys
        
        */
        ColumnNames kc;
        kc.push_back("a");
        BTreeIndex* bt = new BTreeIndex(table, "fooindex", kc, true);
        bt->create();
        
        Handles* handles = nullptr;
        ValueDict kd1;
        kd1["a"] = 12;
        ValueDict kd2;
        kd2["a"] = 88;
        ValueDict kd3;
        kd3["a"] = 6;
        
        std::stringstream ss;
        
        handles = bt->lookup(&kd1);
        ss << "handles size: " << handles->size() << "\n";
        //printf(ss.str().c_str());
        handles = bt->lookup(&kd2);
        ss << "handles size: " << handles->size() << "\n";
        //printf(ss.str().c_str());
        handles = bt->lookup(&kd3);
        ss << "handles size: " << handles->size() << "\n";
        printf(ss.str().c_str());
        
        
        printf("done with btree index create\n");
        if(handles == nullptr)
                printf("handles still null\n");
        /*
        for (u_int i = 0; i < handles->size(); i++) {
                //ValueDict* result = table.project(handles->at(i));
                std::stringstream ss;
                printf("here\n");
                //ss << "a: " << result->at("a").n;// << " b: " << result->at("b").n << "\n";
                //printf(ss.str().c_str());

        }
        */
        delete bt;
        
        return true;
        
}


