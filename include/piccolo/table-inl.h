#ifndef TABLE_INL_H_
#define TABLE_INL_H_

#include <algorithm>

#include "piccolo/table.h"
#include "piccolo/sparse-table.h"

#include "util/common.h"
#include "util/rpc.h"
#include "util/timer.h"

namespace piccolo {

class ProtoTableCoder: public TableCoder {
private:
  TableData* t_;
  int pos_;
public:
  ProtoTableCoder(TableData*);
  void write(StringPiece key, StringPiece value);
  bool read(string* key, string* value);
};

class RemoteIterator: public TableIterator {
public:
  RemoteIterator(ShardedTable* table, int shard);
  void keyStr(string* out);
  void valueStr(string* out);
  bool done();
  void Next();

private:
  ShardedTable* owner_;
  IteratorRequest request_;
  IteratorResponse response_;

  int pos_;
  int shard_;
  bool done_;
};

class RemoteTable: public Table {
public:
};

template<class T, class K, class V>
class ShardedTableImpl: public ShardedTable, public TableT<K, V> {
private:
  T* cast() {
    return static_cast<T*>(this);
  }

public:
  ~ShardedTableImpl() {
    for (int i = 0; i < shards_.size(); ++i) {
      delete shards_[i];
    }
  }

  void updateShards(const ShardInfo& info) {
    shardInfo_[info.shard()].CopyFrom(info);

  }

  int64_t shardSize(int shard) {
    return shardInfo_[shard].entries();
  }

  bool isLocalShard(int shard) {
    return workerForShard(shard) == workerId_;
  }

  bool isLocalKey(const StringPiece &k) {
    return workerId_ == shardForKeyStr(k);
  }

  Table* shard(int shard) {
    return shards_[shard];
  }

  ShardInfo* shardInfo(int shard) {
    return &shardInfo_[shard];
  }

  int workerForShard(int shard) {
    return shardInfo_[shard].owner();
  }
protected:
  std::vector<ShardInfo> shardInfo_;
  std::vector<Table*> shards_;
  int32_t workerId_;
};

template<class K, class V>
class TableIteratorTMixin: public TableIteratorT<K, V> {
private:
  TableIterator *iter_;
  K key_;
  V value_;
public:

  TableIteratorTMixin(TableIterator *t) :
      iter_(t) {
  }

  const K& key() {
    unmarshal(keyStr(), &key_);
    return key_;
  }

  const V& value() {
    unmarshal(valueStr(), &value_);
    return value_;
  }

  void keyStr(string* out) {
    return iter_->keyStr(out);
  }
  void valueStr(string* out) {
    return iter_->valueStr(out);
  }

  void Next() {
    return iter_->Next();
  }
  bool done() {
    return iter_->done();
  }
};

// Add untyped methods to a typed table.
template<class K, class V, class Base>
class UntypedTableMixin: public Base {
public:
  // Default specialization for untyped methods
  bool containsStr(const StringPiece& s) {
    return this->contains(unmarshal<K>(s));
  }

  string getStr(const StringPiece &s) {
    return marshal(this->get(unmarshal<K>(s)));
  }

  void updateStr(const StringPiece& kstr, const StringPiece &vstr) {
    this->update(unmarshal<K>(kstr), unmarshal<V>(vstr));
  }

  TableIterator* iterator() {
    return this->typedIterator();
  }
};

// Adds TableT<> to an untyped table.
template<class K, class V, class Base>
class TypedTableMixin: public Base {
public:
  void put(const K &k, const V &v) {
    this->putStr(marshal<K>(k), marshal<V>(v));
  }

  void update(const K &k, const V &v) {
    this->updateStr(marshal<K>(k), marshal<V>(v));
  }

  // Return the value associated with 'k', possibly blocking for a remote fetch.
  V get(const K &k) {
    return unmarshal<V>(this->getStr(k));
  }

  bool contains(const K &k) {
    return this->containsStr(marshal<K>(k));
  }

  TableIteratorT<K, V>* typedIterator() {
    return new TableIteratorTMixin<K, V>(this->iterator());
  }
};

template<class K, class V, class Base>
class ShardedTableBaseMixin: public Base {
private:
  TableT<K, V>* typedP(int idx) {
    return static_cast<TableT<K, V>*>(this->shard(idx));
  }
  int32_t numShards_;
  int32_t id_;
public:
  int32_t id() {
    return id_;
  }

  int32_t numShards() {
    return numShards_;
  }

  void clear() {
    for (int i = 0; i < this->shards_.size(); ++i) {
      typedP(i)->clear();
    }
  }

  bool empty() {
    for (int i = 0; i < this->shards_.size(); ++i) {
      if (!typedP(i)->empty()) {
        return false;
      }
    }
    return true;
  }

  void reserve(int64_t numEntries) {
    for (int i = 0; i < this->shards_.size(); ++i) {
      typedP(i)->reserve(numEntries / this->shards_.size());
    }
  }

  int64_t size() {
    int64_t s = 0;
    for (int i = 0; i < this->shards_.size(); ++i) {
      s += typedP(i)->size();
    }
    return s;
  }

  void swap(Table* t) {
    ShardedTable* other = dynamic_cast<ShardedTable*>(t);
    for (int i = 0; i < this->shards_.size(); ++i) {
      typedP(i)->swap(other->shard(i));
    }
  }
};

// Adds TableT<> to a sharded table.
template<class T, class K, class V, class Base>
class ShardedTableTMixin: public Base {
private:
  TableT<K, V>* getShard(const K& k) {
    int shard = static_cast<T*>(this)->shardForKey(k);
    return static_cast<TableT<K, V>*>(this->shard(shard));
  }
public:
  void put(const K& k, const V& v) {
    getShard(k)->put(k, v);
  }

  void update(const K& k, const V& v) {
    getShard(k)->update(k, v);
  }

  bool contains(const K& k) {
    return getShard(k)->contains(k);
  }

  V get(const K& k) {
    return getShard(k)->get(k);
  }

  int shardForKeyStr(StringPiece sp) {
    return static_cast<T*>(this)->shardForKey(unmarshal<K>(sp));
  }

  void remove(const K& k) {
    getShard(k)->remove(k);
  }

  TableIteratorT<K, V>* typedIterator() {
    return NULL;
  }
};

template<class K, class V>
class RemoteTableT: public TypedTableMixin<K, V, RemoteTable> {
public:
};

template<class K, class V>
class ShardedTableT: public
    ShardedTableTMixin<ShardedTableT<K, V>, K, V,
    ShardedTableBaseMixin<K, V,
    UntypedTableMixin<K, V,
    ShardedTableImpl<ShardedTableT<K, V>, K, V>>> > {
public:
  int shardForKey(const K& k) {
    return (*sharder_)(k, numShards_);
  }

  Table* createLocal() {
    return new SparseTable<K, V>;
  }

  Table* createRemote() {
    return new RemoteTableT<K, V>;
  }

  ShardedTableT(int numShards, Sharder<K>* s, Accumulator<V>* a) :
    numShards_(numShards_), sharder_(s), accum_(a) {
  }
private:
  int numShards_;
  Sharder<K>* sharder_;
  Accumulator<V>* accum_;
};

template<class K, class V>
TableT<K, V>* TableRegistry::sparse(int numShards, Sharder<K>* sharding, Accumulator<V>* accum) {
  int tableId = tables.size();
  ShardedTableT<K, V>* out = new ShardedTableT<K, V>(numShards, sharding, accum);
  tables[tableId] = out;
  return out;
}

}

#endif /* TABLE_INL_H_ */
