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

template<class K, class V, class Base>
class UntypedMixin: public Base {
public:
  // Default specialization for untyped methods
  virtual bool containsStr(const StringPiece& s) {
    return contains(unmarshal<K>(s));
  }

  virtual string getStr(const StringPiece &s) {
    return marshal(get(unmarshal<K>(s)));
  }

  virtual void updateStr(const StringPiece& kstr, const StringPiece &vstr) {
    update(unmarshal<K>(kstr), unmarshal<V>(vstr));
  }

  virtual TableIterator* iterator() {
    return this->typedIterator();
  }
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

// Adds TableT<> to an underlying table.
template<class K, class V, class Parent>
class TableTMixin: public TableT<K, V>, public Parent {
public:
  void put(const K &k, const V &v) {
    putStr(marshal<K>(k), marshal<V>(v));
  }

  void update(const K &k, const V &v) {
    updateStr(marshal<K>(k), marshal<V>(v));
  }

  // Return the value associated with 'k', possibly blocking for a remote fetch.
  V get(const K &k) {
    return unmarshal<V>(getStr(k));
  }

  bool contains(const K &k) {
    return containsStr(marshal<K>(k));
  }

  TableIteratorT<K, V>* typedIterator() {
    return new TableIteratorTMixin<K, V>(this->iterator());
  }
};

template<class K, class V>
class RemoteTableT: public TableTMixin<K, V, RemoteTable> {
public:
  static Table* create();
};

template<class K, class V, class Base>
class ShardedTableMixin: public Base, public TableT<K, V> {
public:

  TableT<K, V>* typedP(int idx) {
    return static_cast<TableT<K, V>*>(this->partitions_[idx]);
  }

  void clear() {
    for (int i = 0; i < this->partitions_.size(); ++i) {
      typedP(i)->clear();
    }
  }

  bool empty() {
    for (int i = 0; i < this->partitions_.size(); ++i) {
      if (!typedP(i)->empty()) {
        return false;
      }
    }
    return true;
  }

  void reserve(int64_t numEntries) {
    for (int i = 0; i < this->partitions_.size(); ++i) {
      typedP(i)->reserve(numEntries / this->partitions_.size());
    }
  }

  int64_t size() {
    int64_t s = 0;
    for (int i = 0; i < this->partitions_.size(); ++i) {
      s += typedP(i)->size();
    }
    return s;
  }

  int64_t shardForKey(const K& k) {
    return -1;
  }

  void put(const K& k, const V& v) {
    typedP(shardForKey(k))->put(k, v);
  }

  void update(const K& k, const V& v) {
    typedP(shardForKey(k))->update(k, v);
  }

  bool contains(const K& k) {
    return typedP(shardForKey(k))->contains(k);
  }

  V get(const K& k) {
    return typedP(shardForKey(k))->get(k);
  }

  int shardForKeyStr(StringPiece sp) {
    return shardForKey(unmarshal<K>(sp));
  }

  void remove(const K& k) {
    return typedP(shardForKey(k))->remove(k);
  }

  void swap(Table *other) {
    LOG(FATAL) << "Not implemented, man.";
  }

  TableIteratorT<K, V>* typedIterator() {
    return NULL;
  }
};

template<class K, class V>
class ShardedTableT: public UntypedMixin<K, V,
    ShardedTableMixin<K, V, ShardedTable> > {
public:
};

template<class K, class V>
TableT<K, V>* TableRegistry::sparse(int numShards, Sharder<K>* sharding,
    Accumulator<V>* accum) {
  int tableId = tables.size();
  ShardedTableT<K, V>* out = new ShardedTableT<K, V>();
  tables[tableId] = out;
  return out;
}

}

#endif /* TABLE_INL_H_ */
