#ifndef PICCOLO_TABLE_H
#define PICCOLO_TABLE_H

#include "util/common.h"
#include "util/file.h"
#include "util/marshal.h"

#include <boost/smart_ptr.hpp>
#include <boost/thread.hpp>

#include "piccolo.pb.h"

namespace piccolo {

class Table;
class TableHelper;
class TableData;

static const int kReadAhead = 1024;
static const int kWriteFlushCount = 1000000;

typedef boost::function<Table* (void)> TableCreator;

struct AccumulatorBase {
};

struct SharderBase {
};

// Each table is associated with a single accumulator.  Accumulators are
// applied whenever an update is supplied for an existing key-value cell.
template<class V>
struct Accumulator: public AccumulatorBase {
  virtual void Accumulate(V* a, const V& b) = 0;
};

template<class K>
struct Sharder: public SharderBase {
  virtual int operator()(const K& k, int shards) = 0;
};

// Commonly used accumulation and sharding operators.
template<class V>
struct Accumulators {
  struct Min: public Accumulator<V> {
    void Accumulate(V* a, const V& b) {
      *a = std::min(*a, b);
    }
  };

  struct Max: public Accumulator<V> {
    void Accumulate(V* a, const V& b) {
      *a = std::max(*a, b);
    }
  };

  struct Sum: public Accumulator<V> {
    void Accumulate(V* a, const V& b) {
      *a = *a + b;
    }
  };

  struct Replace: public Accumulator<V> {
    void Accumulate(V* a, const V& b) {
      *a = b;
    }
  };
};

struct Sharding {
  struct String: public Sharder<string> {
    int operator()(const string& k, int shards) {
      return StringPiece(k).hash() % shards;
    }
  };

  struct Mod: public Sharder<int> {
    int operator()(const int& key, int shards) {
      return key % shards;
    }
  };

  struct UintMod: public Sharder<uint32_t> {
    int operator()(const uint32_t& key, int shards) {
      return key % shards;
    }
  };
};

struct TableCoder {
  virtual void write(StringPiece key, StringPiece value) = 0;
  virtual bool read(string* key, string* value) = 0;
};

struct TableIterator {
  virtual void keyStr(string *out) = 0;
  virtual void valueStr(string *out) = 0;
  virtual bool done() = 0;
  virtual void Next() = 0;

  virtual string keyStr() {
    string out;
    keyStr(&out);
    return out;
  }

  virtual string valueStr() {
    string out;
    valueStr(&out);
    return out;
  }
};

struct Table {
  int id;
  int numShards;

  AccumulatorBase* accumulator;
  MarshalBase* keyMarshal;
  MarshalBase* valueMarshal;
  SharderBase* sharder;

  virtual bool containsStr(const StringPiece& k) = 0;
  virtual string getStr(const StringPiece &k) = 0;
  virtual void updateStr(const StringPiece &k, const StringPiece &v) = 0;

  virtual void clear() = 0;
  virtual bool empty() = 0;
  virtual void reserve(int64_t new_size) = 0;
  virtual void swap(Table*) = 0;

  virtual int64_t size() = 0;

  virtual TableIterator* iterator() = 0;
};

template<class K, class V>
struct TableIteratorT: public TableIterator {
  virtual const K& key() = 0;
  virtual V& value() = 0;

  void keyStr(string *out) {
    marshal<K>(key(), out);
  }
  void valueStr(string *out) {
    marshal<V>(value(), out);
  }
protected:
};

// Key/value typed interface.
template<class K, class V>
class TableT: public Table {
public:
  typedef TableIteratorT<K, V> TypedIter;
  virtual bool contains(const K &k) = 0;

  V get(const K& k, const V& defValue) {
    if (contains(k)) {
      return get(k);
    }
    return defValue;
  }

  virtual V get(const K &k) = 0;
  virtual void put(const K &k, const V &v) = 0;
  virtual void update(const K &k, const V &v) = 0;
  virtual void remove(const K &k) = 0;
  virtual TypedIter* typedIterator() = 0;

  template<void (*MapFunction)(const K&, V&)>
  void map() {

  }

  template<void (*RunFunction)(TableT<K, V>*, int)>
  void run() {

  }

  template<class KernelClass, void (KernelClass::*)(TableT<K, V>*, int)>
  void runKernel() {

  }
protected:
};

class ShardedTable: public Table {
public:
  virtual ~ShardedTable();

  Table* partition(int shard);
  ShardInfo* partitionInfo(int shard);
  void updatePartitions(const ShardInfo& sinfo);
  int64_t shardSize(int shard);

  bool isLocalShard(int shard);
  bool isLocalKey(const StringPiece &k);
  int workerForShard(int shard);

  virtual int shardForKeyStr(StringPiece) = 0;

  void setHelper(TableHelper* h) {
    worker_ = h;
  }

protected:
  friend class Worker;
  friend class Master;

  boost::recursive_mutex mutex_;
  std::vector<ShardInfo> partInfo_;
  std::vector<Table*> partitions_;
  int workerId_;
  int pendingWrites_;
  TableHelper* worker_;
};

template<class K, class V>
class ProxyTableT: public TableT<K, V> {
public:

};

class TableRegistry: private boost::noncopyable {
private:
  TableRegistry();
public:
  typedef std::map<int, ShardedTable*> Map;
  static Map tables;

  static ShardedTable* table(int id) {
    return tables[id];
  }

  template<class K, class V>
  static TableT<K, V>* sparse(int numShards, Sharder<K>* sharding,
      Accumulator<V>* accum);
private:
};

}

#endif // PICCOLO_TABLE_H
