#ifndef PICCOLO_TABLE_H
#define PICCOLO_TABLE_H

#include "util/common.h"
#include "util/file.h"
#include "util/marshal.h"
#include <boost/thread.hpp>

#include "piccolo.pb.h"

namespace piccolo {

class TableHelper;
class TableData;

static const int kReadAhead = 1024;
static const int kWriteFlushCount = 1000000;

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

struct ProtoTableCoder : public TableCoder {
  ProtoTableCoder(TableData*);
  void write(StringPiece key, StringPiece value);
  bool read(string* key, string* value);
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
  int shard;
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
protected:
};

struct PartitionInfo {
  PartitionInfo() :
      dirty(false), tainted(false) {
  }
  bool dirty;
  bool tainted;
  ShardInfo sinfo;
};

class ShardedTable: public Table {
public:
  virtual ~ShardedTable();
  virtual void updatePartitions(const ShardInfo& sinfo);
  virtual TableIterator* iterator(int shard);

  virtual bool isLocalShard(int shard);
  virtual bool isLocalKey(const StringPiece &k);
  virtual int workerForShard(int shard);

  virtual PartitionInfo* partitionInfo(int shard);
  virtual Table* partition(int shard);

  virtual bool tainted(int shard);

  // Handle updates from the master or other workers.
  virtual void sendUpdates();
  virtual void applyUpdates(const TableData& req);
  virtual void handlePutRequests();

  // Exchange the content of this table with that of table 'b'.
  virtual void swap(ShardedTable *b);
  virtual void clear();

  virtual int shardForKeyStr(StringPiece);
  virtual int64_t pendingWriteBytes();


  void setHelper(TableHelper* h) {
    worker_ = h;
  }

protected:
  friend class Worker;
  friend class Master;

  // Fill in a response from a remote worker for the given key.
  virtual void handleGetRequest(const HashGet& req, TableData* resp);
  virtual int64_t shardSize(int shard);
  virtual void swapLocal(ShardedTable *b);

  boost::recursive_mutex mutex_;
  std::vector<PartitionInfo> partInfo_;
  std::vector<Table*> partitions_;
  int workerId_;
  int pendingWrites_;
  TableHelper* worker_;
};

template<class K, class V>
class RemoteIterator: public TableIteratorT<K, V> {
public:
  RemoteIterator(ShardedTable* table, int shard);
  void keyStr(string* out);
  void valueStr(string* out);
  bool done();
  void Next();

  const K& key();
  V& value();

private:
  ShardedTable* owner_;
  IteratorRequest request_;
  IteratorResponse response_;

  int pos_;
  int shard_;
  K key_;
  V value_;
  bool done_;
};

class TableRegistry: private boost::noncopyable {
private:
  TableRegistry();
public:
  typedef std::map<int, ShardedTable*> Map;

  static Map& tables() {
    return getInstance()->tmap_;
  }

  static ShardedTable* table(int id) {
    return getInstance()->tmap_[id];
  }

  template<class K, class V>
  static TableT<K, V>* sparse(int numShards, Sharder<K>* sharding,
      Accumulator<V>* accum) {
    return static_cast<TableT<K, V>*>(getInstance()->_sparse(numShards,
        sharding, accum));
  }

private:
  static TableRegistry* getInstance();
  Table* _sparse(int numShards, SharderBase* sharder, AccumulatorBase* accum);

  Map tmap_;
};

}

#endif // PICCOLO_TABLE_H
