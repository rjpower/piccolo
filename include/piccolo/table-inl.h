#ifndef TABLE_INL_H_
#define TABLE_INL_H_

#include "piccolo/table.h"

#include "util/common.h"
#include "util/rpc.h"
#include "util/timer.h"

namespace piccolo {

template<class T, class K, class V>
class UntypedMixin: public Table {
private:
  TableT<K, V>* tt() {
    return static_cast<TableT<K, V>*>(this);
  }

  // Default specialization for untyped methods
  virtual bool containsStr(const StringPiece& s) {
    return tt()->contains(unmarshal<K>(s));
  }

  virtual string getStr(const StringPiece &s) {
    return marshal(tt()->get(unmarshal<K>(s)));
  }

  virtual void updateStr(const StringPiece& kstr, const StringPiece &vstr) {
    tt()->update(unmarshal<K>(kstr), unmarshal<V>(vstr));
  }

  virtual TableIterator* iterator() {
    return tt()->typedIterator();
  }
};

template<class K, class V>
class ShardingMixin: public TableT<K, V> {
public:
  ShardedTable* asSharded() {
    return static_cast<ShardedTable*>(this);
  }

  int shardForKey(const K& k) {
    Sharder<K> *sharder = (Sharder<K>*) (this->sharder);
    int shard = (*sharder)(k, this->numShards);
    return shard;
  }

  int shardForKeyStr(const StringPiece& k) {
    return shardForKey(unmarshal<K>(k));
  }

  // Store the given key-value pair in this hash. If 'k' has affinity for a
  // remote thread, the application occurs immediately on the local host,
  // and the update is queued for transmission to the workerForShard.
  void put(const K &k, const V &v) {
    int shard = this->shardForKey(k);

    this->partition(shard)->put(k, v);

    if (!this->isLocalShard(shard)) {
      ++this->pendingWrites_;
    }

    if (this->pendingWrites_ > kWriteFlushCount) {
      this->sendUpdates();
    }

    PERIODIC(0.1, {this->handlePutRequests();});
  }

  void update(const K &k, const V &v) {
    int shard = this->shardForKey(k);
    if (this->isLocalShard(shard)) {
      this->partition(shard)->update(k, v);
    } else {
      this->partition(shard)->update(k, v);
      ++this->pendingWrites_;
      if (this->pendingWrites_ > kWriteFlushCount) {
        this->sendUpdates();
      }
    }
    PERIODIC(0.1, {this->handlePutRequests();});
  }

  // Return the value associated with 'k', possibly blocking for a remote fetch.
  V get(const K &k) {
    int shard = this->shardForKey(k);

    // If we received a request for this shard; but we haven't received all of the
    // data for it yet. Continue reading from other workers until we do.
    while (this->tainted(shard)) {
      this->handlePutRequests();
      sched_yield();
    }

    PERIODIC(0.1, this->handlePutRequests());

    if (this->isLocalShard(shard)) {
      return this->partition(shard)->get(k);
    }

    string v_str;
    getRemote(shard, marshal(k), &v_str);
    return unmarshal<V>(v_str);
  }

  bool contains(const K &k) {
    int shard = this->shardForKey(k);

    // If we received a request for this shard; but we haven't received all of the
    // data for it yet. Continue reading from other workers until we do.
    while (this->tainted(shard)) {
      this->handlePutRequests();
      sched_yield();
    }

    if (this->isLocalShard(shard)) {
      return this->partition(shard)->contains(k);
    }

    string v_str;
    return getRemote(shard, marshal(k), &v_str);
  }

  TableIteratorT<K, V> typedIterator(int shard) {
    if (this->isLocalShard(shard)) {
      return (TableIteratorT<K, V>*) this->partitions_[shard]->iterator();
    } else {
      return new RemoteIterator<K, V>(this, shard);
    }
  }

  void remove(const K &k) {
    LOG(FATAL)<< "Not implemented!";
  }
};

template<class K, class V>
inline RemoteIterator<K, V>::RemoteIterator(ShardedTable *table, int shard) :
    owner_(table), shard_(shard), done_(false) {
  request_.set_table(table->id);
  request_.set_shard(shard_);
  request_.set_row_count(kReadAhead);
  int target_worker = table->workerForShard(shard);
  rpc::NetworkThread::Get()->Call(target_worker + 1, MTYPE_ITERATOR, request_,
      &response_);
  request_.set_id(response_.id());
  pos_ = 0;
}

template<class K, class V>
inline void RemoteIterator<K, V>::Next() {
  int target_worker = dynamic_cast<ShardedTable*>(owner_)->workerForShard(
      shard_);
  if (pos_ == response_.row_count() - 1) {
    CHECK(!response_.done());
    rpc::NetworkThread::Get()->Call(target_worker + 1, MTYPE_ITERATOR, request_,
        &response_);
    if (response_.row_count() < 1 && !response_.done())
      LOG(ERROR)<< "Call to server requesting " << request_.row_count()
      << " rows returned " << response_.row_count() << " rows.";
    pos_ = 0;
  } else {
    ++pos_;
  }
}

template<class K, class V>
inline const K& RemoteIterator<K, V>::key() {
  unmarshal<V>(response_.key(pos_), &key_);
  return key_;
}

template<class K, class V>
inline V& RemoteIterator<K, V>::value() {
  unmarshal<V>(response_.value(pos_), &value_);
  return value_;
}

template<class K, class V>
inline bool RemoteIterator<K, V>::done() {
  return response_.done() && pos_ == response_.row_count();
}

template<class K, class V>
inline void RemoteIterator<K, V>::valueStr(string* out) {
  *out = response_.value(pos_);
}

template<class K, class V>
inline void RemoteIterator<K, V>::keyStr(string* out) {
  *out = response_.key(pos_);
}

}

#endif /* TABLE_INL_H_ */
