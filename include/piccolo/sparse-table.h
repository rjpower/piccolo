#ifndef SPARSE_MAP_H_
#define SPARSE_MAP_H_

#include "util/common.h"

#include "piccolo.pb.h"
#include "piccolo/table.h"

#include <boost/noncopyable.hpp>
#include <boost/dynamic_bitset.hpp>

namespace piccolo {

static const double kLoadFactor = 0.4;

template<class K, class V>
class SparseTable: public TableT<K, V>, private boost::noncopyable {
private:
#pragma pack(push, 1)
  struct Bucket {
    K k;
    V v;
    bool in_use;
  };
#pragma pack(pop)

public:
  struct Iterator: public TableIteratorT<K, V> {
    Iterator(SparseTable<K, V>& parent) :
        pos(-1), parent_(parent) {
      Next();
    }

    void Next() {
      do {
        ++pos;
      } while (pos < parent_.size_ && !parent_.buckets_[pos].in_use);
    }

    bool done() {
      return pos == parent_.size_;
    }

    const K& key() {
      return parent_.buckets_[pos].k;
    }
    V& value() {
      return parent_.buckets_[pos].v;
    }

    int pos;
    SparseTable<K, V> &parent_;
  };

  static Table* create() {
    return new SparseTable;
  }

  // Construct a SparseTable with the given initial size; it will be expanded as necessary.
  SparseTable(int size = 1);
  ~SparseTable() {
  }

  V get(const K& k);
  bool contains(const K& k);
  void put(const K& k, const V& v);
  void update(const K& k, const V& v);
  void remove(const K& k) {
    LOG(FATAL)<< "Not implemented.";
  }
  void resize(int64_t size);

  bool empty() {return size() == 0;}
  int64_t size() {return entries_;}
  int64_t capacity() {return size_;}

  void clear() {
    for (int i = 0; i < size_; ++i) {buckets_[i].in_use = 0;}
    entries_ = 0;
  }

  TableIterator *get_iterator() {
    return new Iterator(*this);
  }

  void write(TableCoder *out);
  int64_t read(TableCoder *in);
  void applyUpdates(TableCoder *in);

private:
  uint32_t bucket_idx(K k) {
    return hashobj_(k) % size_;
  }

  int bucket_for_key(const K& k) {
    int start = bucket_idx(k);
    int b = start;
    int tries = 0;

    do {
      ++tries;
      if (buckets_[b].in_use) {
        if (buckets_[b].k == k) {
          return b;
        }
      } else {
        return -1;
      }

      b = (b + 1) % size_;
    }while (b != start);

    return -1;
  }

  std::vector<Bucket> buckets_;

  int64_t entries_;
  int64_t size_;
  int bitset_epoch_;

  std::tr1::hash<K> hashobj_;
};

template<class K, class V>
SparseTable<K, V>::SparseTable(int size) :
    buckets_(0), entries_(0), size_(0), bitset_epoch_(0) {
  clear();

  resize(size);
}

template<class K, class V>
void SparseTable<K, V>::write(TableCoder *out) {
  Iterator *i = (Iterator*) get_iterator();
  string k, v;
  while (!i->done()) {
    k.clear();
    v.clear();
    marshal(i->key(), &k);
    marshal(i->value(), &v);
    out->write(k, v);
    i->Next();
  }
  delete i;
}

template<class K, class V>
int64_t SparseTable<K, V>::read(TableCoder *in) {
  string k, v;
  int updates = 0;
  while (in->read(&k, &v)) {
    this->updateStr(k, v);
    updates++;
  }
  return updates;
}

template<class K, class V>
void SparseTable<K, V>::applyUpdates(TableCoder *in) {
  K k;
  V v;
  string kt, vt;

  while (in->read(&kt, &vt)) {
    unmarshal(kt, &k);
    unmarshal(vt, &v);
    update(k, v);
  }
}

template<class K, class V>
void SparseTable<K, V>::resize(int64_t size) {
  CHECK_GT(size, 0);
  VLOG(1) << "Resizing/rehashing table " << this->id << "... " << entries_ << " : "
             << size_ << " -> " << size;

  if (size_ == size)
    return;

  std::vector<Bucket> old_b = buckets_;

  int old_entries = entries_;

  buckets_.resize(size);
  size_ = size;
  clear();

  for (size_t i = 0; i < old_b.size(); ++i) {
    if (old_b[i].in_use) {
      put(old_b[i].k, old_b[i].v);
    }
  }

  CHECK_EQ(old_entries, entries_);
}

template<class K, class V>
bool SparseTable<K, V>::contains(const K& k) {
  return bucket_for_key(k) != -1;
}

template<class K, class V>
V SparseTable<K, V>::get(const K& k) {
  int b = bucket_for_key(k);

  CHECK_NE(b, -1)<< "No entry for requested key";

  return buckets_[b].v;
}

template<class K, class V>
void SparseTable<K, V>::update(const K& k, const V& v) {
  int b = bucket_for_key(k);
  if (b != -1) {
    static_cast<Accumulator<V>*>(this->accumulator)->Accumulate(&buckets_[b].v,
        v);
  }
}

template<class K, class V>
void SparseTable<K, V>::put(const K& k, const V& v) {
  int start = bucket_idx(k);
  int b = start;
  bool found = false;

  do {
    if (!buckets_[b].in_use) {
      break;
    }

    if (buckets_[b].k == k) {
      found = true;
      break;
    }

    b = (b + 1) % size_;
  } while (b != start);

// Inserting a new entry:
  if (!found) {
    if (entries_ > size_ * kLoadFactor) {
      resize((int) (1 + size_ * 2));
      put(k, v);
    } else {
      buckets_[b].in_use = 1;
      buckets_[b].k = k;
      buckets_[b].v = v;

      ++entries_;
    }
  } else {
    // Replacing an existing entry
    buckets_[b].v = v;
  }
}

} /* namespace piccolo */

#endif /* SPARSE_MAP_H_ */
