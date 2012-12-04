#ifndef DENSE_MAP_H_
#define DENSE_MAP_H_

#include "util/common.h"
#include "piccolo.pb.h"
#include "local-table.h"
#include "table.h"
#include "table-inl.h"
#include <boost/noncopyable.hpp>
#include <boost/dynamic_bitset.hpp>

#include <tr1/unordered_map>

namespace piccolo {

template<class K>
struct BlockInfo: BlockInfoBase {
  virtual ~BlockInfo() {
  }
  // Returns the key representing the first element in the block
  // containing 'k'.
  virtual K start(const K& k, int block_size) = 0;

  // Returns the index offset of 'k' within it's block.
  virtual int offset(const K& k, int block_size) = 0;

};

struct IntBlockInfo: public BlockInfo<int> {
  int start(const int& k, int block_size) {
    return k - (k % block_size);
  }

  int offset(const int& k, int block_size) {
    return k % block_size;
  }

};

// Provides fast lookups for dense key-spaces.  Keys are divided into 'blocks' of
// contiguous elements; users can operate on single entries or blocks at a time for
// more efficient access.  Modifying a single entry in a block marks the entire
// block as dirty, triggering a future of the block if non-local.
template<class K, class V>
class DenseTable: public LocalTable,
    public TableT<K, V>,
    private boost::noncopyable {
public:
  struct Bucket {
    Bucket() :
        entries(0) {
    }
    Bucket(int count) :
        entries(count) {
    }

    std::vector<V> entries;
    bool dirty;
  };

  typedef typename std::tr1::unordered_map<K, Bucket> BucketMap;
  typedef DecodeIterator<K, V> UpdateDecoder;

  struct Iterator: public TableIteratorT<K, V> {
    Iterator(DenseTable<K, V> &parent) :
        parent_(parent), it_(parent_.m_.begin()), idx_(0) {
    }

    Marshal<K> *kmarshal() {
      return ((Marshal<K>*) parent_.kmarshal());
    }
    Marshal<V> *vmarshal() {
      return ((Marshal<V>*) parent_.vmarshal());
    }

    void Next() {
      ++idx_;
      if (idx_ >= parent_.info_.block_size) {
        ++it_;
        idx_ = 0;
      }
    }

    bool done() {
      return it_ == parent_.m_.end();
    }

    const K& key() {
      k_ = it_->first + idx_;
      return k_;
    }
    V& value() {
      return it_->second.entries[idx_];
    }

    DenseTable<K, V> &parent_;
    K k_;
    typename BucketMap::iterator it_;
    int idx_;
  };

  struct Factory: public TableFactory {
    TableBase* New() {
      return new DenseTable<K, V>();
    }
  };

  BlockInfo<K>& block_info() {
    return *(BlockInfo<K>*) this->info_.block_info;
  }

  // return the first key in a bucket
  K start_key(const K& k) {
    return block_info().start(k, this->info_.block_size);
  }

  int block_pos(const K& k) {
    return block_info().offset(k, this->info_.block_size);
  }

  // Construct a hashmap with the given initial size; it will be expanded as necessary.
  DenseTable(int size = 1) :
      m_(size), bitset_epoch_(0) {
    last_block_ = NULL;
    trigger_flags_.resize(size);
  }

  ~DenseTable() {
  }

  void Init(const TableDescriptor * td) {
    TableBase::init(td);
  }

  bool contains(const K& k) {
    return m_.find(start_key(k)) != m_.end();
  }

  // We anticipate a strong locality relationship between successive operations.
  // The last accessed block is cached, and can be returned immediately if the
  // subsequent operation(s) also access the same block.
  V* get_block(const K& k) {
    K start = start_key(k);

    if (last_block_ && start == last_block_start_) {
      return last_block_;
    }

    Bucket &vb = m_[start];
    if (vb.entries.size() != this->info_.block_size) {
      vb.entries.resize(this->info_.block_size);
    }

    last_block_ = &vb.entries[0];
    last_block_start_ = start;

    return last_block_;
  }

  V get(const K& k) {
    return get_block(k)[block_pos(k)];
  }

  void update(const K& k, const V& v) {
    V* vb = get_block(k);

    ((Accumulator<V>*) this->info_.accum)->Accumulate(&vb[block_pos(k)], v);
  }

  void put(const K& k, const V& v) {
    V* vb = get_block(k);
    vb[block_pos(k)] = v;
  }

  void remove(const K& k) {
    LOG(FATAL)<< "Not implemented.";
  }

  TableIterator* get_iterator() {
    return new Iterator(*this);
  }

  bool empty() {
    return size() == 0;
  }

  int64_t size() {
    return m_.size() * this->info_.block_size;
  }

  int64_t capacity() {
    return size();
  }

  void clear() {
    m_.clear();
    last_block_ = NULL;
  }

  void resize(int64_t s) {
    LOG(FATAL) << " DenseTable resize not implemented!";
  }

  void Serialize(TableCoder* out, bool tryOptimize = false) {
    string k, v;
    for (typename BucketMap::iterator i = m_.begin(); i != m_.end(); ++i) {
      v.clear();

      // For the purposes of serialization, all values in a bucket are assumed
      // to be the same number of bytes.
      marshal(i->first, &k);

      string tmp;
      Bucket &b = i->second;
      for (int j = 0; j < b.entries.size(); ++j) {
        marshal(b.entries[j], &tmp);
        v += tmp;
      }

      out->write(k, v);
    }
  }

  void read(TableCoder* in) {
    string k,v;
    while(in->read(&k,&v)) {
      updateStr(k,v);
    }
  }

  void DecodeUpdates(TableCoder *in, DecodeIteratorBase *itbase) {
    UpdateDecoder *it = static_cast<UpdateDecoder*>(itbase);
    K k;
    string kt, vt;

    it->clear();
    while (in->read(&kt, &vt)) {
      unmarshal(kt, &k);
      const int value_size = vt.size() / this->info_.block_size;

      V tmp;
      for (int j = 0; j < this->info_.block_size; ++j) {
        unmarshal(StringPiece(vt.data() + (value_size * j), value_size), &tmp);
        it->append(k + j, tmp);
      }
    }
    it->rewind();
    return;
  }

private:
  BucketMap m_;
  V* last_block_;
  K last_block_start_;
  boost::dynamic_bitset<uint32_t> trigger_flags_;        //Retrigger flags
  int bitset_epoch_;
};}
#endif
