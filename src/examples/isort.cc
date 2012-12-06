#include "piccolo.h"
#include "examples/examples.h"
#include <algorithm>

using namespace piccolo;

typedef uint32_t KeyType;
typedef Bucket ValueType;

static std::vector<int> src;
typedef TableT<KeyType, ValueType> SortTable;
static SortTable *dst = NULL;

struct BucketMerge: public Accumulator<Bucket> {
  void Accumulate(Bucket *l, const Bucket &r) {
    l->MergeFrom(r);
  }
};

struct KeyGen {
  KeyGen() :
      x_(314159625), a_(1220703125) {
  }
  KeyType next() {
    uint64_t n = a_ * x_ % (2ll << 46);
    x_ = n;
    return x_;
  }

  uint64_t x_;
  uint64_t a_;
};

DEFINE_int64(sort_size, 1000000, "");

class SortKernel: public Kernel {
public:
  void Init(SortTable* t, int shard) {
    KeyGen k;
    for (int i = 0; i < FLAGS_sort_size / dst->numShards; ++i) {
      src.push_back(k.next());
    }
  }

  void Partition(SortTable* t, int shard) {
    Bucket b;
    b.mutable_value()->Add(0);
    for (int i = 0; i < src.size(); ++i) {
      PERIODIC(1.0, LOG(INFO) << "Partitioning...." << 100. * i / src.size());
      b.set_value(0, src[i]);
      dst->put(src[i] & 0xffff, b);
    }
  }

  void Sort(SortTable* t, int shard) {
    TableIteratorT<KeyType, ValueType> *i = dst->typedIterator();
    while (!i->done()) {
      Bucket b = i->value();
      uint32_t* t = b.mutable_value()->mutable_data();
      std::sort(t, t + b.value_size());
      i->Next();
    }
  }

};

struct IntegerSort: public Worker {
  void setup(const ConfigData& conf) {
    dst = TableRegistry::sparse(conf.num_workers(), new Sharding::UintMod,
        new BucketMerge);
  }

  void run(Master* m, const ConfigData& conf) {
    dst->runKernel<SortKernel, &SortKernel::Init>();
    dst->runKernel<SortKernel, &SortKernel::Partition>();
    dst->runKernel<SortKernel, &SortKernel::Sort>();
  }
};
