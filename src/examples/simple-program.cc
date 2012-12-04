#include "piccolo.h"

using namespace piccolo;

static TableT<int32_t, double> *a, *b, *c;

struct SimpleProgram {
  void setup(const ConfigData& conf) {
    a = TableRegistry::sparse(100, new Sharding::Mod,
        new Accumulators<double>::Sum);
    b = TableRegistry::sparse(100, new Sharding::Mod,
        new Accumulators<double>::Sum);
    c = TableRegistry::sparse(100, new Sharding::Mod,
        new Accumulators<double>::Sum);
  }

  void run(Master* m, const ConfigData& conf) {
    a->reserve(100000);
    b->reserve(100000);
    m->run(a, [](TableT<int32_t, double>* shard) {
      for (int64_t i = shard->shard; i < 100000; i += shard->numShards) {
        a->update(i, random());
        b->update(i, random());}
    });
  }
};
