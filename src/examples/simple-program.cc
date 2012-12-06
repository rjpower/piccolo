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

  static void ShardFunction(TableT<int32_t, double>* table, int shard) {
    for (int64_t i = shard; i < 100000; i += table->numShards) {
      a->update(i, random());
      b->update(i, random());}
  }

  void run(Master* m, const ConfigData& conf) {
    a->reserve(100000);
    b->reserve(100000);
    a->run<ShardFunction>();
  }
};
