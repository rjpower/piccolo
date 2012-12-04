#include "examples/examples.h"

using std::vector;

using namespace piccolo;

DEFINE_int32(num_nodes, 10000, "Default number of nodes in graph");

static int NUM_WORKERS = 0;
static TableT<int, double>* distance_map;
static TableT<int, PathNode>* nodes;

struct ShortestPath {
  void setup(const ConfigData& conf) {
    NUM_WORKERS = conf.num_workers();

    distance_map = TableRegistry::sparse(FLAGS_shards, new Sharding::Mod,
        new Accumulators<double>::Min);

    // nodes = TableRegistry::record(FLAGS_source);
  }

  void run(Master* m, const ConfigData& conf) {
    distance_map->reserve(2 * FLAGS_num_nodes);

    for (int i = 0; i < 100; ++i) {
    }
  }
};

REGISTER_RUNNER(ShortestPath);
