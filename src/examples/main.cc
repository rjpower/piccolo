#include "piccolo.h"
#include "examples.h"

namespace piccolo {
RunnerRegistry::Map RunnerRegistry::runners;
}

using namespace piccolo;

DEFINE_string(runner, "", "");

DEFINE_int32(shards, 10, "");
DEFINE_int32(iterations, 10, "");
DEFINE_int32(block_size, 10, "");
DEFINE_int32(edge_size, 1000, "");
DEFINE_bool(build_graph, false, "");

DECLARE_bool(log_prefix);

int main(int argc, char** argv) {
  FLAGS_log_prefix = false;

  Init(argc, argv);

  ConfigData conf;
  conf.set_num_workers(MPI::COMM_WORLD.Get_size() - 1);
  conf.set_worker_id(MPI::COMM_WORLD.Get_rank() - 1);

  RunnerRegistry::runners[FLAGS_runner]->setup(conf);
  if (!StartWorker(conf)) {
    Master m(conf);
    RunnerRegistry::runners[FLAGS_runner]->run(&m, conf);
  }
  LOG(INFO)<< "Exiting.";
}
