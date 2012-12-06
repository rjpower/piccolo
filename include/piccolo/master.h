#ifndef MASTER_H_
#define MASTER_H_

#include "piccolo.pb.h"
#include "piccolo/table.h"
#include "piccolo/kernel.h"

#include "util/common.h"
#include "util/timer.h"
#include "util/rpc.h"

#include <vector>
#include <map>

namespace piccolo {

class WorkerState;
class TaskState;

struct RunDescriptor {
  ShardedTable *table;
  int kernelId;
  std::vector<int> shards;
};

template <class K, class V>
struct Mapper {
  Mapper(boost::function<void (const K&, V&)> mapF);
  void operator()(const K&, V&);
};

class Master {
public:
  Master(const ConfigData &conf);
  ~Master();

  void run(RunDescriptor r);

private:

  WorkerState* worker_for_shard(int table, int shard);

  // Find a worker to run a kernel on the given table and shard.  If a worker
  // already serves the given shard, return it.  Otherwise, find an eligible
  // worker and assign it to them.
  WorkerState* assign_worker(int table, int shard);

  void send_table_assignments();
  void assign_tables();

  void dump_stats();
  int reap_one_task();

  void barrier();
  void assign_tasks(const RunDescriptor& r, std::vector<int> shards);
  int dispatch_work(const RunDescriptor& r);
  bool steal_work(const RunDescriptor& r, int idle_worker,
      double avg_completion_time);

  RunDescriptor current_run_;
  double current_run_start_;

  ConfigData config_;
  int kernel_epoch_;

  size_t dispatched_;
  size_t finished_;

  bool shards_assigned_;

  std::vector<WorkerState*> workers_;

  typedef std::map<int, MethodStats> MethodStatsMap;
  MethodStatsMap method_stats_;

  TableRegistry::Map& tables_;
  rpc::NetworkThread* network_;
  Timer runtime_;
};
}

#endif /* MASTER_H_ */
