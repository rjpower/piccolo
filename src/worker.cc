#include <boost/bind.hpp>
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>
#include <signal.h>

#include "piccolo/table.h"
#include "piccolo/table-inl.h"
#include "piccolo/worker.h"

#include "util/common.h"
#include "util/tuple.h"
#include "util/stats.h"

using boost::unordered_map;
using boost::unordered_set;

DECLARE_double(sleep_time);
DEFINE_double(sleep_hack, 0.0, "");
DEFINE_string(checkpoint_write_dir, "/var/tmp/piccolo-checkpoint", "");
DEFINE_string(checkpoint_read_dir, "/var/tmp/piccolo-checkpoint", "");

namespace piccolo {

struct Worker::Stub: private boost::noncopyable {
  int32_t id;
  int32_t epoch;

  Stub(int id) :
      id(id), epoch(0) {
  }
};

Worker::Worker(const ConfigData &c) {
  epoch_ = 0;
  active_checkpoint_ = CP_NONE;

  network_ = rpc::NetworkThread::Get();

  config_.CopyFrom(c);
  config_.set_worker_id(network_->id() - 1);

  num_peers_ = config_.num_workers();
  peers_.resize(num_peers_);
  for (int i = 0; i < num_peers_; ++i) {
    peers_[i] = new Stub(i + 1);
  }

  running_ = true; //this is WORKER running, not KERNEL running!
  krunning_ = false; //and this is for KERNEL running
  handling_putreqs_ = false;
  iterator_id_ = 0;

  // HACKHACKHACK - register ourselves with any existing tables
  TableRegistry::Map &t = TableRegistry::tables();
  for (TableRegistry::Map::iterator i = t.begin(); i != t.end(); ++i) {
    i->second->setHelper((TableHelper*)this);
  }

  // Register RPC endpoints.
  rpc::RegisterCallback(MTYPE_GET, new HashGet, new TableData,
      &Worker::HandleGetRequest, this);

  rpc::RegisterCallback(MTYPE_SHARD_ASSIGNMENT, new ShardAssignmentRequest,
      new EmptyMessage, &Worker::HandleShardAssignment, this);

  rpc::RegisterCallback(MTYPE_ITERATOR, new IteratorRequest,
      new IteratorResponse, &Worker::HandleIteratorRequest, this);

  rpc::RegisterCallback(MTYPE_CLEAR_TABLE, new ClearTable, new EmptyMessage,
      &Worker::HandleClearRequest, this);

  rpc::RegisterCallback(MTYPE_SWAP_TABLE, new SwapTable, new EmptyMessage,
      &Worker::HandleSwapRequest, this);

  rpc::RegisterCallback(MTYPE_WORKER_FLUSH, new EmptyMessage, new FlushResponse,
      &Worker::HandleFlush, this);

  rpc::RegisterCallback(MTYPE_WORKER_APPLY, new EmptyMessage, new EmptyMessage,
      &Worker::HandleApply, this);

  rpc::RegisterCallback(MTYPE_WORKER_FINALIZE, new EmptyMessage,
      new EmptyMessage, &Worker::HandleFinalize, this);

  rpc::RegisterCallback(MTYPE_START_CHECKPOINT_ASYNC, new CheckpointRequest,
      new EmptyMessage, &Worker::HandleStartCheckpointAsync, this);

  rpc::RegisterCallback(MTYPE_FINISH_CHECKPOINT_ASYNC,
      new CheckpointFinishRequest, new EmptyMessage,
      &Worker::HandleFinishCheckpointAsync, this);

  rpc::RegisterCallback(MTYPE_RESTORE, new StartRestore, new EmptyMessage,
      &Worker::HandleStartRestore, this);

  rpc::NetworkThread::Get()->SpawnThreadFor(MTYPE_WORKER_FLUSH);
  rpc::NetworkThread::Get()->SpawnThreadFor(MTYPE_WORKER_APPLY);
}

int Worker::peer_for_shard(int table, int shard) const {
  return TableRegistry::tables()[table]->workerForShard(shard);
}

void Worker::Run() {
  KernelLoop();
}

Worker::~Worker() {
  running_ = false;

  for (size_t i = 0; i < peers_.size(); ++i) {
    delete peers_[i];
  }
}

void Worker::KernelLoop() {
  VLOG(1) << "Worker " << config_.worker_id() << " registering...";
  RegisterWorkerRequest req;
  req.set_id(id());
  network_->Send(0, MTYPE_REGISTER_WORKER, req);

  KernelRequest kreq;

  while (running_) {
    Timer idle;

    while (!network_->TryRead(config_.master_id(), MTYPE_RUN_KERNEL, &kreq)) {
      CheckNetwork();
      Sleep(FLAGS_sleep_time);

      if (!running_) {
        return;
      }

    }
    krunning_ = true; //a kernel is running
    stats_["idle_time"] += idle.elapsed();

    VLOG(1) << "Received run request for " << kreq;

    if (peer_for_shard(kreq.table(), kreq.shard()) != config_.worker_id()) {
      LOG(FATAL)<< "Received a shard I can't work on! : " << kreq.shard()
      << " : " << peer_for_shard(kreq.table(), kreq.shard());
    }

    KernelInfo *helper = KernelRegistry::Get()->kernel(kreq.kernel());
    KernelId id(kreq.kernel(), kreq.table(), kreq.shard());
    KernelBase* d = kernels_[id];

    if (!d) {
      d = helper->create();
      kernels_[id] = d;
      d->initialize_internal(this, kreq.table(), kreq.shard());
      d->_init();
    }

    if (this->id() == 1 && FLAGS_sleep_hack > 0) {
      Sleep(FLAGS_sleep_hack);
    }

    // Run the user kernel
    helper->Run(d, kreq.method());

    KernelDone kd;
    kd.mutable_kernel()->CopyFrom(kreq);
    TableRegistry::Map &tmap = TableRegistry::tables();
    for (TableRegistry::Map::iterator i = tmap.begin(); i != tmap.end(); ++i) {
      ShardedTable* t = i->second;
      for (int j = 0; j < t->numShards; ++j) {
        if (t->isLocalShard(j)) {
          ShardInfo *si = kd.add_shards();
          si->set_entries(t->partition(j)->size());
          si->set_owner(this->id());
          si->set_table(i->first);
          si->set_shard(j);
        }
      }
    }
    krunning_ = false;
    network_->Send(config_.master_id(), MTYPE_KERNEL_DONE, kd);

    VLOG(1) << "Kernel finished: " << kreq;
    DumpProfile();
  }
}

void Worker::CheckNetwork() {
  Timer net;
  CheckForMasterUpdates();
  HandlePutRequest();

  // Flush any tables we no longer own.
  for (unordered_set<ShardedTable*>::iterator i = dirty_tables_.begin();
      i != dirty_tables_.end(); ++i) {
    ShardedTable *mg = dynamic_cast<ShardedTable*>(*i);
    if (mg) {
      mg->sendUpdates();
    }
  }

  dirty_tables_.clear();
  stats_["network_time"] += net.elapsed();
}

int64_t Worker::pending_kernel_bytes() const {
  int64_t t = 0;

  TableRegistry::Map &tmap = TableRegistry::tables();
  for (TableRegistry::Map::iterator i = tmap.begin(); i != tmap.end(); ++i) {
    ShardedTable *mg = dynamic_cast<ShardedTable*>(i->second);
    if (mg) {
      t += mg->pendingWriteBytes();
    }
  }

  return t;
}

bool Worker::network_idle() const {
  return network_->pending_bytes() == 0;
}

bool Worker::has_incoming_data() const {
  return true;
}

void Worker::HandlePutRequest() {
  boost::recursive_try_mutex::scoped_lock sl(state_lock_);
  if (!sl.owns_lock() || handling_putreqs_ == true)
    return;

  handling_putreqs_ = true; //protected by state_lock_

  TableData put;
  while (network_->TryRead(rpc::ANY_SOURCE, MTYPE_PUT_REQUEST, &put)) {
    if (put.marker() != -1) {
      UpdateEpoch(put.source(), put.marker());
      continue;
    }

    VLOG(2) << "Read put request of size: " << put.kv_data_size() << " for "
               << MP(put.table(), put.shard());

    ShardedTable *t = TableRegistry::table(put.table());
    t->applyUpdates(put);
  }

  handling_putreqs_ = false; //protected by state_lock_
}

void Worker::HandleGetRequest(const HashGet& get_req, TableData *get_resp,
    const rpc::RPCInfo& rpc) {
//    LOG(INFO) << "Get request: " << get_req;

  get_resp->Clear();
  get_resp->set_source(config_.worker_id());
  get_resp->set_table(get_req.table());
  get_resp->set_shard(-1);
  get_resp->set_done(true);
  get_resp->set_epoch(epoch_);

  {
    ShardedTable * t = TableRegistry::table(get_req.table());
    t->handleGetRequest(get_req, get_resp);
  }

  VLOG(2) << "Returning result for " << MP(get_req.table(), get_req.shard())
             << " - found? " << !get_resp->missing_key();
}

void Worker::HandleSwapRequest(const SwapTable& req, EmptyMessage *resp,
    const rpc::RPCInfo& rpc) {
  ShardedTable *ta = TableRegistry::table(req.table_a());
  ShardedTable *tb = TableRegistry::table(req.table_b());

  ta->swapLocal(tb);
}

void Worker::HandleClearRequest(const ClearTable& req, EmptyMessage *resp,
    const rpc::RPCInfo& rpc) {
  ShardedTable *ta = TableRegistry::table(req.table());

  for (int i = 0; i < ta->numShards; ++i) {
    if (ta->isLocalShard(i)) {
      ta->partition(i)->clear();
    }
  }
}

void Worker::HandleIteratorRequest(const IteratorRequest& iterator_req,
    IteratorResponse *iterator_resp, const rpc::RPCInfo& rpc) {
  int table = iterator_req.table();
  int shard = iterator_req.shard();

  ShardedTable * t = TableRegistry::table(table);
  TableIterator* it = NULL;
  if (iterator_req.id() == -1) {
    it = t->iterator(shard);
    uint32_t id = iterator_id_++;
    iterators_[id] = it;
    iterator_resp->set_id(id);
  } else {
    it = iterators_[iterator_req.id()];
    iterator_resp->set_id(iterator_req.id());
    CHECK_NE(it, (void *)NULL);
    it->Next();
  }

  iterator_resp->set_row_count(0);
  iterator_resp->clear_key();
  iterator_resp->clear_value();
  for (int i = 1; i <= iterator_req.row_count(); i++) {
    iterator_resp->set_done(it->done());
    if (!it->done()) {
      std::string* respkey = iterator_resp->add_key();
      it->keyStr(respkey);
      std::string* respvalue = iterator_resp->add_value();
      it->valueStr(respvalue);
      iterator_resp->set_row_count(i);
      if (i < iterator_req.row_count())
        it->Next();
    } else
      break;
  }
  VLOG(2) << "[PREFETCH] Returning " << iterator_resp->row_count()
             << " rows in response to request for " << iterator_req.row_count()
             << " rows in table " << table << ", shard " << shard;
}

void Worker::HandleShardAssignment(const ShardAssignmentRequest& shard_req,
    EmptyMessage *resp, const rpc::RPCInfo& rpc) {
//  LOG(INFO) << "Shard assignment: " << shard_req.DebugString();
  for (int i = 0; i < shard_req.assign_size(); ++i) {
    const ShardAssignment &a = shard_req.assign(i);
    ShardedTable *t = TableRegistry::table(a.table());
    int old_workerForShard = t->workerForShard(a.shard());
    t->partitionInfo(a.shard())->sinfo.set_owner(a.new_worker());

    VLOG(3) << "Setting workerForShard: " << MP(a.shard(), a.new_worker());

    if (a.new_worker() == id() && old_workerForShard != id()) {
      VLOG(1) << "Setting self as workerForShard of "
                 << MP(a.table(), a.shard());

      // Don't consider ourselves canonical for this shard until we receive updates
      // from the old workerForShard.
      if (old_workerForShard != -1) {
        LOG(INFO)<< "Setting " << MP(a.table(), a.shard())
        << " as tainted.  Old workerForShard was: " << old_workerForShard
        << " new workerForShard is :  " << id();
        t->partitionInfo(a.shard())->tainted = true;
      }
    } else if (old_workerForShard == id() && a.new_worker() != id()) {
      VLOG(1)
      << "Lost workerForShardship of " << MP(a.table(), a.shard()) << " to "
      << a.new_worker();
      // A new worker has taken workerForShardship of this shard.  Flush our data out.
      t->partitionInfo(a.shard())->dirty = true;
      dirty_tables_.insert(t);
    }
  }
}

void Worker::HandleFlush(const EmptyMessage& req, FlushResponse *resp,
    const rpc::RPCInfo& rpc) {
  Timer net;

  TableRegistry::Map &tmap = TableRegistry::tables();

  for (TableRegistry::Map::iterator i = tmap.begin(); i != tmap.end(); ++i) {
    ShardedTable* t = dynamic_cast<ShardedTable*>(i->second);
    if (t) {
      VLOG(2) << "Doing flush for table " << i->second;
      t->sendUpdates();
    }
  }
  network_->Flush();

  network_->Send(config_.master_id(), MTYPE_FLUSH_RESPONSE, *resp);

  network_->Flush();
  stats_["network_time"] += net.elapsed();
}

void Worker::HandleApply(const EmptyMessage& req, EmptyMessage *resp,
    const rpc::RPCInfo& rpc) {
  if (krunning_) {
    LOG(FATAL)<< "Received APPLY message while still running!?!";
    return;
  }

  HandlePutRequest();

  network_->Send(config_.master_id(), MTYPE_WORKER_APPLY_DONE, *resp);
}

void Worker::HandleFinalize(const EmptyMessage& req, EmptyMessage *resp,
    const rpc::RPCInfo& rpc) {
  Timer net;
  VLOG(2) << "Finalize request received from master; performing finalization.";

  if (active_checkpoint_ == CP_CONTINUOUS) {
    active_checkpoint_ = CP_INTERVAL; //this will let it finalize properly
  }

  VLOG(2) << "Telling master: Finalized.";
  network_->Send(config_.master_id(), MTYPE_WORKER_FINALIZE_DONE, *resp);

  stats_["network_time"] += net.elapsed();
}

void Worker::CheckForMasterUpdates() {
  boost::recursive_mutex::scoped_lock sl(state_lock_);
  // Check for shutdown.
  EmptyMessage empty;
  KernelRequest k;

  if (network_->TryRead(config_.master_id(), MTYPE_WORKER_SHUTDOWN, &empty)) {
    VLOG(1) << "Shutting down worker " << config_.worker_id();
    running_ = false;
    return;
  }

  CheckpointRequest checkpoint_msg;
  while (network_->TryRead(config_.master_id(), MTYPE_START_CHECKPOINT,
      &checkpoint_msg)) {
    for (int i = 0; i < checkpoint_msg.table_size(); ++i) {
      checkpoint_tables_.insert(std::make_pair(checkpoint_msg.table(i), true));
    }

    VLOG(1) << "Starting checkpoint type " << checkpoint_msg.checkpoint_type()
               << ", epoch " << checkpoint_msg.epoch();
    StartCheckpoint(checkpoint_msg.epoch(),
        (CheckpointType) checkpoint_msg.checkpoint_type(), false);
  }

  CheckpointFinishRequest checkpoint_finish_msg;
  while (network_->TryRead(config_.master_id(), MTYPE_FINISH_CHECKPOINT,
      &checkpoint_finish_msg)) {
    VLOG(1) << "Finishing checkpoint on master's instruction";
    FinishCheckpoint(checkpoint_finish_msg.next_delta_only());
  }
}

void Worker::HandleStartCheckpointAsync(const CheckpointRequest& req,
    EmptyMessage* resp, const rpc::RPCInfo& rpc) {
  VLOG(1) << "Async order for checkpoint received.";
  checkpoint_tables_.clear();
  for (int i = 0; i < req.table_size(); ++i) {
    checkpoint_tables_.insert(std::make_pair(req.table(i), true));
  }
  StartCheckpoint(req.epoch(), (CheckpointType) req.checkpoint_type(), false);
}

void Worker::HandleFinishCheckpointAsync(const CheckpointFinishRequest& req,
    EmptyMessage *resp, const rpc::RPCInfo& rpc) {
  VLOG(1) << "Async order for checkpoint finish received.";
  FinishCheckpoint(req.next_delta_only());
}

bool StartWorker(const ConfigData& conf) {

  if (rpc::NetworkThread::Get()->id() == 0)
    return false;

  Worker w(conf);
  w.Run();
  Stats s = w.get_stats();
  s.Merge(rpc::NetworkThread::Get()->stats);
  VLOG(1) << "Worker stats: \n"
             << s.ToString(StringPrintf("[W%d]", conf.worker_id()));
  exit(0);
}

} // end namespace
