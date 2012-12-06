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
  network_ = rpc::NetworkThread::Get();

  config_.CopyFrom(c);
  config_.set_worker_id(network_->id() - 1);

  num_peers_ = config_.num_workers();
  peers_.resize(num_peers_);
  for (int i = 0; i < num_peers_; ++i) {
    peers_[i] = new Stub(i + 1);
  }

  workerRunning_ = true;
  kernelRunning_ = false;
  handlingPuts_ = false;
  iterator_id_ = 0;

  // HACKHACKHACK - register ourselves with any existing tables
  TableRegistry::Map &t = TableRegistry::tables;
  for (TableRegistry::Map::iterator i = t.begin(); i != t.end(); ++i) {
    i->second->setHelper((TableHelper*) this);
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

  rpc::NetworkThread::Get()->SpawnThreadFor(MTYPE_WORKER_FLUSH);
  rpc::NetworkThread::Get()->SpawnThreadFor(MTYPE_WORKER_APPLY);
}

int Worker::peer_for_shard(int table, int shard) const {
  return TableRegistry::tables[table]->workerForShard(shard);
}

void Worker::Run() {
  KernelLoop();
}

Worker::~Worker() {
  workerRunning_ = false;

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

  while (workerRunning_) {
    Timer idle;

    while (!network_->TryRead(config_.master_id(), MTYPE_RUN_KERNEL, &kreq)) {
      CheckNetwork();
      Sleep(FLAGS_sleep_time);

      if (!workerRunning_) {
        return;
      }

    }
    kernelRunning_ = true; //a kernel is running
    stats_["idle_time"] += idle.elapsed();

    VLOG(1) << "Received run request for " << kreq;

    if (peer_for_shard(kreq.table(), kreq.shard()) != config_.worker_id()) {
      LOG(FATAL)<< "Received a shard I can't work on! : " << kreq.shard()
      << " : " << peer_for_shard(kreq.table(), kreq.shard());
    }

    Kernel* k = KernelRegistry::create(kreq.kernelid());

    if (this->id() == 1 && FLAGS_sleep_hack > 0) {
      Sleep(FLAGS_sleep_hack);
    }

    k->run(TableRegistry::table(kreq.table()), kreq.shard());

    KernelDone kd;
    kd.mutable_kernel()->CopyFrom(kreq);
    TableRegistry::Map &tmap = TableRegistry::tables;
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
    kernelRunning_ = false;
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
    ShardedTable *t = dynamic_cast<ShardedTable*>(*i);
  }

  dirty_tables_.clear();
  stats_["network_time"] += net.elapsed();
}

int64_t Worker::pending_kernel_bytes() const {
  int64_t t = 0;

  TableRegistry::Map &tmap = TableRegistry::tables;
  for (TableRegistry::Map::iterator i = tmap.begin(); i != tmap.end(); ++i) {
    ShardedTable *mg = dynamic_cast<ShardedTable*>(i->second);
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
  handlingPuts_ = true;

  TableData put;
  while (network_->TryRead(rpc::ANY_SOURCE, MTYPE_PUT_REQUEST, &put)) {
    VLOG(2) << "Read put request of size: " << put.kv_data_size() << " for "
               << MP(put.table(), put.shard());

    ShardedTable *t = TableRegistry::table(put.table());
    //t->applyUpdates(put);
    LOG(FATAL)<< "TODO";
  }

  handlingPuts_ = false;
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
    Table* shard = t->partition(get_req.shard());
    if (shard->containsStr(get_req.key())) {
      get_resp->set_missing_key(false);
      Arg* kv = get_resp->add_kv_data();
      kv->set_key(get_req.key());
      kv->set_value(shard->getStr(get_req.key()));
    } else {
      get_resp->set_missing_key(true);
    }
  }

  VLOG(2) << "Returning result for " << MP(get_req.table(), get_req.shard())
             << " - found? " << !get_resp->missing_key();
}

void Worker::HandleSwapRequest(const SwapTable& req, EmptyMessage *resp,
    const rpc::RPCInfo& rpc) {
  ShardedTable *ta = TableRegistry::table(req.table_a());
  ShardedTable *tb = TableRegistry::table(req.table_b());

  // ta->swapLocal(tb);
  LOG(FATAL) << "TODO";
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
    it = t->partition(shard)->iterator();
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
    t->partitionInfo(a.shard())->set_owner(a.new_worker());

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
        t->partitionInfo(a.shard())->set_tainted(true);
      }
    } else if (old_workerForShard == id() && a.new_worker() != id()) {
      VLOG(1)
      << "Lost workerForShardship of " << MP(a.table(), a.shard()) << " to "
      << a.new_worker();
      // A new worker has taken workerForShardship of this shard.  Flush our data out.
      t->partitionInfo(a.shard())->set_dirty(true);
      dirty_tables_.insert(t);
    }
  }
}

void Worker::HandleFlush(const EmptyMessage& req, FlushResponse *resp,
    const rpc::RPCInfo& rpc) {
  Timer net;

  TableRegistry::Map &tmap = TableRegistry::tables;

  for (TableRegistry::Map::iterator i = tmap.begin(); i != tmap.end(); ++i) {
    ShardedTable* t = dynamic_cast<ShardedTable*>(i->second);
    if (t) {
      VLOG(2) << "Doing flush for table " << i->second;
      LOG(FATAL) << "TODO - send updates";
    }
  }
  network_->Flush();

  network_->Send(config_.master_id(), MTYPE_FLUSH_RESPONSE, *resp);

  network_->Flush();
  stats_["network_time"] += net.elapsed();
}

void Worker::HandleApply(const EmptyMessage& req, EmptyMessage *resp,
    const rpc::RPCInfo& rpc) {
  HandlePutRequest();

  network_->Send(config_.master_id(), MTYPE_WORKER_APPLY_DONE, *resp);
}

void Worker::HandleFinalize(const EmptyMessage& req, EmptyMessage *resp,
    const rpc::RPCInfo& rpc) {
  Timer net;
  VLOG(2) << "Finalize request received from master; performing finalization.";

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
    workerRunning_ = false;
    return;
  }
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
