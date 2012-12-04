#include "piccolo/table.h"
#include "piccolo/table-inl.h"
#include "util/tuple.h"

namespace piccolo {

void ShardedTable::updatePartitions(const ShardInfo& info) {
  partInfo_[info.shard()].sinfo.CopyFrom(info);
}

ShardedTable::~ShardedTable() {
  for (int i = 0; i < partitions_.size(); ++i) {
    delete partitions_[i];
  }
}

bool ShardedTable::isLocalShard(int shard) {
  return workerForShard(shard) == workerId_;
}

bool ShardedTable::isLocalKey(const StringPiece &k) {
  return isLocalShard(shardForKeyStr(k));
}

void ShardedTable::swap(ShardedTable *b) {
  SwapTable req;

  req.set_table_a(this->id);
  req.set_table_b(b->id);
  VLOG(2)
             << StringPrintf("Sending swap request (%d <--> %d)", req.table_a(),
                 req.table_b());

  rpc::NetworkThread::Get()->SyncBroadcast(MTYPE_SWAP_TABLE, req);
}

void ShardedTable::clear() {
  ClearTable req;

  req.set_table(this->id);
  VLOG(2) << StringPrintf("Sending clear request (%d)", req.table());

  rpc::NetworkThread::Get()->SyncBroadcast(MTYPE_CLEAR_TABLE, req);
}

void ShardedTable::handleGetRequest(const HashGet& get_req,
    TableData *get_resp) {
  boost::recursive_mutex::scoped_lock sl(mutex_);

  int shard = get_req.shard();
  if (!isLocalShard(shard)) {
    LOG_EVERY_N(WARNING, 1000)
    << "Not local for shard: " << shard;
  }

  Table *t = dynamic_cast<Table*>(partitions_[shard]);
  if (!t->containsStr(get_req.key())) {
    get_resp->set_missing_key(true);
  } else {
    Arg *kv = get_resp->add_kv_data();
    kv->set_key(get_req.key());
    kv->set_value(t->getStr(get_req.key()));
  }
}

void ShardedTable::handlePutRequests() {
}

void ShardedTable::sendUpdates() {
  TableData put;
  boost::recursive_mutex::scoped_lock sl(mutex_);
  for (int i = 0; i < partitions_.size(); ++i) {
    Table *t = partitions_[i];
    TableCoder* c = NULL;

    if (!isLocalShard(i) && (partitionInfo(i)->dirty || !t->empty())) {
      // Always send at least one chunk, to ensure that we clear taint on
      // tables we own.
      do {
        put.Clear();

        if (!t->empty()) {
          ProtoTableCoder c(&put);
          TableIterator* it = t->iterator();
          for (; !it->done(); it->Next()) {
            c.write(it->keyStr(), it->valueStr());
          }
          t->clear();
        }

        put.set_shard(i);
        put.set_source(workerId_);
        put.set_table(id);
        put.set_epoch(-1);

        put.set_done(true);

        VLOG(3) << "Sending update for " << MP(t->id, t->shard) << " to "
                   << workerForShard(i) << " size " << put.kv_data_size();

        rpc::NetworkThread::Get()->Send(workerForShard(i) + 1,
            MTYPE_PUT_REQUEST, put);
      } while (!t->empty());

      VLOG(3) << "Done with update for " << MP(t->id, t->shard);
      t->clear();
    }
  }

  pendingWrites_ = 0;
}

int64_t ShardedTable::pendingWriteBytes() {
  int64_t s = 0;
  for (int i = 0; i < partitions_.size(); ++i) {
    Table *t = partitions_[i];
    if (!isLocalShard(i)) {
      s += t->size();
    }
  }

  return s;
}

void ShardedTable::swapLocal(ShardedTable *b) {
  CHECK(this != b);

  ShardedTable *mb = dynamic_cast<ShardedTable*>(b);
  std::swap(partInfo_, mb->partInfo_);
  std::swap(partitions_, mb->partitions_);
}

}
