#include "piccolo/table.h"
#include "piccolo/table-inl.h"
#include "util/tuple.h"

namespace piccolo {

TableRegistry::Map TableRegistry::tables;

ProtoTableCoder::ProtoTableCoder(TableData* t) :
    t_(t), pos_(0) {
}

void ProtoTableCoder::write(StringPiece key, StringPiece value) {
  Arg* a = t_->add_kv_data();
  a->set_key(key.data, key.size());
  a->set_value(value.data, value.size());
}

bool ProtoTableCoder::read(string* key, string* value) {
  if (pos_ < t_->kv_data_size()) {
    *key = t_->kv_data(pos_).key();
    *value = t_->kv_data(pos_).value();
    ++pos_;
    return true;
  }
  return false;
}

RemoteIterator::RemoteIterator(ShardedTable *table, int shard) :
    owner_(table), shard_(shard), done_(false) {
  request_.set_table(table->id());
  request_.set_shard(shard_);
  request_.set_row_count(kReadAhead);
  int target_worker = table->workerForShard(shard);
  rpc::NetworkThread::Get()->Call(target_worker + 1, MTYPE_ITERATOR, request_,
      &response_);
  request_.set_id(response_.id());
  pos_ = 0;
}

void RemoteIterator::Next() {
  int target_worker = dynamic_cast<ShardedTable*>(owner_)->workerForShard(
      shard_);
  if (pos_ == response_.row_count() - 1) {
    CHECK(!response_.done());
    rpc::NetworkThread::Get()->Call(target_worker + 1, MTYPE_ITERATOR, request_,
        &response_);
    if (response_.row_count() < 1 && !response_.done())
      LOG(ERROR)<< "Call to server requesting " << request_.row_count()
      << " rows returned " << response_.row_count() << " rows.";
    pos_ = 0;
  } else {
    ++pos_;
  }
}

bool RemoteIterator::done() {
  return response_.done() && pos_ == response_.row_count();
}

void RemoteIterator::valueStr(string* out) {
  *out = response_.value(pos_);
}

void RemoteIterator::keyStr(string* out) {
  *out = response_.key(pos_);
}

}
