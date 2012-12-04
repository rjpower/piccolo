#include <stdio.h>

#include "piccolo/kernel.h"
#include "piccolo/table.h"

namespace piccolo {

class Worker;
void KernelBase::initialize_internal(Worker* w, int table_id, int shard) {
  w_ = w;
  table_id_ = table_id;
  shard_ = shard;
}

ShardedTable* KernelBase::table(int id) {
  ShardedTable* t = (ShardedTable*)TableRegistry::table(id);
  CHECK_NE(t, (void*)NULL);
  return t;
}

KernelRegistry* KernelRegistry::Get() {
  static KernelRegistry* r = NULL;
  if (!r) { r = new KernelRegistry; }
  return r;
}

}
