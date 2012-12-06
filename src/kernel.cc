#include <stdio.h>

#include "piccolo/kernel.h"
#include "piccolo/table.h"

namespace piccolo {

ShardedTable* Kernel::table(int id) {
  ShardedTable* t = (ShardedTable*)TableRegistry::table(id);
  CHECK_NE(t, (void*)NULL);
  return t;
}

KernelRegistry::Map KernelRegistry::kernels;

}
