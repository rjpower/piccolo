#ifndef PICCOLO_H_
#define PICCOLO_H_

#include "util/common.h"
#include "util/file.h"
#include "util/timer.h"

#include "piccolo/worker.h"
#include "piccolo/master.h"
#include "piccolo/kernel.h"
#include "piccolo/table.h"
#include "piccolo/table-inl.h"
#ifndef SWIG
DECLARE_int32(shards);
DECLARE_int32(iterations);
#endif

#endif /* PICCOLO_H_ */
