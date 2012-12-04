#include "examples/examples.pb.h"

#include "piccolo.h"
#include "util/tuple.h"

using namespace piccolo;

DECLARE_int32(shards);
DECLARE_int32(iterations);
DECLARE_int32(block_size);
DECLARE_int32(edge_size);
DECLARE_bool(build_graph);

namespace piccolo {
template <>
struct Marshal<Bucket> : public MarshalBase {
  static void marshal(const Bucket& t, string *out) { t.SerializePartialToString(out); }
  static void unmarshal(const StringPiece& s, Bucket* t) { t->ParseFromArray(s.data, s.len); }
};
}
