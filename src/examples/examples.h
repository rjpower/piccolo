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
template<>
struct Marshal<Bucket> : public MarshalBase {
  static void marshal(const Bucket& t, string *out) {
    t.SerializePartialToString(out);
  }
  static void unmarshal(const StringPiece& s, Bucket* t) {
    t->ParseFromArray(s.data, s.len);
  }
};

class RunHelper {
public:
  virtual void setup(const ConfigData&) = 0;
  virtual void run(Master* m, const ConfigData&) = 0;
};

class RunnerRegistry {
public:
  typedef std::map<string, RunHelper*> Map;
  static Map& runners() {
    static Map m;
    return m;
  }
};

template<class C>
struct RunnerRegistrationHelper: public RunHelper {
  C* _instance;
  RunnerRegistrationHelper(const char* klass) {
    _instance = NULL;
    RunnerRegistry::runners()[klass] = this;
  }

  void setup(const ConfigData& c) {
    _instance = new C;
    _instance->setup(c);
  }

  void run(Master *m, const ConfigData& c) {
    _instance->run(m, c);
  }
};

} // namespace piccolo

#define REGISTER_RUNNER(klass)\
  static piccolo::RunnerRegistrationHelper<klass> run_helper_ ## klass(#klass);
