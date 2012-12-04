#include "examples/examples.h"
#include "external/webgraph/webgraph.h"

#include <algorithm>
#include <libgen.h>

using namespace piccolo;
using namespace std;

#define PREFETCH 512
static float TOTALRANK = 0;
static int NUM_WORKERS = 2;

static const float kPropagationFactor = 0.8;

DEFINE_string(graph_prefix, "", "Path to web graph.");
DEFINE_int32(nodes, 10000, "");

static float powerlaw_random(float dmin, float dmax, float n) {
  float r = (float) random() / RAND_MAX;
  return pow((pow(dmax, n) - pow(dmin, n)) * pow(r, 3.f) + pow(dmin, n),
      1.f / n);
}

static float random_restart_seed() {
  return (1 - kPropagationFactor) * (TOTALRANK / FLAGS_nodes);
}

// I'd like to use a pair here, but for some reason they fail to count
// as POD types according to C++.  Sigh.
struct PageId {
  int64_t site :32;
  int64_t page :32;
};

bool operator==(const PageId& a, const PageId& b) {
  return a.site == b.site && a.page == b.page;
}

std::ostream& operator<<(std::ostream& s, const PageId& a) {
  return s << a.site << ":" << a.page;
}

size_t hash_value(const PageId& p) {
  return SuperFastHash((const char*) &p, sizeof p);
}

struct SiteSharding: public Sharder<PageId> {
  int operator()(const PageId& p, int nshards) {
    return p.site % nshards;
  }
};

//Tables in use
TableT<PageId, float>* curr_pr;
TableT<PageId, float>* next_pr;
TableT<uint64_t, Page> *pages;

struct Pagerank {
  void setup(const ConfigData& conf) {
    NUM_WORKERS = conf.num_workers();
    TOTALRANK = FLAGS_nodes;

    curr_pr = TableRegistry::sparse(FLAGS_shards, new SiteSharding,
        new Accumulators<float>::Sum);
    next_pr = TableRegistry::sparse(FLAGS_shards, new SiteSharding,
        new Accumulators<float>::Sum);
  }

  void run(Master* m, const ConfigData& conf) {
    next_pr->reserve((int) (2 * FLAGS_nodes));
    curr_pr->reserve((int) (2 * FLAGS_nodes));

    m->map(pages, [](TableT<PageId, Page>* t) {
      TableIteratorT<uint64_t, Page> *it = pages->typedIterator();
      for (; !it->done(); it->Next()) {
        Page& n = it->value();
        struct PageId p = {n.site(), n.id()};
        next_pr->update(p, random_restart_seed());

        float v = curr_pr->get(p, 0);

        float contribution = kPropagationFactor * v / n.target_site_size();
        for (int i = 0; i < n.target_site_size(); ++i) {
          PageId target = {n.target_site(i), n.target_id(i)};
          if (!(target == p)) {
            next_pr->update(target, contribution);
          }
        }
      }
    });

    // Move the values computed from the last iteration into the current table.
    curr_pr->swap(next_pr);
    next_pr->clear();
  }
};
