#include "piccolo.h"
#include "examples.h"

using namespace piccolo;

DEFINE_int64(num_clusters, 2, "");
DEFINE_int64(num_points, 100, "");
DEFINE_bool(dump_results, false, "");

struct Point {
  float x, y;
  float min_dist;
  int source;
};

struct Cluster {
  float x, y;
};

static TableT<int32_t, Point> *points;
static TableT<int32_t, Cluster> *clusters;
static TableT<int32_t, Cluster> *actual;

Cluster random_cluster() {
  Cluster c = { (float) (0.5 - rand_float()), (float) (0.5 - rand_float()) };
  return c;
}

struct ClusterAccum: public Accumulator<Cluster> {
  void Accumulate(Cluster* c1, const Cluster& c2) {
    Cluster o = { c1->x + c2.x, c1->y + c2.y };
    *c1 = o;
  }
};

struct KMeans {
  void setup(const ConfigData& conf) {
    clusters = TableRegistry::sparse(conf.num_workers() * 4, new Sharding::Mod,
        new ClusterAccum);
    points = TableRegistry::sparse(conf.num_workers() * 4, new Sharding::Mod,
        new Accumulators<Point>::Replace);
    actual = TableRegistry::sparse(conf.num_workers() * 4, new Sharding::Mod,
        new Accumulators<Cluster>::Replace);
  }

  static void initialize(TableT<int32_t, Point>* points, int shard) {
    const int numShards = points->numShards;
    for (int64_t i = shard; i < FLAGS_num_points; i += numShards) {
      Cluster c = actual->get(i % FLAGS_num_clusters);
      Point p = {c.x + 0.1f * (rand_float() - 0.5f), c.y + 0.1f * (rand_float() - 0.5f), -1, 0};
      points->update(i, p);
    }
  }

  static void updatePoints(const int32_t& key, Point& p) {
    p.min_dist = 2;
    for (int i = 0; i < FLAGS_num_clusters; ++i) {
      const Cluster& c = clusters->get(i);
      double d_squared = pow(p.x - c.x, 2) + pow(p.y - c.y, 2);
      if (d_squared < p.min_dist) {
        p.min_dist = d_squared;
        p.source = i;
      }
    }
  }

  static void resetClusters(const int32_t& key, Cluster& c) {
    if (c.x == 0 && c.y == 0) {
      Point p = points->get(random() % FLAGS_num_points);
      c.x = p.x; c.y = p.y;
    } else {
      c.x = 0; c.y = 0;
    }
  }

  static void updateClusters(const int32_t& key, Point& p) {
    Cluster c = {p.x * FLAGS_num_clusters / FLAGS_num_points,
      p.y * FLAGS_num_clusters / FLAGS_num_points};

    clusters->update(p.source, c);
  }

  void run(Master* m, const ConfigData& conf) {
    for (int i = 0; i < FLAGS_num_clusters; ++i) {
      actual->update(i, random_cluster());
      clusters->update(i, random_cluster());
    }

    points->reserve(FLAGS_num_points);
    points->run<initialize>();

    for (int i = 0; i < 10; ++i) {
      points->map<updatePoints>();
      clusters->map<resetClusters>();
      points->map<updateClusters>();
    }
  }
};

REGISTER_RUNNER(KMeans);
