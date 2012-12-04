#include "examples/examples.h"

using namespace piccolo;

static const float kCutoffRadius = 1.0;
static const float kTimestep = 1e-6;

// Each box is 1*1*1
static const int kBoxSize = (int) ceil(kCutoffRadius);

// A partition is 4*4*4 boxes.
static const int kPartitionSize = 20;

// World is a cube of boxes.
static const int kWorldSize = 20;
static const int kNumPartitions = kWorldSize / (kPartitionSize * kBoxSize);

DEFINE_int64(particles, 1000000, "");

struct pos {
  int id;
  float x, y, z;

  pos() :
      x(0), y(0), z(0) {
    id = -1;
  }
  pos(float nx, float ny, float nz) :
      id(0), x(nx), y(ny), z(nz) {
  }
  pos(int nid, float nx, float ny, float nz) :
      id(nid), x(nx), y(ny), z(nz) {
  }
  pos(const pos& b) :
      id(b.id), x(b.x), y(b.y), z(b.z) {
  }

  // Find the partition corresponding to this point
  static int shard_for_pos(const pos& p) {
    const pos& grid = p.get_box() / kPartitionSize;
    return int(grid.z) * kNumPartitions * kNumPartitions
        + int(grid.y) * kNumPartitions + int(grid.x);
  }

  static pos pos_for_shard(int id) {
    int z = id / (kNumPartitions * kNumPartitions);
    int y = (id % (kNumPartitions * kNumPartitions)) / kNumPartitions;
    int x = id % kNumPartitions;

    return pos(x, y, z) * kPartitionSize;
  }

  uint32_t hash() const {
    return uint32_t(z * kWorldSize * kWorldSize) + uint32_t(y * kWorldSize)
        + uint32_t(x);
  }

  // Return the upper left corner of the box containing this position.
  pos get_box() const {
    return pos(int(x / kBoxSize) * kBoxSize, int(y / kBoxSize) * kBoxSize,
        int(z / kBoxSize) * kBoxSize);
  }

  bool out_of_bounds() {
    return x < 0 || y < 0 || z < 0 || x >= kWorldSize || y >= kWorldSize
        || z >= kWorldSize;
  }

  bool operator==(const pos& b) const {
    return x == b.x && y == b.y && z == b.z;
  }

  bool operator!=(const pos& b) const {
    return !(*this == b);
  }

  pos operator*(float d) {
    return pos(id, x * d, y * d, z * d);
  }
  pos operator/(float d) {
    return pos(id, x / d, y / d, z / d);
  }
  pos& operator+=(const pos& b) {
    x += b.x;
    y += b.y;
    z += b.z;
    return *this;
  }
  pos& operator-=(const pos& b) {
    x -= b.x;
    y -= b.y;
    z -= b.z;
    return *this;
  }

  pos operator+(const pos& b) const {
    return pos(id, x + b.x, y + b.y, z + b.z);
  }
  pos operator-(const pos& b) const {
    return pos(id, x - b.x, y - b.y, z - b.z);
  }

  float magnitude_squared() {
    return x * x + y * y + z * z;
  }
  float magnitude() {
    return sqrt(magnitude_squared());
  }
};

size_t hash_value(const pos& k) {
  return k.hash();
}

typedef boost::unordered_set<pos> PosSet;

namespace std {
static ostream & operator<<(ostream &out, const pos& p) {
  out << MP(p.id, p.x, p.y, p.z);
  return out;
}
}

namespace piccolo {

template<>
struct Marshal<pos> : public MarshalBase {
  static void marshal(const pos& t, string *out) {
    out->append((char*) &t, sizeof(pos));
  }
  static void unmarshal(const StringPiece& s, pos* t) {
    *t = *(pos*) (s.data);
  }
};

template<>
struct Marshal<PosSet> : public MarshalBase {
  static void marshal(const PosSet& t, string *out) {
    for (PosSet::const_iterator i = t.begin(); i != t.end(); ++i) {
      out->append(reinterpret_cast<const char*>(&(*i)), sizeof(pos));
    }
  }

  static void unmarshal(const StringPiece& s, PosSet* t) {
    t->clear();
    const pos *p = reinterpret_cast<const pos*>(s.data);
    for (int i = 0; i < s.len / sizeof(pos); ++i) {
      t->insert(*p);
    }
  }
};

struct PosSharding: public Sharder<pos> {
  int operator()(const pos& p, int shards) {
    return pos::shard_for_pos(p);
  }
};

struct SetAccum: public Accumulator<PosSet> {
  void Accumulate(PosSet* a, const PosSet& b) {
    a->insert(b.begin(), b.end());
  }
};

static pos kZero(0, 0, 0);
static TableT<pos, PosSet> *curr;
static TableT<pos, PosSet> *next;

class NBodyKernel: public DSMKernel {
public:
  virtual ~NBodyKernel() {
  }
  boost::unordered_map<pos, PosSet> cache;
  PosSet scratch;

  PosSet& get_set(pos pt) {
    scratch.clear();
    scratch.insert(pt);
    return scratch;
  }

  void CreatePoints() {
    pos ul = pos::pos_for_shard(currentShard());

    int pid = 0;
    // Create randomly distributed particles for each box inside of this shard
    for (int dx = 0; dx < kPartitionSize; ++dx) {
      for (int dy = 0; dy < kPartitionSize; ++dy) {
        for (int dz = 0; dz < kPartitionSize; ++dz) {
          int num_points = std::max(1,
              int(FLAGS_particles / pow(kWorldSize, 3)));
          pos b = ul + pos(dx, dy, dz) * kBoxSize;
          for (int i = 0; i < num_points; ++i) {
            pos pt = b
                + pos(rand_double() * kBoxSize, rand_double() * kBoxSize,
                    rand_double() * kBoxSize);
            pt.id = pid++;

            curr->update(pt.get_box(), get_set(pt));
          }
        }
      }
    }
  }

  pos compute_force(pos p1, pos p2) {
    float dist = (p1 - p2).magnitude_squared();
    if (dist > kCutoffRadius) {
      return kZero;
    }
    if (dist < 1e-8) {
      return kZero;
    }

    ++interaction_count;
    return (p1 - p2) / dist;
  }

  void compute_update(pos box, const PosSet& points) {
    PosSet neighbors;
    for (int dx = -1; dx <= 1; ++dx) {
      for (int dy = -1; dy <= 1; ++dy) {
        for (int dz = -1; dz <= 1; ++dz) {
          pos bk = box + pos(dx, dy, dz);
          bk = bk.get_box();
          if (bk.out_of_bounds()) {
            continue;
          }

          if (cache.find(bk) == cache.end()) {
            cache[bk] = curr->get(bk);
          }
          neighbors.insert(cache[bk].begin(), cache[bk].end());
        }
      }
    }

    for (PosSet::const_iterator i = points.begin(); i != points.end(); ++i) {
      pos a = *i;
      pos a_force = kZero;

      for (PosSet::iterator j = neighbors.begin(); j != neighbors.end(); ++j) {
        const pos& b = *j;
        a_force += compute_force(a, b);
      }

      a = a + (a_force * kTimestep);
      if (a.get_box().out_of_bounds()) {
        continue;
      }

      LOG_EVERY_N(INFO, 10000) << "Update: " << a;
      next->update(a.get_box(), get_set(a));
    }
  }

  void Simulate() {
    cache.clear();

    // Iterate over each box in this partition.
    TableIteratorT<pos, PosSet>* it = curr->typedIterator();

    for (int count = 0; !it->done(); ++count) {
//      LOG(INFO) << it->key() << " : " << it->value().size();
      interaction_count = 0;
      const pos& box_pos = it->key();
      compute_update(box_pos, it->value());
      it->Next();
    }
    delete it;
  }

  int interaction_count;
};
}

struct NBody {
  void setup(const ConfigData& conf) {
    curr = TableRegistry::sparse(
        kNumPartitions * kNumPartitions * kNumPartitions, new PosSharding,
        new SetAccum);
    next = TableRegistry::sparse(
        kNumPartitions * kNumPartitions * kNumPartitions, new PosSharding,
        new SetAccum);
  }

  void run(Master* m, const ConfigData& conf) {
    m->run(curr, &NBodyKernel::CreatePoints);
    for (int i = 0; i < FLAGS_iterations; ++i) {
      LOG(INFO)<< "Running iteration: " << MP(i, FLAGS_iterations);
      m->run(curr, &NBodyKernel::CreatePoints);

      curr->clear();
      curr->swap(next);
    }
  }
};
