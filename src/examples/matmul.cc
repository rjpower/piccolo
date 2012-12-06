#include "examples/examples.h"
#include <cblas.h>

using namespace piccolo;

static int bRows = -1;
static int bCols = -1;

struct Block {
  float *d;

  Block() :
      d(new float[FLAGS_block_size * FLAGS_block_size]) {
  }
  Block(const Block& other) :
      d(new float[FLAGS_block_size * FLAGS_block_size]) {
    memcpy(d, other.d, sizeof(float) * FLAGS_block_size * FLAGS_block_size);
  }

  Block& operator=(const Block& other) {
    memcpy(d, other.d, sizeof(float) * FLAGS_block_size * FLAGS_block_size);
    return *this;
  }

  ~Block() {
    delete[] d;
  }
};

namespace piccolo {
template<>
struct Marshal<Block> : MarshalBase {
  void marshal(const Block& t, string *out) {
    out->assign((const char*) t.d,
        sizeof(float) * FLAGS_block_size * FLAGS_block_size);
  }

  void unmarshal(const StringPiece& s, Block *t) {
    CHECK_EQ(s.len, sizeof(float) * FLAGS_block_size * FLAGS_block_size);
    memcpy(t->d, s.data, s.len);
  }
};
}

static TableT<int, Block>* matrix_a = NULL;
static TableT<int, Block>* matrix_b = NULL;
static TableT<int, Block>* matrix_c = NULL;

struct BlockSum: public Accumulator<Block> {
  void Accumulate(Block *a, const Block& b) {
    for (int i = 0; i < FLAGS_block_size * FLAGS_block_size; ++i) {
      a->d[i] += b.d[i];
    }
  }
};

struct ShardHelper {
  ShardHelper(int mshard, int nshards) :
      numShards(nshards), my_shard(mshard) {
  }
  int numShards, my_shard;

  int block_id(int y, int x) {
    return (y * bCols + x);
  }

  bool is_local(int y, int x) {
    return block_id(y, x) % numShards == my_shard;
  }
};

struct MatrixMultiplication {
  void setup(const ConfigData& conf) {
    bCols = FLAGS_edge_size / FLAGS_block_size;
    bRows = FLAGS_edge_size / FLAGS_block_size;

    matrix_a = TableRegistry::sparse(bCols * bRows, new Sharding::Mod,
        new BlockSum);
    matrix_b = TableRegistry::sparse(bCols * bRows, new Sharding::Mod,
        new BlockSum);
    matrix_c = TableRegistry::sparse(bCols * bRows, new Sharding::Mod,
        new BlockSum);
  }

  static void init(TableT<int, Block>* table, int my_shard) {
    int numShards = matrix_a->numShards;

    ShardHelper sh(my_shard, numShards);

    Block b, z;
    for (int i = 0; i < FLAGS_block_size * FLAGS_block_size; ++i) {
      b.d[i] = 2;
      z.d[i] = 0;
    }

    int bcount = 0;

    for (int by = 0; by < bRows; by++) {
      for (int bx = 0; bx < bCols; bx++) {
        if (!sh.is_local(by, bx)) {
          continue;
        }
        ++bcount;
        matrix_a->update(sh.block_id(by, bx), b);
        matrix_b->update(sh.block_id(by, bx), b);
        matrix_c->update(sh.block_id(by, bx), z);
      }
    }
  }

  static void multiply(TableT<int, Block>* table, int my_shard) {
    Block a, b, c;

    int numShards = matrix_a->numShards;
    ShardHelper sh(my_shard, numShards);

    for (int k = 0; k < bRows; k++) {
      for (int i = 0; i < bRows; i++) {
        for (int j = 0; j < bCols; j++) {
          if (!sh.is_local(i, k)) {
            continue;
          }
          a = matrix_a->get(sh.block_id(i, k));
          b = matrix_b->get(sh.block_id(k, j));
          cblas_sgemm(CblasRowMajor, CblasNoTrans, CblasNoTrans,
              FLAGS_block_size, FLAGS_block_size, FLAGS_block_size, 1, a.d,
              FLAGS_block_size, b.d, FLAGS_block_size, 1, c.d,
              FLAGS_block_size);
          matrix_c->update(sh.block_id(i, j), c);
        }
      }
    }
  }

  static void print(TableT<int, Block>* table, int shard) {
    ShardHelper sh(shard, matrix_a->numShards);
    Block b = matrix_c->get(sh.block_id(0, 0));
    for (int i = 0; i < 5; ++i) {
      for (int j = 0; j < 5; ++j) {
        printf("%.2f ", b.d[FLAGS_block_size * i + j]);
      }
      printf("\n");
    }
  }

  void run(Master* m, const ConfigData& conf) {
    for (int i = 0; i < FLAGS_iterations; ++i) {
      matrix_a->run<&init>();
      matrix_a->run<&multiply>();
      matrix_c->run<&print>();
    }
  }
};
REGISTER_RUNNER(MatrixMultiplication);
