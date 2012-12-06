#ifndef KERNELREGISTRY_H_
#define KERNELREGISTRY_H_

#include "piccolo/table.h"
#include "util/common.h"

#include <boost/smart_ptr.hpp>
#include <boost/function.hpp>
#include <map>

namespace piccolo {

class ConfigData;
class TableBase;
class Master;
class Worker;


class Kernel {
public:
  ShardedTable* table(int id);

  template<class K, class V>
  TableT<K, V>* table(int id) {
    return dynamic_cast<TableT<K, V>*>(table(id));
  }

  virtual void run(ShardedTable* table, int shard);

private:
  friend class Worker;
  friend class Master;
};

struct KernelMaker {
  virtual Kernel* create() = 0;
};

class KernelRegistry {
public:
  typedef std::map<int, KernelMaker*> Map;
  static int add(KernelMaker* k) {
    int id = kernels.size();
    kernels[id] = k;
    return id;
  }

  static Kernel* create(int id) {
    return kernels[id]->create();
  }

  static Map kernels;
};

template<class K, class V, void(*MapFunction)(const K&, V&)>
class MapKernel: public Kernel {
public:
  typedef TableIteratorT<K, V> Iter;

  void run(TableT<K, V> *t) {
    Iter* iter = t->typedIterator();
    for (; !iter->done(); iter->Next()) {
      MapFunction(iter->key(), iter->value());
    }
    delete iter;
  }
};

template<class K, class V, void(*MapFunction)(const K&, V&)>
struct MapKernelRegister: public KernelMaker {
public:
  static int id;
  Kernel* create() {
    return new MapKernel<K, V, MapFunction>();
  }
};

template<class K, class V, void(*MapFunction)(const K&, V&)>
int MapKernelRegister<K, V, MapFunction>::id = KernelRegistry::add(
    new MapKernelRegister<K, V, MapFunction>);

} // namespace piccolo

#endif /* KERNELREGISTRY_H_ */
