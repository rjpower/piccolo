#ifndef STATIC_INITIALIZERS_H
#define STATIC_INITIALIZERS_H

#include <string>
#include <map>

using namespace std;

namespace piccolo {

// There exists a separate registry mapping for each value type.
template<class T>
class Registry {
public:
  typedef map<string, T*> Map;

  static void put(const string& k, T *t) {
    get_map()[k] = t;
  }
  static T *get(const string& k) {
    return get_map()[k];
  }
  static Map& get_map() {
    if (!m_) {
      m_ = new Map;
    }
    return *m_;
  }
private:
  static Map *m_;
};

template<class T> map<string, T*>* Registry<T>::m_;

template<class T>
struct RegistryHelper {
  RegistryHelper(const string& k, T* t) {
    Registry<T>::put(k, t);
  }
};

struct CodeHelper {
  virtual ~CodeHelper() {
  }
  virtual void Run() = 0;
};

class TestHelper: public CodeHelper {
};
class InitHelper: public CodeHelper {
};

extern void RunInitializers();
extern void RunTests();

}

#define REGISTER(type, k, v) static piccolo::RegistryHelper<type> rhelper_ ## k (#k, v);
#define REGISTER_CODE(type, k, code)\
struct k ## CodeHelper : public type {\
  virtual ~ k ## CodeHelper() {} \
  void Run() { code; }\
};\
REGISTER(type, k, new k ## CodeHelper);

#define REGISTER_TEST(k, code) REGISTER_CODE(piccolo::TestHelper, k, code);
#define REGISTER_INITIALIZER(k, code) REGISTER_CODE(piccolo::InitHelper, k, code);

#endif
