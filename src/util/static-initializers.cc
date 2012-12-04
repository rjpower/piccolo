#include "util/static-initializers.h"

#include <stdio.h>
#include <gflags/gflags.h>

using namespace std;

namespace piccolo {

typedef Registry<InitHelper>::Map InitMap;
typedef Registry<TestHelper>::Map TestMap;

void RunInitializers() {
//  fprintf(stderr, "Running %zd initializers... \n", helpers()->size());
  for (InitMap::iterator i = Registry<InitHelper>::get_map().begin();
      i != Registry<InitHelper>::get_map().end(); ++i) {
    i->second->Run();
  }
}

void RunTests() {
  fprintf(stderr, "Starting tests...\n");
  int c = 1;
  TestMap& m = Registry<TestHelper>::get_map();
  for (TestMap::iterator i = m.begin(); i != m.end(); ++i) {
    fprintf(stderr, "Running test %5d/%5zu: %s\n", c, m.size(), i->first.c_str());
    i->second->Run();
    ++c;
  }
  fprintf(stderr, "Done.\n");
}


}
