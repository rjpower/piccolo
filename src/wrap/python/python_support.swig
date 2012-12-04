%module piccolo

#include "kernel/table.h"
#include "kernel/table-inl.h"
#include "kernel/global-table.h"
#include "client/python/python_support.h"

using namespace std;

struct PyObject;

namespace google {
  namespace protobuf {
    typedef int int32;
    typedef long int64;
  }
}

%typemap(in) long& { $1 = PyInt_AsLong($input); }
%typemap(out) long& { $result = PyInt_FromLong(*$1); }

%typemap(in) long { $1 = PyInt_AsLong($input); }
%typemap(out) long { $result = PyInt_FromLong($1); }

%typemap(in) PyObject* { $1 = $input; Py_IncRef($1); }
%typemap(out) PyObject* { $result = $1; }

%typemap(in) PyObject*& { $1 = $input; Py_IncRef($1); }
%typemap(out) PyObject*& { $result = *$1; }

%typemap(in) const PyObject*& { $1 = $input; Py_IncRef($1); }
%typemap(out) const PyObject*& { $result = *$1; }


%typemap(in) (int argc, char **argv) {
  if (PyList_Check($input)) {
    int i;
    $1 = PyList_Size($input);
    $2 = (char **) malloc(($1+1)*sizeof(char *));
    for (i = 0; i < $1; i++) {
      PyObject *o = PyList_GetItem($input,i);
      if (PyString_Check(o))
        $2[i] = PyString_AsString(PyList_GetItem($input,i));
      else {
        PyErr_SetString(PyExc_TypeError,"list must contain strings");
        free($2);
        return NULL;
      }
    }
    $2[i] = 0;
  } else {
    PyErr_SetString(PyExc_TypeError,"not a list");
    return NULL;
  }
}

%typemap(freearg) (int argc, char **argv) {
  free((char *) $2);
}

namespace std {
  %typemap(in) string& (string temp) {
    if (PyString_Check($input)) {
      temp = PyString_AsString($input);
      $1 = &temp;
     } else{
       PyErr_SetString(PyExc_TypeError,"Expected a string type!");
       return NULL; 
     }
  }
  
  %typemap(in) string (string temp) {
    if (PyString_Check($input)) {
      $1 = PyString_AsString($input);
     } else{
       PyErr_SetString(PyExc_TypeError,"Expected a string type!");
       return NULL; 
     }
  }

  %typemap(out) string { 
    $result = PyString_FromString(($1).c_str());
  }
  
  %typemap(out) string& { 
    $result = PyString_FromString((*$1).c_str());
  }
}

%{

#include "client/python/python_support.h"
#include "kernel/table.h"
#include "kernel/table-inl.h"
#include "kernel/global-table.h"

namespace piccolo {
typedef TypedGlobalTable<string, int64_t> IntTable;
typedef TypedGlobalTable<string, string> StringTable;

typedef TypedTableIterator<string, int64_t> IntIterator;
typedef TypedTableIterator<string, string> StringIterator;

//typedef Trigger<string, int64_t> IntTrigger;
//typedef Trigger<string, string> StringTrigger;

typedef PythonTrigger<string, int64_t> IntPythonTrigger;
typedef PythonTrigger<string, string> StringPythonTrigger;

}

%}

#define GOOGLE_PROTOBUF_VERSION 2003000
#define LIBPROTOBUF_EXPORT

typedef long int64_t;

%include "util/rpc.h"

%include "kernel/kernel.h"
%include "kernel/table.h"
%include "kernel/table-registry.h"
%include "kernel/table-inl.h"
%include "kernel/global-table.h"

%include "master/master.h"
%include "client/python/python_support.h"
//typedef int TriggerID;

void Init(int argc, char** argv);

%warnfilter(401) SwigDomainSharding;
%inline %{
  namespace piccolo {
  struct IntAccum { 
    static Accumulator<int64_t>* Min() { return new piccolo::Accumulators<int64_t>::Min; }
    static Accumulator<int64_t>* Max() { return new piccolo::Accumulators<int64_t>::Max; }
    static Accumulator<int64_t>* Sum() { return new piccolo::Accumulators<int64_t>::Sum; }
    static Accumulator<int64_t>* Replace() { return new piccolo::Accumulators<int64_t>::Replace; }
  };
  
  struct StringAccum { 
    static Accumulator<string>* Min() { return new piccolo::Accumulators<string>::Min; }
    static Accumulator<string>* Max() { return new piccolo::Accumulators<string>::Max; }
    static Accumulator<string>* Sum() { return new piccolo::Accumulators<string>::Sum; }
    static Accumulator<string>* Replace() { return new piccolo::Accumulators<string>::Replace; }
  };

  struct IntTrigger {
    static HybridTrigger<string,int64_t>* Null() { return new piccolo::Triggers<string,int64_t>::NullTrigger; }
    static HybridTrigger<string,int64_t>* ReadOnly() { return new piccolo::Triggers<string,int64_t>::ReadOnlyTrigger; }
    static HybridTrigger<string,int64_t>* PythonCode(const string& codeshort, const string& codelong) { return new IntPythonTrigger(0,codeshort,codelong); }
  };

  struct StringTrigger {
    static HybridTrigger<string,string>* Null() { return new piccolo::Triggers<string,string>::NullTrigger; }
    static HybridTrigger<string,string>* ReadOnly() { return new piccolo::Triggers<string,string>::ReadOnlyTrigger; }
    static HybridTrigger<string,string>* PythonCode(const string& codeshort, const string& codelong) { return new StringPythonTrigger(0,codeshort,codelong); }
  };
  
  static Sharding::String* StringSharding() { return new Sharding::String; }
  static Sharding::Mod* ModSharding() { return new Sharding::Mod; }
  static Sharding::UintMod* UintModSharding() { return new Sharding::UintMod; }
  
  struct SwigDomainSharding : public Sharder<string> {    //SWIG stupidly complains about this; couldn't find a fix. :(
    int operator()(const string& in, int num_shards) {
      int d_end = in.find(" ");
      d_end = (d_end > 0)?d_end:in.length();
      //printf("hashing %s (length %d) -> %u/%d = %u\n",in.data(),d_end, SuperFastHash(in.data(), d_end), num_shards,SuperFastHash(in.data(), d_end) % num_shards);
      return SuperFastHash(in.data(), d_end) % num_shards;
    }
  };
  
  static Sharder<string>* DomainSharding() { return new SwigDomainSharding; }

  }
%};

%rename(__assign__) *::operator=;	//Workaround for SWIG "Operator= ignored" issue
namespace piccolo {

class ConfigData {
public:
  ConfigData();
  virtual ~ConfigData();
  ConfigData(const ConfigData& from);
  ConfigData& operator=(const ConfigData& from);
  
  int num_workers() const;
  void set_num_workers(int value);

  int worker_id() const;
  void set_worker_id(int value);

  int master_id() const;
  void set_master_id(int value);
};

class RecordFile {
public:
  enum CompressionType {
    NONE = 0,
    LZO = 1
  };

  RecordFile(const string& path, const string& mode, int compression=NONE);
  ~RecordFile();

  void write(const google::protobuf::Message &m);
  bool read(google::protobuf::Message *m);
  const char* name();

  bool eof();
  void sync();
};

bool StartWorker(const ConfigData& conf);

/*
template <class K, class V>
struct TypedTableIterator {
private:
  TypedIterator();
public:
  void key_str(string *out);
  void value_str(string *out);
  bool done();
  void Next();

  const K& key();
  V& value();
}; 
*/

struct PythonAccumulate {
  PythonAccumulate(PyObjectPtr callback) : c_(callback) {}
  void Accumulate(PyObjectPtr* a, const PyObjectPtr& b);
private:
  PyObjectPtr c_;
};

template <class K, class V>
struct Trigger<K, V> {
private:
  Trigger();
public:
  Accumulator(GlobalTable* thistable);
  Trigger(GlobalTable* thistable);
  Init(GlobalTable* thistable);
  bool Accumulate(V& value, const V& update);
  bool LongFire(const K& k, bool isNew);
};

template <class K, class V>
struct PythonTrigger<K, V> {
public:
  PythonTrigger(GlobalTable* thistable, const string& codeshort, const string& codelong);
  void Init(GlobalTable* thistable);
  bool Accumulate(V& value, const V& update);
  bool LongFire(const K& k, bool isNew);
  bool CallPythonTrigger(PyObjectPtr callable, PyObjectPtr key, const V& current, V& update);
};

template <class K, class V> 
struct TypedGlobalTable<K, V>  {
private:
  TypedGlobalTable();
public:
  
  V get(const K& k);
  bool contains(const K& k);
  void update (const K& k, const V&);
  void enqueue_update(const K& k, const V&);
  void put(const K& k, const V&);
  
  void enable_retrigger(K k);

  TypedTableIterator<K, V>* get_iterator(int shard);
  
  void SendUpdates();
  void HandlePutRequests();

  %extend {
    // Register a trigger
    void py_swap_accumulator(const string& codeshort, const string& codelong) {
		LOG(FATAL) << "Python may not yet swap accumulators" << endl;
        return;
    }
  }
};

%template(IntTable) TypedGlobalTable<string, int64_t>;
%template(StringTable) TypedGlobalTable<string, string>;

%template(IntIterator) TypedTableIterator<string, int64_t>;
%template(StringIterator) TypedTableIterator<string, string>;

%extend KernelBase {
  TypedGlobalTable<string, string>* GetStringTable(int idx) { return $self->get_table<string, string>(idx); }
  TypedGlobalTable<string, int64_t>* GetIntTable(int idx) { return $self->get_table<string, int64_t>(idx); }
}

%template(CreateStringTable) CreateTable<string, string>;
%template(CreateIntTable) CreateTable<string, int64_t>;

%extend Master {
  // Run the given kernel function on all worker nodes.
  #define py_run_all(code, locality) { \
    RunDescriptor r("PythonKernel", "run_python_code", locality); \
    r.params.put<string>("python_code", code); \
    r.shards = range(locality->num_shards()); \
    $self->run(r); \
  }
  void py_run_all_stringtable(const string& code, TypedGlobalTable<string, string>* locality) {
    py_run_all(code, locality);
  }
  void py_run_all_inttable(const string& code, TypedGlobalTable<string, int64_t>* locality) {
    py_run_all(code, locality);
  }

  // Run the given kernel function on one (arbitrary) worker node.
  #define py_run_one(code, locality) { \
    RunDescriptor r("PythonKernel", "run_python_code", locality); \
    r.params.put<string>("python_code", code); \
    r.shards = range(1); \
    $self->run(r); \
  }
  void py_run_one_stringtable(const string& code, TypedGlobalTable<string, string>* locality) {
    py_run_one(code, locality);
  }
  void py_run_one_inttable(const string& code, TypedGlobalTable<string, int64_t>* locality) {
    py_run_one(code, locality);
  }

}

} //End namespace piccolo
