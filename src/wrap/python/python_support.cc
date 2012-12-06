#include "glog/logging.h"
#include "python_support.h"
#include <boost/python.hpp>

using namespace boost::python;
using namespace google::protobuf;
using namespace std;

DEFINE_double(crawler_runtime, -1, "Amount of time to run, in seconds.");
DEFINE_bool(crawler_triggers, false, "Use trigger-based crawler (t/f).");

namespace piccolo {

Kernel* kernel() { return the_kernel; }

double crawler_runtime() {
  return FLAGS_crawler_runtime;
}

bool crawler_triggers() {
  return FLAGS_crawler_triggers;
}

int PythonSharder::operator()(const string& k, int shards) {
  PyObjectPtr result = PyEval_CallFunction(c_, "(si)", k.c_str(), shards);
  if (PyErr_Occurred()) {
    PyErr_Print();
    exit(1);
  }

  long r = PyInt_AsLong(result);
//  Py_DecRef(result);
  return r;
}

void PythonAccumulate::Accumulate(PyObjectPtr* a, const PyObjectPtr& b) {
  PyObjectPtr result = PyEval_CallFunction(c_, "OO", *a, b);
  if (PyErr_Occurred()) {
    PyErr_Print();
    exit(1);
  }

  *a = result;
}

template<class K, class V>
PythonTrigger<K, V>::PythonTrigger(ShardedTable* thistable, const string& codeshort, const string& codelong) {
  Init(thistable);
  params_.put("python_code_short", codeshort);
  params_.put("python_code_long", codelong);
  //trigid = thistable->register_trigger(this);
}

template<class K, class V>
void PythonTrigger<K, V>::Init(ShardedTable* thistable) {
  try {
    object sys_module = import("sys");
    object sys_ns = sys_module.attr("__dict__");
    crawl_module_ = import("crawler");
    crawl_ns_ = crawl_module_.attr("__dict__");
  } catch (error_already_set& e) {
    PyErr_Print();
    exit(1);
  }
}

template<class K, class V>
bool PythonTrigger<K, V>::LongFire(const K k, bool lastrun) {
  string python_code = params_.get<string> ("python_code_long");
  PyObject *key, *callable;
  callable = PyObject_GetAttrString(crawl_module_.ptr(), python_code.c_str());
  key = PyString_FromString(k.c_str());

  // Make sure all the callfunctionobjarg arguments are fine
  if (key == NULL || callable == NULL) {
    LOG(ERROR) << "Failed to launch trigger " << python_code << "!";
    if (key == NULL) LOG(ERROR) << "[FAIL] key was null";
    if (callable == NULL) LOG(ERROR) << "[FAIL] callable was null";
    return true;
  }

  Py_INCREF(callable);
  Py_INCREF(key);

  V dummyv;
  bool rv = PythonTrigger<K, V>::CallPythonTrigger(callable, key, &dummyv, dummyv, true, lastrun); //hijack "isNew" for "lastrun"

  Py_DECREF(key);
  Py_DECREF(callable);

  //LOG(INFO) << "returning " << (rv?"TRUE":"FALSE") << " from trigger";
  return rv;
}

template<class K, class V>
bool PythonTrigger<K, V>::ProcessPyRetval(PyObjectPtr retval) {
  if (NULL == retval) {
    PyErr_Print();
    LOG(FATAL) << "Attempting to flush and die because Trigger/Accumulator returned non-bool/non-int.";
  }
  if (!PyBool_Check(retval) && !PyInt_Check(retval)) {
    PyErr_SetString(PyExc_RuntimeError,"Trigger returned neither bool nor int!");
    PyErr_Print();
    LOG(FATAL) << "Python trigger returned invalid value";
  }
  return (retval == Py_True)?true:((retval == Py_False)?false:(bool)PyInt_AsLong(retval));
}

template<class K, class V>
bool PythonTrigger<K, V>::Accumulate(V* value, const V& newvalue) {
  string python_code = params_.get<string> ("python_code_short");
  PyObject *callable;
  callable = PyObject_GetAttrString(crawl_module_.ptr(), python_code.c_str());

  // Make sure all the callfunctionobjarg arguments are fine
  if (callable == NULL) {
    LOG(ERROR) << "Failed to launch trigger " << python_code << "!";
    LOG(ERROR) << "[FAIL] callable was null";
    return false;
  }

  Py_INCREF(callable);

  bool rv = PythonTrigger<K, V>::CallPythonTrigger(callable, NULL, value, newvalue, false, false);

  Py_DECREF(callable);

  //LOG(INFO) << "returning " << (rv?"TRUE":"FALSE") << " from trigger-connected accumulator";
  return rv;
}

template<class K, class V>
bool PythonTrigger<K, V>::CallPythonTrigger(PyObjectPtr callable, PyObjectPtr key, V* value, const V& newvalue, bool isTrigger, bool isNewOrLast) {
  LOG(FATAL) << "No such CallPythonTrigger for this key/value pair type!";
  exit(1);
}

template<>
bool PythonTrigger<string, int64_t>::CallPythonTrigger(PyObjectPtr callable, PyObjectPtr key, int64_t* value, const int64_t& update, bool isTrigger, bool isLast) {
  PyObjectPtr retval;
  PyGILState_STATE gstate;

  //LOG(ERROR) << "Handling Python trigger call type " << isTrigger << " on string:int64_t table";
  //Handle LongTriggers
  if (isTrigger) {
    PyObject* lastrun_obj = PyBool_FromLong((long)isLast);
    if (lastrun_obj == NULL) {
      LOG(ERROR) << "Failed to bootstrap <int,int> trigger (trigger) launch";
      return true;
    }
    try {

      gstate = PyGILState_Ensure();
      retval = PyObject_CallFunctionObjArgs(callable, key, lastrun_obj, NULL);

    } catch (error_already_set& e) {
      PyErr_Print();
      PyGILState_Release(gstate);
      LOG(FATAL) << "Fatal Python error aborted Oolong";
    }
    if (PyErr_Occurred()) {
      PyErr_Print();
      LOG(FATAL) << "Fatal Python error aborted Oolong";
    }
    PyGILState_Release(gstate);
    return ProcessPyRetval(retval);
  }
 
  PyObject* cur_obj = PyLong_FromLongLong(*value);
  PyObject* upd_obj = PyLong_FromLongLong(update);

  if (cur_obj == NULL || upd_obj == NULL) {
    LOG(ERROR) << "Failed to bootstrap <int,int> trigger (accumulator) launch";
    return true;
  }
  try {

    gstate = PyGILState_Ensure();
    retval = PyEval_CallFunction(
               callable, "OO", cur_obj, upd_obj);

  } catch (error_already_set& e) {
    PyErr_Print();
    PyGILState_Release(gstate);
    LOG(FATAL) << "Fatal Python error aborted Oolong";
  }
  if (PyErr_Occurred()) {
    PyErr_Print();
    PyGILState_Release(gstate);
    LOG(FATAL) << "Fatal Python error aborted Oolong";
  }

  if (retval == NULL || !PyTuple_Check(retval) || 2 != PyTuple_GET_SIZE(retval)) {
    PyErr_SetString(PyExc_RuntimeError,"Accum/Trigger must return 2-item tuple");
    PyErr_Print();
    PyGILState_Release(gstate);
    LOG(FATAL) << "Python accum/trigger returned invalid value";
  }
 
  *value = PyLong_AsLongLong(PyTuple_GetItem(retval,0));

  PyGILState_Release(gstate);
  return ProcessPyRetval(PyTuple_GetItem(retval,1));
}

template<>
bool PythonTrigger<string, string>::CallPythonTrigger(PyObjectPtr callable, PyObjectPtr key, string* value, const string& update, bool isTrigger, bool isLast) {
  PyObjectPtr retval;
  PyGILState_STATE gstate;

  //LOG(ERROR) << "Handling Python trigger call type " << isTrigger << " on string:string table";
  //Handle LongTriggers
  if (isTrigger) {
    PyObject* lastrun_obj = PyBool_FromLong((long)isLast);
    if (lastrun_obj == NULL) {
      LOG(ERROR) << "Failed to bootstrap <int,int> trigger (trigger) launch";
      return true;
    }
    try {

      gstate = PyGILState_Ensure();
      retval = PyObject_CallFunctionObjArgs(callable, key, lastrun_obj, NULL);

    } catch (error_already_set& e) {
      PyErr_Print();
      PyGILState_Release(gstate);
      LOG(FATAL) << "Fatal Python error aborted Oolong";
    }
    if (PyErr_Occurred()) {
      PyErr_Print();
      PyGILState_Release(gstate);
      LOG(FATAL) << "Fatal Python error aborted Oolong";
    }

    PyGILState_Release(gstate);
    return ProcessPyRetval(retval);
  }
 
  PyObject* cur_obj = PyString_FromStringAndSize(value->c_str(),value->length());
  PyObject* upd_obj = PyString_FromStringAndSize(update.c_str(),update.length());

  if (cur_obj == NULL || upd_obj == NULL) {
    LOG(ERROR) << "Failed to bootstrap <string,string> trigger (accumulator) launch";
    return true;
  }
  try {

    gstate = PyGILState_Ensure();
    retval = PyObject_CallFunctionObjArgs(
               callable, cur_obj, upd_obj, NULL);
    
  } catch (error_already_set& e) {
    PyErr_Print();
    PyGILState_Release(gstate);
    LOG(FATAL) << "Fatal Python error aborted Oolong";
  }

  if (PyErr_Occurred()) {
    PyErr_Print();
    PyGILState_Release(gstate);
    LOG(FATAL) << "Fatal Python error aborted Oolong";
  }

  if (retval == NULL || !PyTuple_Check(retval) || 2 != PyTuple_GET_SIZE(retval)) {
    PyErr_SetString(PyExc_RuntimeError,"Accum/Trigger must return 2-item tuple");
    PyErr_Print();
    PyGILState_Release(gstate);
    LOG(FATAL) << "Python accum/trigger returned invalid value";
  }
 
  *value = PyString_AsString(PyTuple_GetItem(retval,0));

  PyGILState_Release(gstate);
  return ProcessPyRetval(PyTuple_GetItem(retval,1));
}

class PythonKernel: public Kernel {
public:
  bool is_swapaccum(void) { return false; }
  virtual ~PythonKernel() {}
  PythonKernel() {
    try {
      object sys_module = import("sys");
      object sys_ns = sys_module.attr("__dict__");
      exec("path += ['src/examples/crawler']", sys_ns, sys_ns);
      exec("path += ['bin/release/examples/']", sys_ns, sys_ns);
      exec("path += ['bin/debug/examples/']", sys_ns, sys_ns);

      crawl_module_ = import("crawler");
      crawl_ns_ = crawl_module_.attr("__dict__");
    } catch (error_already_set e) {
      PyErr_Print();
      exit(1);
    }
  }

  void run_python_code() {
    if (!PyEval_ThreadsInitialized()) {
      PyEval_InitThreads();
      PyEval_ReleaseLock(); //cf. SO question 5140998 answer #1
      LOG(INFO) << "Initializing Python C threading safety support";
    } else {
      LOG(INFO) << "Python C threading safety support already initialized";
    }

    the_kernel = this;
    string python_code = get_arg<string> ("python_code");
    LOG(INFO) << "Executing python code: " << python_code;
    try {
      exec(StringPrintf("%s\n", python_code.c_str()).c_str(), crawl_ns_, crawl_ns_);
    } catch (error_already_set e) {
      PyErr_Print();
      exit(1);
    }
  }

  void swap_python_accumulator() {
    LOG(FATAL) << "Swapping accumulators in Python not yet implemented.";
//    the_kernel = this;
//    string python_code = get_arg<string> ("python_accumulator");
//    LOG(INFO) << "Swapping python accumulator: " << python_code;
//    try {
//    } catch (error_already_set e) {
//      PyErr_Print();
//      exit(1);
//    }
  }

private:
  object crawl_module_;
  object crawl_ns_;
};
REGISTER_KERNEL(PythonKernel)
;
REGISTER_METHOD(PythonKernel, run_python_code)
;
REGISTER_METHOD(PythonKernel, swap_python_accumulator)
;

template class PythonTrigger<string, string>;
template class PythonTrigger<string, int64_t>;

} //End namespace piccolo
