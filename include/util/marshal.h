#ifndef UTIL_MARSHAL_H_
#define UTIL_MARSHAL_H_

#include "glog/logging.h"
#include "util/stringpiece.h"

#include <boost/type_traits.hpp>
#include <boost/utility.hpp>
#include <string>

namespace piccolo {

struct MarshalBase {};

template <class T, class Enable = void>
struct Marshal : public MarshalBase {
  virtual ~Marshal() {}
  virtual void marshal(const T& t, std::string* out) {
    GOOGLE_GLOG_COMPILE_ASSERT(boost::is_pod<T>::value, Invalid_Value_Type);
    out->assign(reinterpret_cast<const char*>(&t), sizeof(t));
  }

  virtual void unmarshal(const StringPiece& s, T *t) {
    GOOGLE_GLOG_COMPILE_ASSERT(boost::is_pod<T>::value, Invalid_Value_Type);
    *t = *reinterpret_cast<const T*>(s.data);
  }
};

template <class T>
struct Marshal<T, typename boost::enable_if<boost::is_base_of<std::string, T> >::type> : public MarshalBase {
  void marshal(const std::string& t, std::string *out) { *out = t; }
  void unmarshal(const StringPiece& s, std::string *t) { t->assign(s.data, s.len); }
};

template <class T>
struct Marshal<T, typename boost::enable_if<boost::is_base_of<google::protobuf::Message, T> >::type> : public MarshalBase {
  void marshal(const google::protobuf::Message& t, std::string *out) { t.SerializePartialToString(out); }
  void unmarshal(const StringPiece& s, google::protobuf::Message* t) { t->ParseFromArray(s.data, s.len); }
};

template <class T>
void marshal(const T& t, std::string* out) {
  Marshal<T> m;
  m.marshal(t, out);
}

template <class T>
std::string marshal(const T& t) {
  Marshal<T> m;
  std::string out;
  m.marshal(t, &out);
  return out;
}


template <class T>
T unmarshal(const StringPiece& s) {
  Marshal<T> m;
  T out;
  m.unmarshal(s, &out);
  return out;
}

template <class T>
void unmarshal(const StringPiece& s, T* v) {
  Marshal<T> m;
  m.unmarshal(s, v);
}

}

#endif /* MARSHAL_H_ */
