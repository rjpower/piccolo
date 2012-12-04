#ifndef FILE_H_
#define FILE_H_

#include "boost/noncopyable.hpp"
#include "util/common.h"
#include "util/stringpiece.h"

#include <string>
#include <vector>

#include <glob.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#ifdef HAVE_LZO
#include <lzo/lzo1x.h>
#endif

namespace google {
namespace protobuf {
class Message;
}
}

namespace piccolo {

class File {
public:
  virtual ~File() {
  }
  virtual int read(char *buffer, int len) = 0;
  virtual bool read_line(std::string *out) = 0;
  virtual bool eof() = 0;
  virtual void seek(int64_t pos) = 0;
  virtual uint64_t tell() = 0;
  virtual const char* name() {
    return "";
  }
  virtual void sync() = 0;

  int write_string(const std::string& buffer) {
    return write(buffer.data(), buffer.size());
  }

  virtual int write(const char* buffer, int len) = 0;

  std::string readLine() {
    std::string out;
    read_line(&out);
    return out;
  }

  struct Info {
    std::string name;
    struct stat stat;
  };

  static std::string Slurp(const std::string& file);
  static void Dump(const std::string& file, StringPiece data);
  static void Mkdirs(std::string path);
  static std::vector<std::string> MatchingFilenames(StringPiece glob);
  static std::vector<Info> MatchingFileinfo(StringPiece glob);

  static bool Exists(const std::string& path);
  static void Move(const std::string& src, const std::string&dst);
private:
};

class LocalFile: public File {
public:
  LocalFile(FILE* fp);
  LocalFile(const std::string& path, const std::string& mode);
  virtual ~LocalFile() {
    if (close_on_delete) {
      fflush(fp);
      fclose(fp);
    }
  }

  void sync() {
    fsync(fileno(fp));
  }

  bool read_line(std::string *out);
  int read(char *buffer, int len);
  int write(const char* buffer, int len);
  void seek(int64_t pos) {
    fseek(fp, pos, SEEK_SET);
  }
  uint64_t tell() {
    return ftell(fp);
  }

  void Printf(const char* p, ...);
  virtual FILE* filePointer() {
    return fp;
  }

  const char* name() {
    return path.c_str();
  }

  bool eof();

private:
  FILE* fp;
  std::string path;
  bool close_on_delete;
};

class Encoder {
public:
  Encoder(std::string *s) :
      out_(s), out_f_(NULL) {
  }
  Encoder(File *f) :
      out_(NULL), out_f_(f) {
  }

  template<class T>
  void write(const T& v);

  void write_string(StringPiece v);
  void write_bytes(StringPiece s);
  void write_bytes(const char *a, int len);

  std::string *data() {
    return out_;
  }

  size_t pos() {
    if (out_) {
      return out_->size();
    }
    return out_f_->tell();
  }

private:
  std::string *out_;
  File *out_f_;
};

class Decoder {
private:
  const std::string* src_;
  const char* src_p_;
  File* f_src_;

  int pos_;
public:
  Decoder(const std::string& data) :
      src_(&data), src_p_(data.data()), f_src_(NULL), pos_(0) {
  }
  Decoder(File* f) :
      src_(NULL), src_p_(NULL), f_src_(f), pos_(0) {
  }

  template<class V> void read(V* t) {
    if (src_) {
      memcpy(t, src_p_ + pos_, sizeof(V));
      pos_ += sizeof(V);
    } else {
      f_src_->read((char*) t, sizeof(t));
    }
  }

  template<class V> V read() {
    V v;
    read<V>(&v);
    return v;
  }

  void read_bytes(char* b, int len) {
    if (src_p_) {
      memcpy(b, src_p_ + pos_, len);
    } else {
      f_src_->read(b, len);
    }

    pos_ += len;
  }

  void read_string(std::string* v) {
    uint32_t len;
    read<uint32_t>(&len);
    v->resize(len);

    read_bytes(&(*v)[0], len);
  }

  bool done() {
    if (src_) {
      return pos_ == src_->size();
    } else {
      return f_src_->eof();
    }
  }

  size_t pos() {
    if (src_) {
      return pos_;
    }
    return f_src_->tell();
  }

  void seek(int p) {
    if (src_) {
      pos_ = p;
    } else {
      pos_ = p;
      f_src_->seek(pos_);
    }
  }
};

}

#endif /* FILE_H_ */
