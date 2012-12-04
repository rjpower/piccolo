AUTOMAKE_OPTIONS = subdir-objects
THRIFT_COMPILER = ${top_builddir}/thrift/compiler/cpp/thrift
THRIFT_FLAGS = --gen cpp:cob_style,templates,dense -I ${srcdir}

ACLOCAL_AMFLAGS = ${ACLOCAL_FLAGS} -I m4

AM_CPPFLAGS = \
  -I${builddir}/gen-cpp \
  -I${srcdir} \
  -I${srcdir}/google-flags \
  -I${srcdir}/google-logging \
  -I${top_srcdir}/thrift/lib/cpp/src \
  -I${top_srcdir}/thrift/lib/cpp/src/thrift

AM_CXXFLAGS = \
  -Wno-unused-result

lib_LTLIBRARIES = libglogging.la libgflags.la libpiccolo.la

# Run thrift compiler prior to building anything
BUILT_SOURCES = \
  piccolo/gen-cpp/common_types.h
  
piccolo/gen-cpp/common_types.h: piccolo/util/common.thrift
	mkdir -p $(dir $@)
	${THRIFT_COMPILER} ${THRIFT_FLAGS} -o piccolo/ $<

libglogging_la_SOURCES = \
  google-logging/logging.cc\
  google-logging/utilities.cc\
  google-logging/symbolize.cc\
  google-logging/vlog_is_on.cc\
  google-logging/raw_logging.cc\
  google-logging/signalhandler.cc\
  google-logging/demangle.cc

libgflags_la_SOURCES = \
  google-flags/gflags.cc\
  google-flags/gflags_reporting.cc\
  google-flags/gflags_nc.cc

libpiccolo_la_SOURCES = \
  piccolo/worker/worker.cc\
  piccolo/kernel/kernel.cc\
  piccolo/util/common.cc\
  piccolo/util/marshal.cc\
  piccolo/util/file/file.cc\
  piccolo/util/file/lzo-file.cc\
  piccolo/util/stringpiece.cc\
  piccolo/util/static-initializers.cc\
  piccolo/master/master.cc
  
