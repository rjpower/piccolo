SRCDIR=../src
INCDIR=../include
OUTDIR=.
VPATH=${SRCDIR}:${OUTDIR}

CFLAGS:=${CFLAGS} -fPIC -O0 -ggdb2
CPPFLAGS:=${CPPFLAGS} -I${OUTDIR} -I${SRCDIR} -I${INCDIR} -I${SRCDIR}/external/google-logging -I${SRCDIR}/external/google-flags
CXXFLAGS:=${CXXFLAGS} -std=c++0x ${CFLAGS}
LDFLAGS=-L. -lprotobuf -lboost_thread

CXX=mpic++
PROTO=$(shell find ${SRCDIR}/ -name '*.proto')
PROTO_H := $(subst ../src/,,$(addsuffix .pb.h,$(basename ${PROTO})))


LIB_SRC:=util/stringpiece.cc\
		util/file.cc\
		util/rpc.cc\
		util/common.cc\
		util/static-initializers.cc\
		kernel.cc\
		table.cc\
		worker.cc\
		master.cc\
		piccolo.pb.cc\
		external/google-flags/gflags.cc\
		external/google-flags/gflags_reporting.cc\
		external/google-flags/gflags_nc.cc\
		external/google-logging/utilities.cc\
		external/google-logging/vlog_is_on.cc\
		external/google-logging/demangle.cc\
		external/google-logging/logging.cc\
		external/google-logging/symbolize.cc\
		external/google-logging/signalhandler.cc\
		external/google-logging/raw_logging.cc\
		external/webgraph/webgraph.cc\

LIB_OBJ:=$(subst .cc,.o,${LIB_SRC})

EXAMPLE_SRC := examples/examples.pb.cc\
	            examples/isort.cc\
				examples/n-body.cc\
				examples/main.cc\
				examples/pagerank.cc\
				examples/wordcount.cc\
				examples/simple-program.cc\
				examples/shortest-path.cc\
				examples/k-means.cc\
				examples/matmul.cc\

EXAMPLE_OBJ:=$(EXAMPLE_SRC:.cc=.o)

%.o : %.cc ${PROTO_H} 
	@mkdir -p $(dir $@)
	${CXX} ${CPPFLAGS} ${CXXFLAGS} -c -o $@ $<

%.pb.h %.pb.cc : %.proto
	protoc -I${SRCDIR} $< --cpp_out=./

all: dependencies example-driver piccolo.o

piccolo.o: ${LIB_OBJ}
	ld -r -o $@ $^

example-driver: ${EXAMPLE_OBJ} piccolo.o
	${CXX} -o $@ $(filter %.o, $^) ${LDFLAGS}

clean:
	# don't blow away the source directory in case the user accidently
	# runs make from there.
	cd .. && rm -rf build/
	
.PRECIOUS: %.pb.h %.pb.cc
.PHONY: all clean dependencies
