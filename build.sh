#!/bin/bash

mkdir -p build/

if (pkg-config --exists python) then
	if (which swig > /dev/null) then
		echo Found SWIG
		swig -c++ -python -O -Isrc/ -o build/_piccolo.cc src/client/python/python_support.swig
		CPPFLAGS="$CPPFLAGS $(pkg-config python --cflags)"
		LDFLAGS="$CPPFLAGS $(pkg-config python --libs)"
	fi
fi

if [[ -f /usr/include/google/profiler.h ]]; then
	echo Enabling CPU profiler
	CPPFLAGS="$CPPFLAGS -DHAVE_CPU_PROFILER=1"
	LDFLAGS="$LDFLAGS -lprofiler"
fi

if [[ -f /usr/include/google/heap-profiler.h ]]; then
echo Enabling heap profiler
	CPPFLAGS="$CPPFLAGS -DHAVE_HEAP_PROFILER=1"
	LDFLAGS="$LDFLAGS -ltcmalloc"
fi

if [[ -f /usr/include/lzo/lzo1x.h ]]; then
	echo Enabling LZO
	CPPFLAGS="$CPPFLAGS -DHAVE_LZO1X=1"
fi

if (pkg-config --exists sdl) then
	echo Enabling SDL
	HAVE_SDL=1
	CPPFLAGS="$CPPFLAGS $(pkg-config sdl --cflags)"
	LDFLAGS="$CPPFLAGS $(pkg-config sdl --libs)" 
else
	HAVE_SDL=0
fi

export HAVE_SDL
export CPPFLAGS
export LDFLAGS

cd build/
make -f ../Makefile -r $*
