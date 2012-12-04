Piccolo - Distributed Tables for Distributed Programming
-------

## Overview

### PREREQUISITES

To build and use Piccolo, you will need a minimum of the following:

* Python (2.*)
* gcc/g++ (> 4)

If available, the following libraries will be used:

* Python development headers; SWIG
* TCMalloc
* google-perftools

In addition to these, Piccolo comes with several support libraries which are 
compiled as part of the build process; these are:

* google-flags
* google-logging


On debian/ubuntu, the required libraries can be acquired by running:

    sudo apt-get install\
     build-essential\
     cmake\
     g++\
     libboost-dev\
     libboost-python-dev\
     libboost-thread-dev\
     liblzo2-dev\
     libnuma-dev

Optional libraries can be install via:

    sudo apt-get install libgoogle-perftools-dev python-dev swig

