#!/bin/sh -xe
OPTION="-g3 -O2"
#OPTION="-g3"
docker run --rm -it -v $PWD/..:$PWD/.. -w $PWD unvme:ve /opt/nec/nosupport/llvm-ve/bin/clang++ --std=c++11 $OPTION static_allocator.cc
./a.out
