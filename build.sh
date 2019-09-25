#!/bin/sh -xe
docker build docker -t vefs-build-base:develop
docker run --rm -it -v $PWD:$PWD -w $PWD vefs-build-base:develop /opt/nec/nosupport/llvm-ve/bin/clang++ -O2 --target=ve-linux --std=c++11 -c vefs.cc
docker rm -f vefs || :
docker run -d --name vefs -it -v $PWD:$PWD -w $PWD vefs-build-base:develop sh
#docker exec -it vefs python -c 'import extract_archive; print extract_archive.extract_archive("/opt/nec/ve/lib/libvedio.a", "workdir")'
docker exec -it vefs rm -f libvefs.a
docker exec -it vefs /opt/nec/nosupport/llvm-ve/bin/llvm-ar rcs libvefs.a vefs.o
docker exec -it vefs cp -r vefs.h /opt/nec/ve/include
docker exec -it vefs cp -r vefs.h /opt/nec/ve/ex_include
docker exec -it vefs cp libvefs.a /opt/nec/ve/lib/
docker exec -it vefs cp libvefs.a /opt/nec/ve/ex_lib/
docker exec -it vefs cp vefs.cc /opt/nec/ve/ex_lib/
docker commit vefs vefs:develop
docker rm -f vefs

