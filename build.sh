#!/bin/bash -lxe
#
# Copyright 2020 NEC Laboratories Europe GmbH
# All rights reserved
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#    1. Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#
#    2. Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
# 
#    3. Neither the name of NEC Laboratories Europe GmbH nor the names of its
#       contributors may be used to endorse or promote products derived
#       from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY NEC Laboratories Europe GmbH AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL NEC Laboratories 
# Europe GmbH OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO,  PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#

#OPTION="-g3 -O2 -DNDEBUG"
OPTION="-g3 -O2" # debug
echo "#ifndef VEFS_AUTOGEN_CONF_H_" > vefs_autogen_conf.h
echo "#define VEFS_AUTOGEN_CONF_H_" >> vefs_autogen_conf.h
for OPT in ${1}
do
    echo "#define ${OPT}" >> vefs_autogen_conf.h
done
echo "#endif" >> vefs_autogen_conf.h
docker run --rm -it -v $PWD:$PWD -w $PWD unvme:ve /opt/nec/nosupport/llvm-ve/bin/clang++ --target=ve-linux --std=c++11 $OPTION -c vefs.cc
docker rm -f vefs || :
docker run -d --name vefs -it -v $PWD:$PWD -w $PWD unvme:ve sh
#docker exec -it vefs python -c 'import extract_archive; print extract_archive.extract_archive("/opt/nec/ve/lib/libvedio.a", "workdir")'
docker exec -it vefs rm -f libvefs.a
docker exec -it vefs /opt/nec/nosupport/llvm-ve/bin/llvm-ar rcs libvefs.a vefs.o
docker exec -it vefs cp -r *.h /opt/nec/ve/include
docker exec -it vefs cp -r *.h /opt/nec/ve/ex_include
docker exec -it vefs mkdir -p /opt/nec/ve/include/common /opt/nec/ve/ex_include/common
docker exec -it vefs cp -r common/*.h /opt/nec/ve/include/common
docker exec -it vefs cp -r common/*.h /opt/nec/ve/ex_include/common
docker exec -it vefs cp libvefs.a /opt/nec/ve/lib/
docker exec -it vefs cp libvefs.a /opt/nec/ve/ex_lib/
docker exec -it vefs cp vefs.cc /opt/nec/ve/ex_lib/
docker commit vefs vefs:develop
docker rm -f vefs
rm -rf vefs_autogen_conf.h
cd itest
#./run.sh
