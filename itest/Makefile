CC=/opt/nec/nosupport/llvm-ve/bin/clang++
CFLAGS=--target=ve-linux -std=c++11 -DBSIZE=$(BSIZE) -O2 -g3 -DNDEBUG
LDFLAGS=-static -lvefs -lunvme -lsysve -lpthread -lc -lunwind

default: vefs vefs_recheck

vefs: vefs.cc
	$(CC) $(CFLAGS) -o vefs $^ $(LDFLAGS)

vefs_recheck: vefs_recheck.cc
	$(CC) $(CFLAGS) -o vefs_recheck $^ $(LDFLAGS)
