#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <vefs.h>

static const size_t bsize = BSIZE;

int main(const int argc, const char *argv[])
{
  Vefs *vefs = Vefs::Get();
  {
    Inode *inode = vefs->Create("/tmp/test", false);
    
    char *buf = (char *)malloc(bsize);
    char *buf2 = (char *)malloc(bsize);
    // append
    for(int i = 0; i < bsize / 8; i+=64) {
      ((uint64_t *)buf)[i] = rand();
    }
    
    vefs->Append(inode, (void *)(buf), 16);
    vefs->Append(inode, (void *)(buf + 16), 16);
    
    vefs->Read(inode, 0, 32, buf2);
    if (memcmp(buf, buf2, 16) != 0) {
      fprintf(stderr, "ERORR: append1 failed.\n");
      return -1;
    }
  }

  {
    Inode *inode = vefs->Create("/tmp/test2", false);
    for(int i = 0; i < 528 * 1024 + 2; i++) {
      vefs->Append(inode, (void *)&i, 4);
    }
    if (vefs->GetLen(inode) != 2112 * 1024 + 8) {
      fprintf(stderr, "ERROR: append2 failed.\n");
      return -1;
    }
  }

  printf("pretest passed.\n");
  return 0;
}
