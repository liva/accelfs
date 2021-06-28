#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <vefs.h>

int main(const int argc, const char *argv[])
{
  Vefs *vefs = Vefs::Get();
  Inode *inode = vefs->Create("/tmp/test2", false);

  if (vefs->GetLen(inode) != 2112 * 1024 + 8) {
    fprintf(stderr, "ERROR: close check1 failed(len = %lu).\n", vefs->GetLen(inode));
    return -1;
  }
  for(int i = 0; i < 528 * 1024 + 2; i++) {
    int buf = 0;
    vefs->Read(inode, i * 4, 4, (char *)&buf);
    if (i != buf) {
      fprintf(stderr, "ERROR: close check2 failed.\n");
      return -1;
    }
  }
  printf("test passed.\n");
  return 0;
}
