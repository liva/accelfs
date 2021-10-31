#pragma once
#include <stdio.h>
#include "rtc.h"

#define DEBUG_SHOW_LINE printf("%s:%d (func:%s)\n", __FILE__, __LINE__, __func__); fflush(stdout)
#define DEBUG_PRINTF(...) printf(__VA_ARGS__)
#define DEBUG(...) (...)

// should be placed at the last
#define EMBED_DEBUG_SIGNATURE int signature_ = 0xbeefcafe;
#define CHECK_DEBUG_SIGNATURE(that) if ((that).signature_ != 0xbeefcafe) { printf("signature check failure. signature at %p is %lx\n", &((that).signature_), (that).signature_); assert(false); abort(); }
// for move & destructor
#define CLEAR_DEBUG_SIGNATURE if (signature_ != 0xbeefcafe) { printf("signature check failure. signature at %p is %lx\n", &signature_, signature_); assert(false); abort(); } signature_ = -1;
