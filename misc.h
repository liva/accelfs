#pragma once

#include <unvme.h>
#include <unvme_nvme.h>
#include <vepci.h>
#include <stdint.h>
#include <stdlib.h>
#include <assert.h>

extern uint64_t tmp_var;
static inline uint64_t ve_gettime()
{
    uint64_t ret;
    void *vehva = ((void *)0x000000001000);
    asm volatile("lhm.l %0,0(%1)"
                 : "=r"(ret)
                 : "r"(vehva));
    return ((uint64_t)1000 * ret) / 800;
}

//#define nvme_printf(...) printf(__VA_ARGS__)
#define nvme_printf(...)

//#define debug_printf(...) printf(__VA_ARGS__)
#define debug_printf(...)

static const size_t kChunkSize = 2L * 1024 * 1024 /* * 1024*/;
