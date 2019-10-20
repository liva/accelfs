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

template <class T, class U>
inline T align(T val, U blocksize)
{
    return (val / blocksize) * blocksize;
}
template <class T, class U>
inline T alignup(T val, U blocksize)
{
    return align(val + blocksize - 1, blocksize);
}
template <class T, class U>
inline T getblocknum_from_size(T size, U blocksize)
{
    return (size + blocksize - 1) / blocksize;
}

//#define nvme_printf(...) printf(__VA_ARGS__)
#define nvme_printf(...)

//#define debug_printf(...) printf(__VA_ARGS__)
#define debug_printf(...)

static const bool kRedirect = false;

static const size_t kChunkSize = 2L * 1024 * 1024 /* * 1024*/;
