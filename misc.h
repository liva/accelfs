#pragma once

#include <unvme.h>
#include <unvme_nvme.h>
#include <vepci.h>
#include <stdint.h>
#include <stdlib.h>
#include <assert.h>
#include <atomic>
#include <vector>
#include "_misc.h"
#include "spinlock.h"

static const bool kRedirect = false;
static const bool kHeaderDump = false;
static const bool kDebugTime = true;

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
extern uint64_t tmp_var;
static inline uint64_t ve_gettime_debug()
{
    if (kDebugTime)
    {
        return ve_gettime();
    }
    else
    {
        return 0;
    }
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

//#define vefs_printf(...) printf(__VA_ARGS__)
#define vefs_printf(...)

class UnvmeWrapper
{
public:
    UnvmeWrapper() : ns_(unvme_open("b3:00.0")), lock_(0)
    {
        printf("%s qc=%d/%d qs=%d/%d bc=%#lx bs=%d maxbpio=%d\n", ns_->device, ns_->qcount,
               ns_->maxqcount, ns_->qsize, ns_->maxqsize, ns_->blockcount,
               ns_->blocksize, ns_->maxbpio);
    }
    ~UnvmeWrapper()
    {
        if (memleak_count_ != 0)
        {
            printf("!!!memory leak detected! (%zu bytes)\n", memleak_count_);
            fflush(stdout);
        }
        unvme_close(ns_);
    }
    void HardWrite()
    {
        Spinlock lock(lock_);
        vfio_dma_t *dma = unvme_alloc(ns_, ns_->blocksize); // dummy
        u32 cdw10_15[6];                                    // dummy
        int stat = unvme_cmd(ns_, 0, NVME_CMD_FLUSH, ns_->id, dma->buf, 512, cdw10_15, 0);
        if (stat)
        {
            printf("failed to sync");
        }
        unvme_free(ns_, dma);
    }
    u16 GetBlockSize()
    {
        return ns_->blocksize;
    }
    u64 GetBlockCount()
    {
        return ns_->blockcount;
    }
    int Apoll(unvme_iod_t iod)
    {
        uint64_t endtime = ve_gettime() + 1000L * 1000 * 1000;
        while (true)
        {
            if (ve_gettime() > endtime)
            {
                return -1;
            }
            {
                Spinlock lock(lock_);
                if (unvme_apoll(iod, 0) == 0)
                {
                    return 0;
                }
            }
            sched_yield();
        }
    }
    int ApollWithoutWait(unvme_iod_t iod)
    {
        Spinlock lock(lock_);
        if (unvme_apoll(iod, 0) == 0)
        {
            return 0;
        }
        return -1;
    }
    int Read(void *buf, u64 slba, u32 nlb)
    {
        Spinlock lock(lock_);
        return unvme_read(ns_, 0, buf, slba, nlb);
    }
    unvme_iod_t Aread(void *buf, u64 slba, u32 nlb)
    {
        Spinlock lock(lock_);
        return unvme_aread(ns_, 0, buf, slba, nlb);
    }
    unvme_iod_t Awrite(const void *buf, u64 slba, u32 nlb)
    {
        Spinlock lock(lock_);
        return unvme_awrite(ns_, 0, buf, slba, nlb);
    }
    vfio_dma_t *AllocChunk()
    {
        Spinlock lock(lock_);
        memleak_count_ += kChunkSize;
        return unvme_alloc(ns_, kChunkSize);
    }
    int Free(vfio_dma_t *dma)
    {
        Spinlock lock(lock_);
        memleak_count_ -= dma->size;
        return unvme_free(ns_, dma);
    }

private:
    void Lock()
    {
        while (lock_.fetch_or(1) == 1)
        {
            asm volatile("" ::
                             : "memory");
        }
    }
    void Unlock()
    {
        lock_ = 0;
    }
    const unvme_ns_t *ns_;
    std::atomic<int> lock_;
    size_t memleak_count_ = 0;
};

class HeaderBuffer
{
public:
    HeaderBuffer()
    {
    }
    void ResetPos()
    {
        pos_ = 0;
    }
    void AppendFromBuffer(HeaderBuffer &buf)
    {
        AppendRaw(buf.buf_.data() + buf.pos_, buf.buf_.size() - buf.pos_);
        buf.pos_ = buf.buf_.size();
    }
    void AlignPos()
    {
        pos_ = ((pos_ + 3) / 4) * 4;
    }
    template <class T>
    void AppendRaw(T *data, size_t len)
    {
        buf_.resize(pos_ + len);
        memcpy(buf_.data() + pos_, data, len);
        pos_ += len;
    }
    template <class T>
    void Append(T i)
    {
        AppendRaw(&i, sizeof(T));
    }
    template <class T>
    bool CompareRaw(T *data, size_t len)
    {
        if (pos_ + len > buf_.size())
        {
            fprintf(stderr, "header is not terminated\n");
            exit(1);
        }
        if (memcmp(buf_.data() + pos_, data, len) == 0)
        {
            pos_ += len;
            return true;
        }
        return false;
    }
    template <class T>
    bool Compare(T i)
    {
        return CompareRaw(&i, sizeof(i));
    }
    std::string GetString()
    {
        std::string str = std::string(buf_.data() + pos_);
        pos_ += ((str.length() + 1 + 3) / 4) * 4;
        if (pos_ > buf_.size())
        {
            fprintf(stderr, "header is not terminated\n");
            exit(1);
        }
        return str;
    }
    template <class T>
    T Get()
    {
        if (pos_ + sizeof(T) > buf_.size())
        {
            fprintf(stderr, "header is not terminated\n");
            exit(1);
        }
        T i = *(reinterpret_cast<T *>(buf_.data() + pos_));
        pos_ += sizeof(T);
        return i;
    }
    size_t Output(void *buf, size_t maxsize)
    {
        size_t size;
        if (maxsize >= buf_.size() - pos_)
        {
            // output all
            size = buf_.size() - pos_;
        }
        else
        {
            size = maxsize;
        }
        memcpy(buf, buf_.data() + pos_, size);
        pos_ += size;
        return size;
    }

    //private:
    int pos_ = 0;
    std::vector<char> buf_;
};

extern uint64_t t1, t2, t3, t4;