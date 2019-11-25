#pragma once

#include "misc.h"
#include <atomic>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include "spinlock.h"
#include <unvme.h>
#include <unvme_nvme.h>

class UnvmeWrapper
{
public:
    UnvmeWrapper() : ns_(unvme_open("b3:00.0")), lock_(0)
    {
        printf("qc=%d/%d qs=%d/%d bc=%#lx bs=%d maxbpio=%d\n", ns_->qcount,
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
    int AreadChunk(u64 lba, vfio_dma_t *&dma, unvme_iod_t &iod)
    {
        dma = Alloc(kChunkSize);
        if (!dma)
        {
            printf("allocation failure\n");
            exit(1);
        }
        u32 bnum = kChunkSize / GetBlockSize();
        iod = Aread(dma->buf, lba, bnum);
        if (!iod)
        {
            fprintf(stderr, "r^ %ld %d\n", lba, bnum);
            fprintf(stderr, "VEFS: read failed\n");
            return -1;
        }
        return 0;
    }
    unvme_iod_t Aread(void *buf, u64 slba, u32 nlb)
    {
        Spinlock lock(lock_);
        return AreadInternal(buf, slba, nlb);
    }
    unvme_iod_t AreadInternal(void *buf, u64 slba, u32 nlb)
    {
        assert(Spinlock::IsAcquired(lock_));
        return unvme_aread(ns_, 0, buf, slba, nlb);
    }
    unvme_iod_t Awrite(const void *buf, u64 slba, u32 nlb)
    {
        Spinlock lock(lock_);
        return AwriteInternal(buf, slba, nlb);
    }
    unvme_iod_t AwriteInternal(const void *buf, u64 slba, u32 nlb)
    {
        assert(Spinlock::IsAcquired(lock_));
        return unvme_awrite(ns_, 0, buf, slba, nlb);
    }
    vfio_dma_t *Alloc(size_t size)
    {
        Spinlock lock(lock_);
        return AllocInternal(size);
    }
    vfio_dma_t *AllocInternal(size_t size)
    {
        assert(Spinlock::IsAcquired(lock_));
        if (size > 2 * 1024 * 1024)
        {
            printf("unvme dma memory allocation failure.\n");
            abort();
        }
        vfio_dma_t *dma;
        dma = unvme_alloc(ns_, size);
        memleak_count_ += dma->size;
        return dma;
    }
    int Free(vfio_dma_t *dma)
    {
        Spinlock lock(lock_);
        return FreeInternal(dma);
    }
    int FreeInternal(vfio_dma_t *dma)
    {
        assert(Spinlock::IsAcquired(lock_));
        memleak_count_ -= dma->size;
        return unvme_free(ns_, dma);
    }
    std::atomic<int> &GetLockFlag()
    {
        return lock_;
    }

private:
    const unvme_ns_t *ns_;
    std::atomic<int> lock_;
    size_t memleak_count_ = 0;
};

class DmaBufferWrapper
{
public:
    DmaBufferWrapper(UnvmeWrapper &ns_wrapper, size_t size) : ns_wrapper_(ns_wrapper), buf_(ns_wrapper_.AllocInternal(size)), size_(size)
    {
        cnt_ = 1;
        if (!buf_)
        {
            printf("allocation failure\n");
            exit(1);
        }
    }
    vfio_dma_t *GetBuffer()
    {
        return buf_;
    }
    size_t GetSize()
    {
        return size_;
    }
    ~DmaBufferWrapper()
    {
        assert(cnt_ == 0);
        ns_wrapper_.Free(buf_);
    }
    void Ref()
    {
        cnt_++;
    }
    void Unref()
    {
        cnt_--;
        if (cnt_ == 0)
        {
            delete this;
        }
    }

private:
    UnvmeWrapper &ns_wrapper_;
    vfio_dma_t *buf_;
    size_t size_;
    int cnt_;
};

class SharedDmaBuffer
{
public:
    SharedDmaBuffer() : wrapper_(nullptr)
    {
    }
    SharedDmaBuffer(UnvmeWrapper &ns_wrapper, size_t size) : wrapper_(new DmaBufferWrapper(ns_wrapper, size))
    {
    }
    SharedDmaBuffer(SharedDmaBuffer &&obj)
    {
        wrapper_ = obj.wrapper_;
        obj.wrapper_ = nullptr;
    }
    SharedDmaBuffer(const SharedDmaBuffer &obj) : wrapper_(obj.wrapper_)
    {
        if (wrapper_ != nullptr)
        {
            wrapper_->Ref();
        }
    }
    SharedDmaBuffer &operator=(const SharedDmaBuffer &obj)
    {
        if (wrapper_ != nullptr)
        {
            wrapper_->Unref();
        }
        wrapper_ = obj.wrapper_;
        if (wrapper_ != nullptr)
        {
            wrapper_->Ref();
        }
        return *this;
    }
    SharedDmaBuffer &operator=(SharedDmaBuffer &&obj)
    {
        if (wrapper_ != nullptr)
        {
            wrapper_->Unref();
        }
        wrapper_ = obj.wrapper_;
        obj.wrapper_ = nullptr;
        return *this;
    }
    ~SharedDmaBuffer()
    {
        if (wrapper_ != nullptr)
        {
            wrapper_->Unref();
        }
    }
    void *GetBuffer()
    {
        assert(wrapper_ != nullptr);
        return wrapper_->GetBuffer()->buf;
    }
    size_t GetSize()
    {
        return wrapper_->GetSize();
    }

private:
    DmaBufferWrapper *wrapper_;
};