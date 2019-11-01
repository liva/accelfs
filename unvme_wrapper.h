#pragma once

#include "misc.h"
#include <atomic>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include "spinlock.h"
#include <unvme.h>
#include <unvme_nvme.h>

#include <map>

class UnvmeWrapper
{
public:
    UnvmeWrapper() : ns_(unvme_open("b3:00.0")), lock_(0), alloc_lock_(0)
    {
        printf("%s qc=%d/%d qs=%d/%d bc=%#lx bs=%d maxbpio=%d\n", ns_->device, ns_->qcount,
               ns_->maxqcount, ns_->qsize, ns_->maxqsize, ns_->blockcount,
               ns_->blocksize, ns_->maxbpio);
    }
    ~UnvmeWrapper()
    {
        for (auto it = chunk_pool_.begin(); it != chunk_pool_.end(); ++it)
        {
            unvme_free(ns_, *it);
        }
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
        {
            Spinlock lock(alloc_lock_);
            memleak_count_ += kChunkSize;
            if (!chunk_pool_.empty())
            {
                auto it = chunk_pool_.end();
                --it;
                vfio_dma_t *dma = *it;
                chunk_pool_.pop_back();
                return dma;
            }
        }
        {
            Spinlock lock(lock_);
            return unvme_alloc(ns_, kChunkSize);
        }
    }
    int Free(vfio_dma_t *dma)
    {
        std::vector<vfio_dma_t *> free_dma;
        {
            Spinlock lock(alloc_lock_);
            memleak_count_ -= dma->size;
            if (dma->size == kChunkSize)
            {
                chunk_pool_.push_back(dma);
            }
            else
            {
                Spinlock lock(lock_);
                return unvme_free(ns_, dma);
            }
            if (chunk_pool_.size() > 64)
            {
                for (int i = 0; i < chunk_pool_.size() - 32; i++)
                {
                    auto it = chunk_pool_.end();
                    --it;
                    free_dma.push_back(*it);
                    chunk_pool_.pop_back();
                }
            }
        }
        if (!free_dma.empty())
        {
            Spinlock lock(lock_);
            for (auto it = free_dma.begin(); it != free_dma.end(); ++it)
            {
                unvme_free(ns_, *it);
            }
        }
        return 0;
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
    std::vector<vfio_dma_t *> chunk_pool_;
    std::atomic<int> alloc_lock_;
};