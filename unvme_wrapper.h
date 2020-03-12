/**
 * Copyright 2020 NEC Laboratories Europe GmbH
 * All rights reserved
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *    1. Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *    2. Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 * 
 *    3. Neither the name of NEC Laboratories Europe GmbH nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY NEC Laboratories Europe GmbH AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL NEC Laboratories 
 * Europe GmbH OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO,  PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#pragma once

#include "misc.h"
#include <atomic>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <assert.h>
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
        for (int i = 0; i < kMemLeakCountArrayIndexMax; i++)
        {
            memleak_count_[i] = 0;
        }
    }
    ~UnvmeWrapper()
    {
        int64_t memleak_count = 0;
        for (int i = 0; i < kMemLeakCountArrayIndexMax; i++)
        {
            memleak_count += memleak_count_[i];
        }
        if (memleak_count != 0)
        {
            printf("!!!memory leak detected! (%zu bytes)\n", memleak_count);
            fflush(stdout);
        }
        unvme_close(ns_);
    }
    void HardWrite()
    {
        Spinlock lock(lock_);
        vfio_dma_t dma;
        unvme_alloc2(ns_, &dma, ns_->blocksize); // dummy
        u32 cdw10_15[6];                         // dummy
        int stat = unvme_cmd(ns_, 0, NVME_CMD_FLUSH, ns_->id, dma.buf, 512, cdw10_15, 0);
        if (stat)
        {
            printf("failed to sync");
        }
        unvme_free2(ns_, &dma);
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
    int AreadChunk(u64 lba, vfio_dma_t *dma, unvme_iod_t &iod)
    {
        Alloc(dma, kChunkSize);
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
    void Alloc(vfio_dma_t *dma, size_t size)
    {
        if (size > 2 * 1024 * 1024)
        {
            printf("unvme dma memory allocation failure.\n");
            abort();
        }
        unvme_alloc2(ns_, dma, size);
        if (kDebug)
        {
            memleak_count_[GetMemLeakCountIndex()] += dma->size;
        }
    }
    int Free(vfio_dma_t *dma)
    {
        if (kDebug)
        {
            memleak_count_[GetMemLeakCountIndex()] -= dma->size;
        }
        return unvme_free2(ns_, dma);
    }
    std::atomic<int> &GetLockFlag()
    {
        return lock_;
    }

private:
    int GetMemLeakCountIndex()
    {
        if (memleak_count_index_ == -1)
        {
            int index = memleak_count_current_index_.fetch_add(1);
            if (index >= kMemLeakCountArrayIndexMax)
            {
                abort();
            }
            memleak_count_index_ = index;
        }
        return memleak_count_index_;
    }
    const unvme_ns_t *ns_;
    std::atomic<int> lock_;
    static const int kMemLeakCountArrayIndexMax = 16;
    int64_t memleak_count_[kMemLeakCountArrayIndexMax];
    thread_local static int memleak_count_index_;
    static std::atomic<int> memleak_count_current_index_;
};

class DmaBufferWrapper
{
public:
    DmaBufferWrapper(UnvmeWrapper &ns_wrapper, size_t size) : ns_wrapper_(ns_wrapper), size_(size)
    {
        ns_wrapper_.Alloc(&buf_, size);
        cnt_ = 1;
    }
    vfio_dma_t *GetBuffer()
    {
        return &buf_;
    }
    size_t GetSize()
    {
        return size_;
    }
    ~DmaBufferWrapper()
    {
        assert(cnt_ == 0);
        ns_wrapper_.Free(&buf_);
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
    vfio_dma_t buf_;
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