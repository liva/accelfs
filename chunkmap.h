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
#include <atomic>
#include <assert.h>
#include <string.h>
#include <stdio.h>
#include "_misc.h"
#include "spinlock.h"

class Chunkmap
{
public:
    static const uint64_t kSize = kChunkSize * 4096;
    class Index
    {
    public:
        Index(const Index &i) : index_(i.index_) {}
        Index() : index_(0) {}
        static Index CreateFromPos(uint64_t pos)
        {
            assert(pos / kChunkSize != 0);
            return Index(pos / kChunkSize);
        }
        static Index CreateFromIndex(uint64_t index)
        {
            assert(index != 0);
            return Index(index);
        }
        uint64_t Get()
        {
            assert(index_ != 0);
            return index_;
        }
        uint64_t GetPos()
        {
            assert(index_ != 0);
            return index_ * kChunkSize;
        }
        bool IsNull()
        {
            return index_ == 0;
        }
        bool operator==(Index i)
        {
            return index_ == i.index_;
        }
        bool operator!=(Index i)
        {
            return index_ != i.index_;
        }

    private:
        Index(uint64_t index) : index_(index)
        {
            assert(index_ != 0);
        }
        uint64_t index_;
    };
    Chunkmap() : lock_(0)
    {
        buf_ = (uint8_t *)malloc(kSize);
        for (int i = 0; i < kSize / kChunkSize; i++)
        {
            needs_written_[i] = false;
        }
    }
    ~Chunkmap()
    {
        free(buf_);
        for (int i = 0; i < kSize / kChunkSize; i++)
        {
            assert(!needs_written_[i]);
        }
    }
    void Create(Index max_supported_by_storage, Index end_of_reserved)
    {
        Spinlock lock(lock_);
        uint64_t _end_of_reserved = end_of_reserved.Get();
        uint64_t _max_supported_by_storage = max_supported_by_storage.Get();
        assert(1 <= _end_of_reserved);
        assert(_end_of_reserved < _max_supported_by_storage);
        for (uint64_t i = 0; i < _end_of_reserved; i++)
        {
            Set(i, kUsed);
        }
        last_allocated_index_ = _end_of_reserved;
        for (uint64_t i = _end_of_reserved; i < _max_supported_by_storage || i < kMaxIndex; i++)
        {
            Set(i, kUnused);
        }
        for (uint64_t i = _max_supported_by_storage; i < kMaxIndex; i++)
        {
            Set(i, kUsed);
        }
        for (int i = 0; i < kSize / kChunkSize; i++)
        {
            needs_written_[i] = true;
        }
    }
    bool NeedsWrite(size_t offset)
    {
        return needs_written_[offset / kChunkSize];
    }
    void Write(size_t offset, void *buf)
    {
        Spinlock lock(lock_);
        assert((offset % kChunkSize) == 0);
        assert(needs_written_[offset / kChunkSize]);
        memcpy(buf, buf_ + offset, kChunkSize);
        needs_written_[offset / kChunkSize] = false;
    }
    void Read(size_t offset, void *buf)
    {
        Spinlock lock(lock_);

        assert((offset % kChunkSize) == 0);
        memcpy(buf_ + offset, buf, kChunkSize);
        needs_written_[offset / kChunkSize] = false;
    }
    void Dump(int start, int end)
    {
        for (uint64_t i = start; i < end; i++)
        {
            printf("%c", Get(i) == kUsed ? 'X' : '-');
        }
        printf("\n");
    }
    Index FindUnused()
    {
        Spinlock lock(lock_);
        for (uint64_t i = last_allocated_index_; i < kMaxIndex; i++)
        {
            if (Get(i) == kUnused)
            {
                Set(i, kUsed);
                SetDirtyFlag(i, true);
                last_allocated_index_ = i;
                return Chunkmap::Index::CreateFromIndex(i);
            }
        }
        for (uint64_t i = 1; i < last_allocated_index_; i++)
        {
            if (Get(i) == kUnused)
            {
                Set(i, kUsed);
                SetDirtyFlag(i, true);
                last_allocated_index_ = i;
                return Chunkmap::Index::CreateFromIndex(i);
            }
        }
        return Chunkmap::Index();
    }
    void Release(Index i)
    {
        Spinlock lock(lock_);
        if (i.IsNull())
        {
            return;
        }
        assert(Get(i.Get()) == kUsed);
        Set(i.Get(), kUnused);
        SetDirtyFlag(i.Get(), true);
    }

private:
    void Set(uint64_t index, bool flag)
    {
        if (index > kMaxIndex)
        {
            return;
        }
        if (flag)
        {
            buf_[index / 8] |= 1 << (index % 8);
        }
        else
        {
            buf_[index / 8] &= ~(1 << (index % 8));
        }
    }
    bool Get(uint64_t index)
    {
        if (index > kMaxIndex)
        {
            return kUsed;
        }
        return ((buf_[index / 8] >> (index % 8)) & 1) == 1;
    }
    void SetDirtyFlag(uint64_t index, bool flag)
    {
        needs_written_[index / (kChunkSize * 8)] = flag;
    }
    uint8_t *buf_;
    bool needs_written_[kSize / kChunkSize];
    static const uint64_t kMaxIndex = kSize * 8;
    static const bool kUsed = true;
    static const bool kUnused = false;
    uint64_t last_allocated_index_ = 1;
    std::atomic<int> lock_;
};

#if 0
int test()
{
    {
        Chunkmap cmap;
        char buf[kChunkSize];
        assert(!cmap.Write(0, buf));
    }
    {
        Chunkmap cmap;
        cmap.FindUnused();
        char buf[kChunkSize];
        assert(cmap.Write(0, buf));
    }
    {
        Chunkmap cmap;
        cmap.FindUnused();
        char buf[kChunkSize];
        assert(cmap.Write(0, buf));
        assert(!cmap.Write(0, buf));
    }
    {
        Chunkmap cmap;
        cmap.FindUnused();
        char buf[kChunkSize];
        cmap.Read(0, buf);
        assert(!cmap.Write(0, buf));
    }
    {
        Chunkmap cmap;
        auto i = cmap.FindUnused();
        char buf[kChunkSize];
        assert(cmap.Write(0, buf));
        assert(!cmap.Write(0, buf));
        cmap.Release(i);
        assert(cmap.Write(0, buf));
    }
    {
        Chunkmap cmap;
        cmap.Create(Chunkmap::Index::CreateFromIndex(10), Chunkmap::Index::CreateFromIndex(3));
        char buf[kChunkSize];
        assert(cmap.Write(0, buf));
    }
}
#endif