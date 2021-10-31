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
#include <vector>
#include <utility>
#include <assert.h>
#include "cache.h"
#include "cache_container.h"

class CacheList
{
public:
    using Vector = std::vector<std::pair<ChunkIndex, Cache>>;
    void Truncate(size_t len)
    {
        for (CacheContainer::Iterator it = container_wrapper_.GetFirstIterator();
             !it.IsEnd(); it = it.Next())
        {
            ChunkIndex cindex = it.GetKey();
            if (cindex.GetPos() >= len)
            {
                Cache c = container_wrapper_.Get(cindex);
                assert(c.IsValid());
                c.ForceRelease();
            }
        }
    }
    void Release()
    {
        for (CacheContainer::Iterator it = container_wrapper_.GetFirstIterator();
             !it.IsEnd(); it = it.Next())
        {
            ChunkIndex cindex = it.GetKey();
            Cache c = container_wrapper_.Get(cindex);
            assert(c.IsValid());
            assert(!c.IsWriteNeeded());
            c.Release();
        }
        assert(container_wrapper_.IsEmpty());
    }

    bool CheckIfExistAndIncCnt(ChunkIndex cindex)
    {
        Cache c = container_wrapper_.Get(cindex);
        if (c.IsValid())
        {
            c.SetTicket(cache_ticket_);
            container_wrapper_.PutToEmptyEntry(cindex, std::move(c));
            cache_ticket_++;
            return true;
        }
        return false;
    }
    void Refresh(ChunkIndex cindex, const char *buf, size_t offset, size_t n)
    {
        Cache c = container_wrapper_.Get(cindex);
        assert(c.IsValid());
        c.Refresh(buf, offset, n);
        container_wrapper_.PutToEmptyEntry(cindex, std::move(c));
    }
    void Apply(ChunkIndex cindex, char *buf, size_t offset, size_t n)
    {
        Cache c = container_wrapper_.Get(cindex);
        assert(c.IsValid());
        c.Apply(buf, offset, n);
        container_wrapper_.PutToEmptyEntry(cindex, std::move(c));
    }
    void ForceRelease(ChunkIndex cindex)
    {
        Cache c = container_wrapper_.Get(cindex);
        assert(c.IsValid());
        c.ForceRelease();
    }
    // element should be released in advance
    int RegisterToCache(const int num, ChunkIndex cindex, SharedDmaBuffer &&dma, bool needs_written, std::pair<ChunkIndex, Cache> *release_cache_list)
    {
        size_t buf_offset = 0;
        int cnt = 0;
        for (int i = 0; i < num; i++)
        {
            CacheContainer::Container old;
            container_wrapper_.Put(cindex, Cache(cache_ticket_, dma, buf_offset, needs_written), old);
            if (old.v.IsValid())
            {
                // flush current cache
                if (old.v.IsWriteNeeded())
                {
                    release_cache_list[cnt].first = old.k;
                    new (&release_cache_list[cnt].second) Cache(std::move(old.v));
                    cnt++;
                }
                else
                {
                    old.v.Release();
                }
            }
            cindex = ChunkIndex::CreateFromIndex(cindex.Get() + 1);
            buf_offset += kChunkSize;
            cache_ticket_++;
        }
        return cnt;
    }
    void ReserveSlots(std::vector<ChunkIndex> incoming_indexes, Vector &release_cache_list)
    {
        for (auto it = incoming_indexes.begin(); it != incoming_indexes.end(); ++it)
        {
            ChunkIndex cindex = *it;
            Cache c = container_wrapper_.Get(cindex);
            if (c.IsValid())
            {
                // flush current cache
                if (c.IsWriteNeeded())
                {
                    release_cache_list.push_back(std::move(std::make_pair(cindex, std::move(c))));
                }
                else
                {
                    c.Release();
                }
            }
        }
    }
    void ShrinkIfNeeded(int keep_num, Vector &release_cache_list)
    {
        if (keep_num < 32)
        {
            keep_num = 32;
        }
        if (container_wrapper_.GetAvailableNum() < keep_num * 2)
        {
            return;
        }
        if (cache_ticket_ < keep_num)
        {
            return;
        }
        uint64_t border_ticket = cache_ticket_ - keep_num;
        for (CacheContainer::Iterator it = container_wrapper_.GetFirstIterator();
             !it.IsEnd(); it = it.Next())
        {
            ChunkIndex cindex = it.GetKey();
            Cache c = container_wrapper_.Get(cindex);
            assert(c.IsValid());
            if (c.GetTicket() <= border_ticket)
            {
                // flush current cache
                if (c.IsWriteNeeded())
                {
                    release_cache_list.push_back(std::move(std::make_pair(cindex, std::move(c))));
                }
                else
                {
                    c.Release();
                }
            }
        }
    }

    void CacheListSync(Vector &sync_cache_list)
    {
        for (CacheContainer::Iterator it = container_wrapper_.GetFirstIterator();
             !it.IsEnd(); it = it.Next())
        {
            ChunkIndex cindex = it.GetKey();
            Cache c = container_wrapper_.Get(cindex);
            assert(c.IsValid());
            sync_cache_list.push_back(std::move(std::make_pair(cindex, std::move(c))));
        }
        assert(container_wrapper_.IsEmpty());
    }

private:
    using CacheContainer = SimpleHashCache<ChunkIndex, Cache>;
    class CacheContainerWrapper
    {
    public:
        CacheContainerWrapper() {}
        CacheContainerWrapper(const CacheContainerWrapper &obj) = delete;
        CacheContainerWrapper &operator=(const CacheContainerWrapper &obj) = delete;
        ~CacheContainerWrapper()
        {
            assert(available_ == 0);
        }
        int GetAvailableNum() const
        {
            return available_;
        }
        Cache Get(ChunkIndex cindex)
        {
            Cache c = container_.Get(cindex);
            if (c.IsValid())
            {
                available_--;
            }
            return c;
        }
        void Put(ChunkIndex cindex, Cache &&c, CacheContainer::Container &old)
        {
            container_.Put(CacheContainer::Container{cindex, std::move(c)}, old);
            if (!old.v.IsValid())
            {
                available_++;
            }
        }
        void PutToEmptyEntry(ChunkIndex cindex, Cache &&c)
        {
            available_++;
            CacheContainer::Container old;
            container_.Put(CacheContainer::Container{cindex, std::move(c)}, old);
            assert(!old.v.IsValid());
        }
        CacheContainer::Iterator GetFirstIterator()
        {
            return CacheContainer::Iterator(container_);
        }
        bool IsEmpty()
        {
            return GetFirstIterator().IsEnd() && (available_ == 0);
        }

    private:
        CacheList::CacheContainer container_;
        int available_ = 0;
    } container_wrapper_;
    uint64_t cache_ticket_ = 0;
};
