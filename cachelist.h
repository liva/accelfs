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
    ~CacheList()
    {
        assert(available_ == 0);
    }
    void Truncate(size_t len)
    {
        for (CacheContainer::Iterator it = CacheContainer::Iterator(container_);
             !it.IsEnd(); it = it.Next())
        {
            Cache &c = it->v;
            assert(c.IsValid());
            ChunkIndex cindex = it->k;
            if (cindex.GetPos() >= len)
            {
                c.ForceRelease();
                available_--;
            }
        }
    }
    void Release()
    {
        for (CacheContainer::Iterator it = CacheContainer::Iterator(container_);
             !it.IsEnd(); it = it.Next())
        {
            Cache &cache = it->v;
            assert(cache.IsValid());
            assert(!cache.IsWriteNeeded());
            cache.Release();
            available_--;
        }
        assert(CacheContainer::Iterator(container_).IsEnd());
        assert(available_ == 0);
    }

    bool CheckIfExistAndIncCnt(ChunkIndex cindex)
    {
        Cache *c = container_.Get(cindex);
        if (c != nullptr)
        {
            c->SetTicket(cache_ticket_);
            cache_ticket_++;
            return true;
        }
        return false;
    }
    void Apply(ChunkIndex cindex, char *buf, size_t offset, size_t n)
    {
        Cache *c = container_.Get(cindex);
        assert(c != nullptr);
        c->Apply(buf, offset, n);
    }
    void ForceRelease(ChunkIndex cindex)
    {
        Cache *c = container_.Get(cindex);
        assert(c != nullptr);
        c->ForceRelease();
        available_--;
    }
    // element should be released in advance
    int RegisterToCache(const int num, ChunkIndex cindex, SharedDmaBuffer &&dma, bool needs_written, std::pair<ChunkIndex, Cache> *release_cache_list)
    {
        size_t buf_offset = 0;
        int cnt = 0;
        for (int i = 0; i < num; i++)
        {
            CacheContainer::Container old;
            container_.Put(CacheContainer::Container{cindex, Cache(cache_ticket_, dma, buf_offset, needs_written)}, old);
            if (old.v.IsValid())
            {
                // flush current cache
                if (old.v.IsWriteNeeded())
                {
                  release_cache_list[cnt].first = old.k;
                  new (&release_cache_list[cnt].second)Cache(std::move(old.v));
                  cnt++;
                }
                else
                {
                    old.v.Release();
                }
            }
            else
            {
                available_++;
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
            CacheContainer::Container &pair = container_.GetFromIndex(CacheContainer::GetIndexFromKey(*it));
            Cache &c = pair.v;
            if (c.IsValid())
            {
                // flush current cache
                if (c.IsWriteNeeded())
                {
                    release_cache_list.push_back(std::move(std::make_pair(pair.k, std::move(pair.v))));
                }
                else
                {
                    c.Release();
                }
                available_--;
            }
        }
    }
    void ShrinkIfNeeded(int keep_num, Vector &release_cache_list)
    {
        if (keep_num < 32)
        {
            keep_num = 32;
        }
        if (available_ < keep_num * 2)
        {
            return;
        }
        if (cache_ticket_ < keep_num)
        {
            return;
        }
        uint64_t border_ticket = cache_ticket_ - keep_num;
        for (CacheContainer::Iterator it = CacheContainer::Iterator(container_);
             !it.IsEnd(); it = it.Next())
        {
            Cache &c = it->v;
            assert(c.IsValid());
            if (c.GetTicket() <= border_ticket)
            {
                ChunkIndex index = it->k;

                // flush current cache
                if (c.IsWriteNeeded())
                {
                    release_cache_list.push_back(std::move(std::make_pair(it->k, std::move(it->v))));
                }
                else
                {
                    c.Release();
                }
                available_--;
            }
        }
    }

    void CacheListSync(Vector &sync_cache_list)
    {
        for (CacheContainer::Iterator it = CacheContainer::Iterator(container_);
             !it.IsEnd(); it = it.Next())
        {
            assert(it->v.IsValid());
            sync_cache_list.push_back(std::move(std::make_pair(it->k, std::move(it->v))));
            available_--;
        }
        assert(available_ == 0);
        assert(CacheContainer::Iterator(container_).IsEnd());
    }

private:
    using CacheContainer = SimpleHashCache<ChunkIndex, Cache>;
    CacheContainer container_;
    uint64_t cache_ticket_ = 0;
    int available_ = 0;
};
