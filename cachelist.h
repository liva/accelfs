#pragma once
#include <vector>
#include <utility>
#include <assert.h>
#include "cache.h"
#include "cache_container.h"

class CacheList
{
public:
    ~CacheList()
    {
        assert(available_ == 0);
    }
    void Truncate(size_t len)
    {
        for (CacheContainer::Iterator it = CacheContainer::Iterator(container_);
             !it.IsEnd(); it = it.Next())
        {
            Cache &c = it->second;
            assert(c.IsValid());
            ChunkIndex cindex = it->first;
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
            Cache &cache = it->second;
            assert(cache.IsValid());
            assert(!cache.IsWriteNeeded());
            cache.Release();
            available_--;
        }
        assert(available_ == 0);
        assert(CacheContainer::Iterator(container_).IsEnd());
    }

    Cache *FindFromCacheList(ChunkIndex cindex)
    {
        Cache *c = container_.Get(cindex);
        if (c != nullptr)
        {
            c->SetTicket(cache_ticket_);
            cache_ticket_++;
            return c;
        }
        return nullptr;
    }
    // element should be released in advance
    void RegisterToCache(const int num, ChunkIndex cindex, SharedDmaBuffer dma)
    {
        size_t buf_offset = 0;
        for (int i = 0; i < num; i++)
        {
            container_.Put(cindex, std::move(Cache(cache_ticket_, dma, buf_offset)));
            cindex = ChunkIndex::CreateFromIndex(cindex.Get() + 1);
            buf_offset += kChunkSize;
            cache_ticket_++;
        }
        available_ += num;
    }
    void ReserveSlots(std::vector<ChunkIndex> incoming_indexes, std::vector<std::pair<ChunkIndex, Cache>> &release_cache_list)
    {
        for (auto it = incoming_indexes.begin(); it != incoming_indexes.end(); ++it)
        {
            std::pair<ChunkIndex, Cache> &pair = container_.GetFromIndex(CacheContainer::GetIndexFromKey(*it));
            Cache &c = pair.second;
            if (c.IsValid())
            {
                // flush current cache
                if (c.IsWriteNeeded())
                {
                    release_cache_list.push_back(std::move(pair));
                }
                else
                {
                    c.Release();
                }
                available_--;
            }
        }
    }
    void ShrinkIfNeeded(int keep_num, std::vector<std::pair<ChunkIndex, Cache>> &release_cache_list)
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
            Cache &c = it->second;
            assert(c.IsValid());
            if (c.GetTicket() <= border_ticket)
            {
                ChunkIndex index = it->first;

                // flush current cache
                if (c.IsWriteNeeded())
                {
                    release_cache_list.emplace_back(std::move(std::make_pair(it->first, std::move(it->second))));
                }
                else
                {
                    c.Release();
                }
                available_--;
            }
        }
    }

    void CacheListSync(std::vector<std::pair<ChunkIndex, Cache>> &sync_cache_list)
    {
        for (CacheContainer::Iterator it = CacheContainer::Iterator(container_);
             !it.IsEnd(); it = it.Next())
        {
            assert(it->second.IsValid());
            sync_cache_list.push_back(std::move(std::make_pair(it->first, std::move(it->second))));
        }
        assert(CacheContainer::Iterator(container_).IsEnd());
        available_ = 0;
    }

private:
    using CacheContainer = SimpleHashCache<ChunkIndex, Cache>;
    CacheContainer container_;
    uint64_t cache_ticket_ = 0;
    int available_ = 0;
};