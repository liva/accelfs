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
    void RegisterToCache(const int num, ChunkIndex cindex, SharedDmaBuffer &&dma, bool needs_written, Vector &release_cache_list)
    {
        size_t buf_offset = 0;
        for (int i = 0; i < num; i++)
        {
            CacheContainer::Container old;
            container_.Put(CacheContainer::Container{cindex, Cache(cache_ticket_, dma, buf_offset, needs_written)}, old);
            if (old.v.IsValid())
            {
                // flush current cache
                if (old.v.IsWriteNeeded())
                {
                    release_cache_list.push_back(std::move(std::make_pair(old.k, std::move(old.v))));
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