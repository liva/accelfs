#pragma once
#include <utility>
#include <assert.h>
#include "misc.h"

class ChunkIndex
{
public:
    static ChunkIndex CreateFromPos(u64 pos)
    {
        return ChunkIndex(pos / kChunkSize);
    }
    static ChunkIndex CreateFromIndex(u64 index)
    {
        return ChunkIndex(index);
    }
    u64 Get()
    {
        return index_;
    }
    u64 GetPos()
    {
        return index_ * kChunkSize;
    }
    bool operator==(ChunkIndex i)
    {
        return index_ == i.index_;
    }
    bool operator!=(ChunkIndex i)
    {
        return index_ != i.index_;
    }

private:
    ChunkIndex() = delete;
    ChunkIndex(u64 index) : index_(index)
    {
    }
    u64 index_;
};

class Cache
{
public:
    const ChunkIndex index_;
    vfio_dma_t *dma_;
    bool needs_written_;
    Cache() = delete;
    Cache(ChunkIndex index, vfio_dma_t *dma) : index_(index), dma_(dma)
    {
        needs_written_ = false;
    }
    bool IsWriteNeeded()
    {
        return needs_written_;
    }
    ChunkIndex GetIndex()
    {
        return index_;
    }
    std::pair<vfio_dma_t *, ChunkIndex> MarkSynced(vfio_dma_t *ndma)
    {
        assert(needs_written_);
        memcpy(ndma->buf, dma_->buf, kChunkSize);
        needs_written_ = false;
        std::pair<vfio_dma_t *, ChunkIndex> pair = std::make_pair(dma_, index_);
        dma_ = ndma;
        return pair;
    }
    // cache <- buf
    void Refresh(const char *buf, size_t offset, size_t n)
    {
        assert(offset + n <= kChunkSize);
        memcpy(reinterpret_cast<u8 *>(dma_->buf) + offset, buf, n);
        needs_written_ = true;
    }
    // buf <- cache
    void Apply(char *buf, size_t offset, size_t n)
    {
        assert(offset + n <= kChunkSize);
        memcpy(buf, reinterpret_cast<u8 *>(dma_->buf) + offset, n);
    }
    vfio_dma_t *Release()
    {
        assert(!needs_written_);
        return dma_;
    }
};