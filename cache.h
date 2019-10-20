#pragma once
#include <utility>
#include "misc.h"

class Cache
{
public:
    const u64 index_;
    vfio_dma_t *const dma_;
    bool needs_written_;
    bool needs_sync_with_storage_;
    const size_t blocksize_;
    Cache() = delete;
    Cache(u64 index, vfio_dma_t *dma, size_t blocksize, bool needs_sync_with_storage) : index_(index), dma_(dma), blocksize_(blocksize), needs_sync_with_storage_(needs_sync_with_storage)
    {
        needs_written_ = false;
    }
    bool IsWriteNeeded()
    {
        if (needs_sync_with_storage_)
        {
            return needs_written_;
        }
        else
        {
            return false;
        }
    }
    u64 GetIndex()
    {
        return index_;
    }
    std::pair<void *, u64> MarkSynced()
    {
        assert(needs_sync_with_storage_);
        assert(needs_written_);
        needs_written_ = false;
        return std::make_pair(dma_->buf, index_);
    }
    // cache <- buf
    void Refresh(char *buf)
    {
        memcpy(dma_->buf, buf, blocksize_);
        if (needs_sync_with_storage_)
        {
            needs_written_ = true;
        }
    }
    // buf <- cache
    bool Apply(void *buf, u64 index, u32 nb)
    {
        if (index <= index_ && index_ < index + nb)
        {
            memcpy(reinterpret_cast<char *>(buf) + (index_ - index) * blocksize_, dma_->buf, blocksize_);
            return true;
        }
        return false;
    }
    vfio_dma_t *Release()
    {
        if (needs_sync_with_storage_)
        {
            assert(!needs_written_);
        }
        return dma_;
    }
};