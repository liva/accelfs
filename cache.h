#pragma once
#include <assert.h>
#include "spinlock.h"
#include "misc.h"

class ChunkIndex
{
public:
    ChunkIndex() = delete;
    ChunkIndex(const ChunkIndex &i) : index_(i.index_)
    {
    }
    static ChunkIndex CreateFromPos(u64 pos)
    {
        return ChunkIndex(pos / kChunkSize);
    }
    static ChunkIndex CreateFromIndex(u64 index)
    {
        return ChunkIndex(index);
    }
    u64 Get() const
    {
        return index_;
    }
    u64 GetPos() const
    {
        return index_ * kChunkSize;
    }
    bool operator==(const ChunkIndex &i) const
    {
        return index_ == i.index_;
    }
    bool operator!=(const ChunkIndex &i) const
    {
        return index_ != i.index_;
    }

private:
    ChunkIndex(u64 index) : index_(index)
    {
    }
    u64 index_;
};

class Cache
{
public:
    Cache() = delete;
    /*    Cache(const Cache &c) : lock_(0)
    {
        dma_ = c.dma_;
        needs_written_ = c.needs_written_;
        ticket_ = c.ticket_;
    }
    Cache &operator=(const Cache &c)
    {
        dma_ = c.dma_;
        needs_written_ = c.needs_written_;
        ticket_ = c.ticket_;
        return *this;
    }*/
    Cache(Cache &&c) : lock_(0)
    {
        Spinlock lock(c.lock_);
        dma_ = c.dma_;
        needs_written_ = c.needs_written_;
        ticket_ = c.ticket_;
        c.dma_ = nullptr;
        c.needs_written_ = false;
    }
    Cache(vfio_dma_t *dma) : lock_(0)
    {
        dma_ = dma;
        needs_written_ = false;
    }
    ~Cache()
    {
        assert(dma_ == nullptr);
        assert(!needs_written_);
    }
    void Reset(Cache &&c)
    {
        assert(!IsValid());
        Spinlock lock(c.lock_);
        dma_ = c.dma_;
        needs_written_ = c.needs_written_;
        ticket_ = c.ticket_;
        c.dma_ = nullptr;
        c.needs_written_ = false;
    }
    bool IsValid()
    {
        return dma_ != nullptr;
    }
    bool IsWriteNeeded()
    {
        return IsValid() && needs_written_;
    }
    vfio_dma_t *MarkSynced(vfio_dma_t *ndma)
    {
        Spinlock lock(lock_);
        assert(needs_written_);
        memcpy(ndma->buf, dma_->buf, kChunkSize);
        needs_written_ = false;
        vfio_dma_t *rdma = dma_;
        dma_ = ndma;
        return rdma;
    }
    // cache <- buf
    void Refresh(const char *buf, size_t offset, size_t n)
    {
        Spinlock lock(lock_);
        assert(offset + n <= kChunkSize);
        memcpy(reinterpret_cast<u8 *>(dma_->buf) + offset, buf, n);
        needs_written_ = true;
    }
    // buf <- cache
    void Apply(char *buf, size_t offset, size_t n)
    {
        Spinlock lock(lock_);
        assert(offset + n <= kChunkSize);
        memcpy(buf, reinterpret_cast<u8 *>(dma_->buf) + offset, n);
    }
    vfio_dma_t *Release()
    {
        Spinlock lock(lock_);
        assert(!needs_written_);
        vfio_dma_t *dma = dma_;
        dma_ = nullptr;
        return dma;
    }
    uint64_t GetTicket()
    {
        return ticket_;
    }
    void SetTicket(uint64_t ticket)
    {
        ticket_ = ticket;
    }

private:
    std::atomic<int> lock_;
    vfio_dma_t *dma_;
    bool needs_written_;
    uint64_t ticket_;
};