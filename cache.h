#pragma once
#include <assert.h>
//#include <unordered_map>
#include "spinlock.h"
#include "misc.h"
#include "unvme.h"

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

/*namespace std
{
template <>
class hash<ChunkIndex>
{
public:
    size_t operator()(const ChunkIndex &p) const { return p.Get(); }
};
} // namespace std
*/
class Cache
{
public:
    Cache() : lock_(0)
    {
        dma_ = nullptr;
        iod_ = nullptr;
        needs_written_ = false;
    }
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
        iod_ = c.iod_;
        needs_written_ = c.needs_written_;
        ticket_ = c.ticket_;
        c.dma_ = nullptr;
        c.iod_ = nullptr;
        c.needs_written_ = false;
    }
    Cache(uint64_t ticket, vfio_dma_t *dma, unvme_iod_t iod) : lock_(0)
    {
        ticket_ = ticket;
        dma_ = dma;
        iod_ = iod;
        needs_written_ = false;
    }
    ~Cache()
    {
        assert(iod_ == nullptr);
        assert(dma_ == nullptr);
        assert(!needs_written_);
    }
    void Reset(Cache &&c)
    {
        assert(!IsValid());
        Spinlock lock(c.lock_);
        dma_ = c.dma_;
        iod_ = c.iod_;
        needs_written_ = c.needs_written_;
        ticket_ = c.ticket_;
        c.dma_ = nullptr;
        c.iod_ = nullptr;
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
        assert(iod_ == nullptr);
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
        assert(iod_ == nullptr);
        assert(offset + n <= kChunkSize);
        memcpy(reinterpret_cast<u8 *>(dma_->buf) + offset, buf, n);
        needs_written_ = true;
    }
    // buf <- cache
    void Apply(char *buf, size_t offset, size_t n)
    {
        Spinlock lock(lock_);
        assert(iod_ == nullptr);
        assert(offset + n <= kChunkSize);
        memcpy(buf, reinterpret_cast<u8 *>(dma_->buf) + offset, n);
    }
    vfio_dma_t *ForceRelease()
    {
        Spinlock lock(lock_);
        assert(iod_ == nullptr);
        vfio_dma_t *dma = dma_;
        needs_written_ = false;
        dma_ = nullptr;
        return dma;
    }
    vfio_dma_t *Release()
    {
        Spinlock lock(lock_);
        assert(!needs_written_);
        assert(iod_ == nullptr);
        vfio_dma_t *dma = dma_;
        iod_ = nullptr;
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
    unvme_iod_t ReleaseIod()
    {
        unvme_iod_t iod = iod_;
        iod_ = nullptr;
        return iod;
    }

private:
    std::atomic<int> lock_;
    vfio_dma_t *dma_;
    unvme_iod_t iod_;
    bool needs_written_;
    uint64_t ticket_;
};