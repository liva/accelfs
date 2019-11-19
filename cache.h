#pragma once
#include <assert.h>
#include "misc.h"
#include "unvme.h"
#include "unvme_wrapper.h"

class ChunkIndex
{
public:
    ChunkIndex() : index_(0) {}
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
    Cache()
    {
        is_valid_ = false;
        needs_written_ = false;
    }
    Cache(Cache &&c)
    {
        is_valid_ = c.is_valid_;
        if (is_valid_)
        {
            dma_ = std::move(c.dma_);
            buf_offset_ = c.buf_offset_;
        }
        needs_written_ = c.needs_written_;
        ticket_ = c.ticket_;
        c.is_valid_ = false;
        c.needs_written_ = false;
    }
    Cache(uint64_t ticket, SharedDmaBuffer dma, size_t buf_offset)
    {
        ticket_ = ticket;
        dma_ = std::move(dma);
        buf_offset_ = buf_offset;
        is_valid_ = true;
        needs_written_ = false;
    }
    ~Cache()
    {
        assert(!IsValid());
        assert(!needs_written_);
    }
    void Reset(Cache &&c)
    {
        if (IsValid() && needs_written_)
        {
            abort();
        }
        is_valid_ = c.is_valid_;
        if (is_valid_)
        {
            dma_ = std::move(c.dma_);
            buf_offset_ = c.buf_offset_;
        }
        needs_written_ = c.needs_written_;
        ticket_ = c.ticket_;
        c.is_valid_ = false;
        c.needs_written_ = false;
    }
    bool IsValid()
    {
        return is_valid_;
    }
    bool IsWriteNeeded()
    {
        return IsValid() && needs_written_;
    }
    void MarkSynced(void *buf)
    {
        // TODO needs optimization
        assert(needs_written_);
        memcpy(buf, GetPtr(), kChunkSize);
        needs_written_ = false;
    }
    // cache <- buf
    void Refresh(const char *buf, size_t offset, size_t n)
    {
        assert(offset + n <= kChunkSize);
        memcpy(GetPtr() + offset, buf, n);
        needs_written_ = true;
    }
    // buf <- cache
    void Apply(char *buf, size_t offset, size_t n)
    {
        assert(offset + n <= kChunkSize);
        memcpy(buf, GetPtr() + offset, n);
    }
    void ForceRelease()
    {
        needs_written_ = false;
        is_valid_ = false;
    }
    void Release()
    {
        assert(!needs_written_);
        is_valid_ = false;
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
    char *GetPtr()
    {
        return (char *)dma_.GetBuffer() + buf_offset_;
    }
    SharedDmaBuffer dma_;
    size_t buf_offset_;
    bool needs_written_;
    bool is_valid_;
    uint64_t ticket_;
};