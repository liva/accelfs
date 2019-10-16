#pragma once
#include "misc.h"

class Chunkmap
{
public:
    static const u64 kSize = kChunkSize;
    class Index
    {
    public:
        static Index CreateFromPos(u64 pos)
        {
            return Index(pos / kSize);
        }
        static Index CreateFromIndex(u64 index)
        {
            return Index(index);
        }
        static Index CreateNull()
        {
            return Index();
        }
        u64 Get()
        {
            assert(index_ != 0);
            return index_;
        }
        u64 GetPos()
        {
            assert(index_ != 0);
            return index_ * kSize;
        }
        bool IsNull()
        {
            return index_ == 0;
        }

    private:
        Index() : index_(0) {}
        Index(u64 index) : index_(index)
        {
            assert(index_ != 0);
        }
        u64 index_;
    };
    void Create(Index max_supported_by_storage, Index end_of_reserved)
    {
        assert(1 <= end_of_reserved.Get());
        assert(end_of_reserved.Get() < max_supported_by_storage.Get());
        assert(max_supported_by_storage.Get() < kMaxIndex);
        for (u64 i = 0; i < end_of_reserved.Get(); i++)
        {
            Set(i, kUsed);
        }
        for (u64 i = end_of_reserved.Get(); i < max_supported_by_storage.Get(); i++)
        {
            Set(i, kUnused);
        }
        for (u64 i = max_supported_by_storage.Get(); i < kMaxIndex; i++)
        {
            Set(i, kUsed);
        }
    }
    void Write(void *buf)
    {
        memcpy(buf, buf_, kSize);
    }
    void Read(void *buf)
    {
        memcpy(buf_, buf, kSize);
    }
    void Dump(int max)
    {
        printf("X");
        if (max == 0 || max < 0 || max > kMaxIndex)
        {
            max = kMaxIndex;
        }
        for (u64 i = 1; i < max; i++)
        {
            printf("%c", Get(i) == kUsed ? 'X' : '-');
        }
        printf("\n");
    }
    Index FindUnused()
    {
        for (u64 i = 1; i < kMaxIndex; i++)
        {
            if (Get(i) == kUnused)
            {
                Set(i, kUsed);
                return Chunkmap::Index::CreateFromIndex(i);
            }
        }
        return Chunkmap::Index::CreateNull();
    }
    void Release(Index i)
    {
        if (i.IsNull())
        {
            return;
        }
        assert(Get(i.Get()) == kUsed);
        Set(i.Get(), kUnused);
    }

private:
    void Set(u64 index, bool flag)
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
    bool Get(u64 index)
    {
        if (index > kMaxIndex)
        {
            return kUsed;
        }
        return ((buf_[index / 8] >> (index % 8)) & 1) == 1;
    }
    char buf_[kSize];
    static const u64 kMaxIndex = kSize * 8;
    static const bool kUsed = true;
    static const bool kUnused = false;
};