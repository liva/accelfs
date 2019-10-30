#pragma once
#include <atomic>
#include <deque>
#include <vector>
#include "spinlock.h"
#include "misc.h"
#include "chunkmap.h"

class Header
{
public:
    Header(UnvmeWrapper &ns_wrapper);
    Header() = delete;
    void Release()
    {
        assert(!updated_);
        Spinlock lock(lock_);
        int i = 0;
        for (auto itr = inodes_.begin(); itr != inodes_.end(); ++itr)
        {
            Inode *inode = *itr;
            bool inode_updated = inode->Sync();
            assert(!inode_updated);
            inode->Release();
            delete inode;
        }
        inodes_.clear();
        //HardWrite();
    }
    ~Header()
    {
        assert(inodes_.empty());
    }
    Inode *GetInode(const std::string &fname)
    {
        Spinlock lock(lock_);
        for (auto itr = inodes_.begin(); itr != inodes_.end(); ++itr)
        {
            Inode *inode = *itr;
            if (inode->GetFname() == fname)
            {
                return inode;
            }
        }
        return nullptr;
    }
    int GetBlockSize()
    {
        return kChunkSize;
    }
    void WriteSync()
    {
        std::deque<Inode::AsyncIoContext> ctxs = Write();
        for (auto it = ctxs.begin(); it != ctxs.end(); ++it)
        {
            if (ns_wrapper_.Apoll((*it).iod))
            {
                printf("failed to unvme_write");
                abort();
            }
            ns_wrapper_.Free((*it).dma);
        }
    }
    bool DoesExist(const std::string &fname)
    {
        return GetInode(fname) != nullptr;
    }
    void GetChildren(const std::string &dir,
                     std::vector<std::string> *result)
    {
        result->clear();
        Spinlock lock(lock_);
        for (auto itr = inodes_.begin(); itr != inodes_.end(); ++itr)
        {
            Inode *inode = *itr;
            const std::string &filename = inode->GetFname();

            if (filename.size() >= dir.size() + 1 && filename[dir.size()] == '/' && (memcmp(filename.data(), dir.data(), dir.size()) == 0))
            {
                result->push_back(filename.substr(dir.size() + 1));
            }
        }
    }
    Inode *Create(const std::string &fname, bool lock)
    {
        Inode *rd = GetInode(fname);

        if (rd == nullptr)
        {
            rd = Inode::CreateEmpty(fname, (lock ? 1 : 0), chunkmap_, ns_wrapper_);
            if (rd != nullptr)
            {
                Spinlock lock(lock_);
                inodes_.push_back(rd);
            }
        }
        return rd;
    }
    void Delete(Inode *inode)
    {
        {
            Spinlock lock(lock_);
            updated_ = true;
            for (auto it = inodes_.begin(); it != inodes_.end(); ++it)
            {
                if (*it == inode)
                {
                    inodes_.erase(it);
                    break;
                }
            }
        }
        inode->Sync();
        inode->Release();
        delete inode;
    }
    void Dump()
    {
        printf(">>>>\n");
        for (auto itr = inodes_.begin(); itr != inodes_.end(); ++itr)
        {
            Inode *inode = *itr;
            printf("> %p %s [", inode, inode->GetFname().c_str());
            auto clist = inode->GetChunkList();
            for (auto it = clist.begin(); it != clist.end(); ++it)
            {
                printf("%lu ", *it);
            }
            printf("] %zu\n", inode->GetLen());
        }
    }

private:
    std::deque<Inode::AsyncIoContext> Write();
    bool Read();
    void WriteSub(HeaderBuffer &buf);
    u32 GetBlockNumFromSize(size_t size)
    {
        return (size + ns_wrapper_.GetBlockSize() - 1) / ns_wrapper_.GetBlockSize();
    }
    void ChunkmapRead()
    {
        vfio_dma_t *dma = ns_wrapper_.AllocChunk();
        if (!dma)
        {
            printf("allocation failure\n");
            exit(1);
        }
        for (u64 offset = 0; offset < Chunkmap::kSize; offset += kChunkSize)
        {
            ns_wrapper_.Read(dma->buf, GetBlockNumFromSize(kChunkmapStartPos + offset),
                             GetBlockNumFromSize(kChunkSize));
            chunkmap_.Read(offset, dma->buf);
        }
        ns_wrapper_.Free(dma);
    }
    std::deque<Inode::AsyncIoContext> ChunkmapWrite()
    {
        std::deque<Inode::AsyncIoContext> ctxs;
        for (u64 offset = 0; offset < Chunkmap::kSize; offset += kChunkSize)
        {
            if (!chunkmap_.NeedsWrite(offset))
            {
                continue;
            }
            vfio_dma_t *dma = ns_wrapper_.AllocChunk();
            if (!dma)
            {
                printf("allocation failure\n");
                exit(1);
            }
            chunkmap_.Write(offset, dma->buf);
            unvme_iod_t iod = ns_wrapper_.Awrite(dma->buf, GetBlockNumFromSize(kChunkmapStartPos + offset),
                                                 GetBlockNumFromSize(kChunkSize));
            if (!iod)
            {
                printf("failed to unvme_write");
                abort();
            }
            ctxs.push_back(Inode::AsyncIoContext{
                .iod = iod,
                .dma = dma,
                .time = ve_gettime(),
            });
        }
        return ctxs;
    }

    static const u64 kHeaderStartPos = 0;

    Chunkmap chunkmap_;
    static const u64 kChunkmapStartPos = kHeaderStartPos + kChunkSize;
    static const u64 kDataStorageStartPos = kChunkmapStartPos + Chunkmap::kSize;
    UnvmeWrapper &ns_wrapper_;
    std::atomic<int> lock_;
    std::vector<Inode *> inodes_;
    static const char *kVersionString;
    bool updated_;
    std::vector<Chunkmap::Index> header_exchunks_;
};

inline Header::Header(UnvmeWrapper &ns_wrapper) : ns_wrapper_(ns_wrapper), lock_(0), updated_(false)
{
    if (!Read())
    {
        chunkmap_.Create(Chunkmap::Index::CreateFromPos(ns_wrapper_.GetBlockCount() * ns_wrapper_.GetBlockSize()), Chunkmap::Index::CreateFromPos(kDataStorageStartPos));
        WriteSync();
    }
    if (kHeaderDump)
    {
        Dump();
        chunkmap_.Dump(1, 500);
    }
}

inline std::deque<Inode::AsyncIoContext> Header::Write()
{
    std::deque<Inode::AsyncIoContext> ctxs;

    size_t info_size;
    do
    {
        HeaderBuffer tmp_buf;
        char tmp_cbuf[kChunkSize];
        tmp_buf.AppendRaw(kVersionString, strlen(kVersionString));
        u64 next_header = 0;
        tmp_buf.Append(next_header);
        tmp_buf.ResetPos();
        info_size = tmp_buf.Output(tmp_cbuf, kChunkSize);
    } while (0);

    Spinlock lock(lock_);
    HeaderBuffer content_buf;
    WriteSub(content_buf);
    content_buf.ResetPos();

    u64 hchunk_pos = kHeaderStartPos;
    size_t extra_chunk = 0;
    while (true)
    {
        vfio_dma_t *dma = ns_wrapper_.AllocChunk();
        if (!dma)
        {
            printf("allocation failure\n");
            exit(1);
        }
        size_t output_size = content_buf.Output(reinterpret_cast<char *>(dma->buf) + info_size, kChunkSize - info_size);
        bool output_completed = (output_size != kChunkSize - info_size);

        HeaderBuffer buf;
        buf.AppendRaw(kVersionString, strlen(kVersionString));
        u64 next_header = 0;
        if (!output_completed)
        {
            assert(header_exchunks_.size() >= extra_chunk);
            if (extra_chunk == header_exchunks_.size())
            {
                auto c = chunkmap_.FindUnused();
                if (c.IsNull())
                {
                    printf("failed to allocate an extra chunk for header\n");
                    exit(1);
                }
                header_exchunks_.push_back(c);
            }
            next_header = header_exchunks_[extra_chunk].GetPos();
            extra_chunk++;
        }
        buf.Append(next_header);
        buf.ResetPos();
        buf.Output(dma->buf, kChunkSize);

        unvme_iod_t iod = ns_wrapper_.Awrite(dma->buf, GetBlockNumFromSize(hchunk_pos),
                                             GetBlockNumFromSize(kChunkSize));
        if (!iod)
        {
            printf("failed to unvme_write");
            abort();
        }
        ctxs.push_back(Inode::AsyncIoContext{
            .iod = iod,
            .dma = dma,
            .time = ve_gettime(),
        });
        if (output_completed)
        {
            break;
        }
        hchunk_pos = next_header;
    }
    assert(extra_chunk <= header_exchunks_.size());
    for (size_t i = extra_chunk; i < header_exchunks_.size(); i++)
    {
        chunkmap_.Release(header_exchunks_[i]);
    }
    header_exchunks_.resize(extra_chunk);

    auto chunkmap_ctxs = ChunkmapWrite();
    for (auto it = chunkmap_ctxs.begin(); it != chunkmap_ctxs.end(); ++it)
    {
        ctxs.push_back(*it);
    }
    updated_ = false;
    return ctxs;
}

inline bool Header::Read()
{
    bool error = false;

    vfio_dma_t *dma = ns_wrapper_.AllocChunk();
    if (!dma)
    {
        printf("allocation failure\n");
        exit(1);
    }
    header_exchunks_.clear();
    u64 hchunk_pos = kHeaderStartPos;
    HeaderBuffer buf;
    while (true)
    {
        if (ns_wrapper_.Read(dma->buf, GetBlockNumFromSize(hchunk_pos), GetBlockNumFromSize(kChunkSize)))
        {
            printf("failed to unvme_read");
            exit(1);
        }
        HeaderBuffer tmp_buf;
        tmp_buf.AppendRaw(dma->buf, kChunkSize);
        tmp_buf.ResetPos();

        if (!tmp_buf.CompareRaw(kVersionString, strlen(kVersionString)))
        {
            fprintf(stderr, "header version mismatch\n");
            error = true;
            break;
        }

        u64 next_header_pos = tmp_buf.Get<u64>();
        buf.AppendFromBuffer(tmp_buf);
        if (next_header_pos == 0)
        {
            break;
        }
        hchunk_pos = next_header_pos;
        header_exchunks_.push_back(Chunkmap::Index::CreateFromPos(hchunk_pos));
    }
    buf.ResetPos();
    if (!error)
    {
        while (!buf.Compare('\0'))
        {
            Inode *inode = Inode::CreateFromBuffer(buf, chunkmap_, ns_wrapper_);
            inodes_.push_back(inode);
        }
        ChunkmapRead();
    }
    ns_wrapper_.Free(dma);
    return !error;
}

inline void Header::WriteSub(HeaderBuffer &buf)
{
    for (auto itr = inodes_.begin(); itr != inodes_.end(); ++itr)
    {
        Inode *inode = *itr;
        inode->HeaderWrite(buf);
    }
    buf.Append('\0');
}