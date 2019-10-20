#pragma once
#include <list>
#include <atomic>
#include <array>

#include "misc.h"
#include "chunkmap.h"

class Header
{
public:
    Header(const unvme_ns_t *ns, int qnum);
    Header() = delete;
    void Release()
    {
        assert(!updated_);
        Lock();
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
        Unlock();
        //HardWrite();
    }
    ~Header()
    {
        assert(inodes_.empty());
    }
    Inode *GetInode(const std::string &fname)
    {
        Lock();
        for (auto itr = inodes_.begin(); itr != inodes_.end(); ++itr)
        {
            Inode *inode = *itr;
            if (inode->GetFname() == fname)
            {
                Unlock();
                return inode;
            }
        }
        Unlock();
        return nullptr;
    }
    int GetBlockSize()
    {
        return kHeaderBlockSize;
    }
    void WriteSync(int qnum)
    {
        std::array<Inode::AsyncIoContext, 2> ctx_array = Write(qnum);
        for (int i = 0; i < 2; i++)
        {
            if (unvme_apoll(ctx_array[i].iod, UNVME_TIMEOUT))
            {
                printf("failed to unvme_write");
                exit(1);
            }
            unvme_free(ns_, ctx_array[i].dma);
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
        Lock();
        for (auto itr = inodes_.begin(); itr != inodes_.end(); ++itr)
        {
            Inode *inode = *itr;
            const std::string &filename = inode->GetFname();

            if (filename.size() >= dir.size() + 1 && filename[dir.size()] == '/' && (memcmp(filename.data(), dir.data(), dir.size()) == 0))
            {
                result->push_back(filename.substr(dir.size() + 1));
            }
        }
        Unlock();
    }
    Inode *Create(const std::string &fname, bool lock, int qnum)
    {
        Inode *rd = GetInode(fname);

        Lock();
        if (rd == nullptr)
        {
            rd = Inode::CreateEmpty(fname, (lock ? 1 : 0), chunkmap_, ns_, ns_->blocksize, qnum);
            if (rd != nullptr)
            {
                inodes_.push_back(rd);
            }
        }

        Unlock();
        return rd;
    }
    void Delete(Inode *inode)
    {
        updated_ = true;
        Lock();
        inodes_.remove(inode);
        Unlock();
        inode->Sync();
        inode->Release();
        delete inode;
    }

private:
    void Lock()
    {
        while (lock_.fetch_or(1) == 1)
        {
            asm volatile("" ::
                             : "memory");
        }
    }
    void Unlock() { lock_ = 0; }
    std::array<Inode::AsyncIoContext, 2> Write(int qnum);
    bool Read(int qnum);
    void WriteSub();
    u32 GetBlockNumFromSize(size_t size)
    {
        return (size + ns_->blocksize - 1) / ns_->blocksize;
    }
    void ChunkmapRead(int qnum)
    {
        vfio_dma_t *dma = unvme_alloc(ns_, Chunkmap::kSize);
        if (!dma)
        {
            nvme_printf("allocation failure\n");
        }
        unvme_read(ns_, qnum, dma->buf, GetBlockNumFromSize(kChunkmapStartPos),
                   GetBlockNumFromSize(Chunkmap::kSize));
        chunkmap_.Read(dma->buf);
        unvme_free(ns_, dma);
    }
    Inode::AsyncIoContext ChunkmapWrite(int qnum)
    {
        vfio_dma_t *dma = unvme_alloc(ns_, Chunkmap::kSize);
        if (!dma)
        {
            nvme_printf("allocation failure\n");
        }
        chunkmap_.Write(dma->buf);
        unvme_iod_t iod = unvme_awrite(ns_, qnum, dma->buf, GetBlockNumFromSize(kChunkmapStartPos),
                                       GetBlockNumFromSize(Chunkmap::kSize));
        if (!iod)
        {
            printf("failed to unvme_write");
            exit(1);
        }
        Inode::AsyncIoContext ctx = {
            .iod = iod,
            .dma = dma,
            .time = ve_gettime(),
        };
        return ctx;
    }

    static const u64 kHeaderStartPos = 0;
    static const u64 kHeaderBlockSize = kChunkSize;

    Chunkmap chunkmap_;
    static const u64 kChunkmapStartPos = kHeaderStartPos + kHeaderBlockSize;
    static const u64 kDataStorageStartPos = kChunkmapStartPos + Chunkmap::kSize;
    const unvme_ns_t *ns_;
    std::atomic<uint> lock_;
    std::list<Inode *> inodes_;
    char buf_[kHeaderBlockSize];
    static const char *kVersionString;
    bool updated_;
};

inline Header::Header(const unvme_ns_t *ns, int qnum) : ns_(ns), lock_(0), updated_(false)
{
    if (!Read(qnum))
    {
        chunkmap_.Create(Chunkmap::Index::CreateFromPos(ns_->blockcount * ns_->blocksize), Chunkmap::Index::CreateFromPos(kDataStorageStartPos));
        WriteSync(qnum);
    }
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
    chunkmap_.Dump(100);
}

inline std::array<Inode::AsyncIoContext, 2> Header::Write(int qnum)
{
    WriteSub();
    vfio_dma_t *dma = unvme_alloc(ns_, kHeaderBlockSize);
    if (!dma)
    {
        nvme_printf("allocation failure\n");
    }
    memcpy(dma->buf, buf_, kHeaderBlockSize);
    unvme_iod_t iod = unvme_awrite(ns_, qnum, dma->buf, GetBlockNumFromSize(kHeaderStartPos),
                                   GetBlockNumFromSize(kHeaderBlockSize));
    if (!iod)
    {
        printf("failed to unvme_write");
        exit(1);
    }
    Inode::AsyncIoContext ctx1 = {
        .iod = iod,
        .dma = dma,
        .time = ve_gettime(),
    };
    Inode::AsyncIoContext ctx2 = ChunkmapWrite(qnum);
    updated_ = false;
    return {ctx1, ctx2};
}

inline bool Header::Read(int qnum)
{
    ChunkmapRead(qnum);

    vfio_dma_t *dma = unvme_alloc(ns_, kHeaderBlockSize);
    if (!dma)
    {
        nvme_printf("allocation failure\n");
    }
    if (unvme_read(ns_, qnum, dma->buf, GetBlockNumFromSize(kHeaderStartPos), GetBlockNumFromSize(kHeaderBlockSize)))
    {
        printf("failed to unvme_read");
        exit(1);
    }
    memcpy(buf_, dma->buf, kHeaderBlockSize);

    unvme_free(ns_, dma);

    int pos = 0;
    pos += strlen(kVersionString);
    if (strncmp(buf_, kVersionString, strlen(kVersionString)))
    {
        fprintf(stderr, "header version mismatch\n");
        return false;
    }
    while (buf_[pos] != '\0')
    {
        int len = 0;
        Inode *inode = Inode::CreateFromBuffer(buf_ + pos, chunkmap_, ns_, ns_->blocksize, len, qnum);
        inodes_.push_back(inode);
        pos += len;
    }
    return true;
}

inline void Header::WriteSub()
{
    Lock();
    int pos = 0;
    memset(buf_, 0, kHeaderBlockSize);
    memcpy(buf_, kVersionString, strlen(kVersionString));
    pos += strlen(kVersionString);
    for (auto itr = inodes_.begin(); itr != inodes_.end(); ++itr)
    {
        Inode *inode = *itr;
        pos += inode->HeaderWrite(buf_ + pos, kHeaderBlockSize - pos);
    }
    buf_[pos] = '\0';
    Unlock();
}