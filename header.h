/**
 * Copyright 2020 NEC Laboratories Europe GmbH
 * All rights reserved
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *    1. Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *    2. Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 * 
 *    3. Neither the name of NEC Laboratories Europe GmbH nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY NEC Laboratories Europe GmbH AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL NEC Laboratories 
 * Europe GmbH OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO,  PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#pragma once
#include <atomic>
#include <deque>
#include <vector>
#include "spinlock.h"
#include "misc.h"
#include "chunkmap.h"
#include "unvme_wrapper.h"

class Header
{
public:
    Header(UnvmeWrapper &ns_wrapper) : ns_wrapper_(ns_wrapper), lock_(0), updated_(false)
    {
        if (!Read())
        {
            chunkmap_.Create(Chunkmap::Index::CreateFromPos(ns_wrapper_.GetBlockCount() * ns_wrapper_.GetBlockSize()), Chunkmap::Index::CreateFromPos(kDataStorageStartPos));
            WriteSync();
        }
        if (header_dump_)
        {
            Dump();
            chunkmap_.Dump(1, 500);
        }
    }
    Header() = delete;
    void Release()
    {
        assert(!updated_);
        Spinlock lock(lock_);
        int i = 0;
        for (auto itr = inodes_.begin(); itr != inodes_.end(); ++itr)
        {
            Inode *inode = *itr;
            {
                Spinlock ilock(inode->GetLock());
                inode->CacheListSync();
                inode->SyncChunkList();
            }
        }
        for (auto itr = inodes_.begin(); itr != inodes_.end(); ++itr)
        {
            Inode *inode = *itr;
            {
                Spinlock ilock(inode->GetLock());
                inode->WaitIoCompletion();
                bool inode_updated = inode->IsUpdated();
                assert(!inode_updated);
                inode->Release();
            }
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
            {
                Spinlock ilock(inode->GetLock());
                if (inode->GetFname() == fname)
                {
                    return inode;
                }
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
            {
                Spinlock ilock(inode->GetLock());
                const std::string &filename = inode->GetFname();
                if (filename.size() >= dir.size() + 1 && filename[dir.size()] == '/' && (memcmp(filename.data(), dir.data(), dir.size()) == 0))
                {
                    result->push_back(filename.substr(dir.size() + 1));
                }
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
        {
            Spinlock lock(inode->GetLock());
            // printf("Vefs::Delete %s\n", inode->fname.c_str());
            inode->Delete();
            inode->Release();
        }
        delete inode;
    }
    void Dump()
    {
        printf(">>>>\n");
        for (auto itr = inodes_.begin(); itr != inodes_.end(); ++itr)
        {
            Inode *inode = *itr;
            printf("> %p %s [", inode, inode->GetFname().c_str());
            /*            auto clist = inode->GetChunkList();
            for (auto it = clist.begin(); it != clist.end(); ++it)
            {
                printf("%lu ", *it);
            }*/
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
        vfio_dma_t dma;
        ns_wrapper_.Alloc(&dma, kChunkSize);
        for (u64 offset = 0; offset < Chunkmap::kSize; offset += kChunkSize)
        {
            ns_wrapper_.Read(dma.buf, GetBlockNumFromSize(kChunkmapStartPos + offset),
                             GetBlockNumFromSize(kChunkSize));
            chunkmap_.Read(offset, dma.buf);
        }
        ns_wrapper_.Free(&dma);
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
            SharedDmaBuffer dma(dmabuf_allocator_, ns_wrapper_, kChunkSize);
            chunkmap_.Write(offset, dma.GetBuffer());
            unvme_iod_t iod = ns_wrapper_.Awrite(dma.GetBuffer(), GetBlockNumFromSize(kChunkmapStartPos + offset),
                                                 GetBlockNumFromSize(kChunkSize));
            if (!iod)
            {
                printf("failed to unvme_write");
                abort();
            }
            ctxs.push_back(Inode::AsyncIoContext{
                .iod = iod,
                .dma = std::move(dma),
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
  StaticAllocator<DmaBufferWrapper> dmabuf_allocator_;
    std::atomic<int> lock_;
    std::vector<Inode *> inodes_;
    static const char *kVersionString;
    bool updated_;
    std::vector<Chunkmap::Index> header_exchunks_;
};
