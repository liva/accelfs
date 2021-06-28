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

#include <string>
#include <atomic>
#include "misc.h"
#include "chunkmap.h"
#include "unvme_wrapper.h"
#include "chunklist.h"
#include "cachelist.h"
#include "autogen.h"

class Inode
{
public:
  enum class Status
  {
    kOk,
    kIoError,
    kTryAgain,
  };
  struct AsyncIoContext
  {
    unvme_iod_t iod;
    SharedDmaBuffer dma;
    uint64_t time;
  };
  Inode(const Inode &) = delete;
  Inode &operator=(const Inode &) = delete;
  Inode(Inode &&) = delete;
  Inode() = delete;
  Inode &operator=(Inode &&) = delete;
  static Inode *CreateEmpty(std::string fname, int lock, Chunkmap &chunkmap, UnvmeWrapper &ns_wrapper)
  {
    Chunkmap::Index cindex = chunkmap.FindUnused();
    if (cindex.IsNull())
    {
      fprintf(stderr, "no space on the stroage");
      return nullptr;
    }
    return new Inode(fname, new ChunkList(cindex.GetPos() / ns_wrapper.GetBlockSize()), chunkmap, ns_wrapper);
  }
  static Inode *CreateFromBuffer(HeaderBuffer &buf, Chunkmap &chunkmap, UnvmeWrapper &ns_wrapper)
  {
    auto fname = buf.GetString();
    uint64_t top_lba = buf.Get<uint64_t>();

    std::vector<vfio_dma_t *> dma_list;
    std::map<uint64_t, void *> buf_map;

    {
      vfio_dma_t *dma = (vfio_dma_t *)malloc(sizeof(vfio_dma_t));
      unvme_iod_t iod;
      if (ns_wrapper.AreadChunk(top_lba, dma, iod) != 0)
      {
        return nullptr;
      }
      dma_list.push_back(dma);
      if (ns_wrapper.Apoll(iod))
      {
        return nullptr;
      }
      buf_map.insert(std::make_pair(top_lba, dma->buf));
    }

    std::vector<uint64_t> required_lba_list;
    ChunkList::AnalyzeForConstruction(top_lba, buf_map, required_lba_list);

    while (!required_lba_list.empty())
    {
      for (auto it = required_lba_list.begin(); it != required_lba_list.end(); ++it)
      {
        auto lba = *it;

        vfio_dma_t *dma = (vfio_dma_t *)malloc(sizeof(vfio_dma_t));
        unvme_iod_t iod;
        if (ns_wrapper.AreadChunk(lba, dma, iod) != 0)
        {
          return nullptr;
        }
        dma_list.push_back(dma);
        if (ns_wrapper.Apoll(iod))
        {
          return nullptr;
        }
        buf_map.insert(std::make_pair(lba, dma->buf));
      }
      required_lba_list.clear();
      ChunkList::AnalyzeForConstruction(top_lba, buf_map, required_lba_list);
    }

    ChunkList *cl = ChunkList::CreateFromBuffer(top_lba, buf_map);
    Inode *inode = new Inode(fname, cl, chunkmap, ns_wrapper);
    inode->inode_updated_ = false;

    for (auto it = dma_list.begin(); it != dma_list.end(); ++it)
    {
      ns_wrapper.Free(*it);
      free(*it);
    }

    return inode;
  }
  void Release()
  {
    cachelist_.Release();
  }
  ~Inode()
  {
    assert(!inode_updated_);
    assert(io_waiting_queue_.IsEmpty());
    if (chunklist_ != nullptr)
    {
      delete chunklist_;
    }
    double ioqueue_overflow_rate = (io_wait_ctxfull_cnt_ * 100.0) / io_wait_cnt_;
    if (ioqueue_overflow_rate > 10.0) {
      fprintf(stderr, "DEBUG: Inode: IO queue overflowed at %f\n", ioqueue_overflow_rate);
    }
  }
  Status Truncate(size_t len)
  {
    if (len > chunklist_->MaxLen())
    {
      return Status::kIoError;
    }
    size_t old_len = GetLen();
    std::vector<uint64_t> release_list;
    chunklist_->Truncate(len, release_list);
    if (len < old_len)
    {
      for (auto it = release_list.begin(); it != release_list.end(); ++it)
      {
        Chunkmap::Index cindex = Chunkmap::Index::CreateFromPos(*it * ns_wrapper_.GetBlockSize());
        chunkmap_.Release(cindex);
      }
      cachelist_.Truncate(len);
    }
    else
    {
      assert(release_list.empty());
    }
    return Status::kOk;
  }
  Status Expand(size_t len)
  {
    if (len > chunklist_->MaxLen())
    {
      return Status::kIoError;
    }
    size_t old_len = GetLen();
    if (len < old_len)
    {
      return Status::kIoError;
    }
    chunklist_->Expand(len);
    return Status::kOk;
  }
  Status Write(size_t offset, const void *data, size_t size)
  {
    MEASURE_TIME;
    if (size == 0) {
      return Status::kOk;
    }
    Spinlock lock(GetLock());
    vefs_printf("w[%s %lu %lu]\n", GetFname().c_str(), offset, size);
    if (redirect_)
    {
      RedirectWrite(offset, data, size);
    }

    size_t oldsize = GetLen();
    size_t end = offset + size;
    if (end > oldsize)
    {
      if (Expand(end) != Status::kOk)
      {
        fprintf(stderr, "vefs: expand error(%lu %lu %lu %lu)\n", end, oldsize, offset, size);
        return Status::kIoError;
      }
    }

    size_t written = 0;

#ifdef FAST_APPEND
    // append
    if (offset == oldsize) {
      // write to cache if available
      while(cachelist_.CheckIfExistAndIncCnt(ChunkIndex::CreateFromPos(offset + written))) {
        size_t cend = AlignChunk(offset + written + kChunkSize);
        if (cend > end) {
          cend = end;
        }
        AppendInChunk(offset + written, (const char*)data + written, cend - offset - written);
        written = cend - offset;
        if (written == size) {
          return Status::kOk;
        }
      }

      // no cache, then replace the old cache buffer with a new dmabuffer
      if ((((offset + written) % kChunkSize) == 0) && (size - written <= 2 * 1024 * 1024)) {
        #if 1
        const size_t cache_size = 2 * 1024 * 1024;
        #else
        const size_t cache_size = AlignChunkUp(size - written);
        #endif
        SharedDmaBuffer dma = SharedDmaBuffer(dmabuf_allocator_, ns_wrapper_, cache_size);
        memcpy((char *)dma.GetBuffer(), (const char*)data + written, size - written);
        std::pair<ChunkIndex, Cache> release_cache_list[cache_size / kChunkSize];
        int release_cache_cnt = cachelist_.RegisterToCache(dma.GetSize() / kChunkSize, ChunkIndex::CreateFromPos(offset + written), std::move(dma), true, release_cache_list);
        
        CleanupCacheListsAndContexts(release_cache_cnt, release_cache_list);

        return Status::kOk;
      }
    }
#endif
    return WriteInternal(offset + written, (const char*)data + written, size - written);
  }
  Status
  Read2(uint64_t offset, size_t size, char *scratch)
  {
    MEASURE_TIME;
    Spinlock lock(GetLock());
    if ((offset % kChunkSize) != 0)
    {
      return Status::kIoError;
    }
    if ((size % kChunkSize) != 0)
    {
      return Status::kIoError;
    }

    size_t flen = GetLen();
    if (offset > flen)
    {
      return Status::kOk;
    }
    if (offset + size > flen)
    {
      size = flen - offset;
    }
    if (!io_waiting_queue_.IsEmpty()) {
      Spinlock lock(ns_wrapper_.GetLockFlag());
      RetrieveContexts();
    }

    if (size > 2 * 1024 * 1024)
    {
      // TODO support
      fprintf(stderr, "not supported\n");
      return Status::kIoError;
    }

    // TODO this time we use SharedDmaBuffer, but it should be another lightweight class
    struct ReadIoContext
    {
      uint64_t lba;
      ChunkIndex cindex;
      size_t size;
      char *data;
      SharedDmaBuffer dma;
      unvme_iod_t iod;
    };
    std::vector<ReadIoContext> io_list;
    std::vector<ChunkIndex> incoming_indexes;
    {
      size_t coffset = offset;
      char *cdata = scratch;
      size_t csize = size;
      incoming_indexes.reserve(size / kChunkSize + 1);
      while (csize != 0)
      {
        size_t boundary = GetNextChunkBoundary(coffset);
        size_t io_size = (coffset + csize > boundary) ? boundary - coffset : csize;

        ChunkIndex cindex = ChunkIndex::CreateFromPos(coffset);

          uint64_t lba;
          if (GetLba(cindex.GetPos(), lba) != Status::kOk)
          {
            return Status::kIoError;
          }
            io_list.push_back(std::move(ReadIoContext{
                lba,
                cindex,
                io_size,
                cdata,
                SharedDmaBuffer(),
                nullptr,
            }));

        coffset += io_size;
        cdata += io_size;
        csize -= io_size;
      }
    }

    // this time, we don't use cache
    // just read it from nvme

    if (!io_list.empty())
    {
      Spinlock lock(ns_wrapper_.GetLockFlag());
      {
        for (auto it = io_list.begin(); it != io_list.end(); ++it)
        {
          it->dma = SharedDmaBuffer(dmabuf_allocator_, ns_wrapper_, it->size);
          it->iod = ns_wrapper_.AreadInternal(it->dma.GetBuffer(), it->lba, it->size / ns_wrapper_.GetBlockSize());
        }
      }
    }
    {
      for (auto it = io_list.begin(); it != io_list.end(); ++it)
      {
        if (ns_wrapper_.Apoll(it->iod))
        {
          abort();
        }
      }
    }
    {
      for (auto it = io_list.begin(); it != io_list.end(); ++it)
      {
        memcpy(it->data, (char *)it->dma.GetBuffer(), it->size);
      }
    }


    if (redirect_)
    {
      RedirectRead(offset, size, scratch);
    }

    return Status::kOk;
  }
  Status
  Read(uint64_t offset, size_t size, char *scratch)
  {
    //printf("read %lu %lu\n", offset, size);
    MEASURE_TIME;
    Spinlock lock(GetLock());
    size_t flen = GetLen();
    if (offset > flen)
    {
      return Status::kOk;
    }
    if (offset + size > flen)
    {
      size = flen - offset;
    }
    if (!io_waiting_queue_.IsEmpty()) {
      Spinlock lock(ns_wrapper_.GetLockFlag());
      RetrieveContexts();
    }

    struct ReadIoContext
    {
      uint64_t lba;
      ChunkIndex cindex;
      size_t inblock_offset;
      size_t size;
      char *data;
      SharedDmaBuffer dma;
      unvme_iod_t iod;
    };
    std::vector<ReadIoContext> io_list;
    std::vector<ChunkIndex> incoming_indexes;
    {
      size_t coffset = offset;
      char *cdata = scratch;
      size_t csize = size;
      incoming_indexes.reserve(size / kChunkSize + 1);
      while (csize != 0)
      {
        size_t boundary = GetNextChunkBoundary(coffset);
        size_t io_size = (coffset + csize > boundary) ? boundary - coffset : csize;

        size_t noffset = AlignChunk(coffset);
        size_t ndsize = AlignChunkUp(io_size);
        size_t inblock_offset = coffset - noffset;
        assert(ndsize == kChunkSize);
        ChunkIndex cindex = ChunkIndex::CreateFromPos(noffset);

        if (!cachelist_.CheckIfExistAndIncCnt(cindex))
        {
          incoming_indexes.push_back(cindex);

          uint64_t lba;
          if (GetLba(cindex.GetPos(), lba) != Status::kOk)
          {
            return Status::kIoError;
          }
          bool new_ioctx = true;
          if (!io_list.empty())
          {
            ReadIoContext &ctx = io_list.back();
            size_t pctx_iosize = ctx.inblock_offset + ctx.size;
            if (IsAbleToConcatIoBlock(ctx.cindex, cindex, pctx_iosize, io_size, ctx.lba, lba))
            {
              assert((pctx_iosize % kChunkSize) == 0);
              assert(inblock_offset == 0);
              ctx.size += io_size;

              new_ioctx = false;
            }
          }
          if (new_ioctx)
          {
            io_list.push_back(std::move(ReadIoContext{
                lba,
                cindex,
                inblock_offset,
                io_size,
                cdata,
                SharedDmaBuffer(),
                nullptr,
            }));
          }
        }
        else
        {
          cachelist_.Apply(cindex, cdata, coffset - noffset, io_size);
        }

        coffset += io_size;
        cdata += io_size;
        csize -= io_size;
      }
    }

    if (!io_list.empty())
    {
      Spinlock lock(ns_wrapper_.GetLockFlag());
      {
        for (auto it = io_list.begin(); it != io_list.end(); ++it)
        {
          size_t size = alignup(it->inblock_offset + it->size, kChunkSize);
          it->dma = SharedDmaBuffer(dmabuf_allocator_, ns_wrapper_, size);
          it->iod = ns_wrapper_.AreadInternal(it->dma.GetBuffer(), it->lba, size / ns_wrapper_.GetBlockSize());
        }
      }
    }
    {
      OrganizeCacheList(incoming_indexes, 0);
    }
    {
      for (auto it = io_list.begin(); it != io_list.end(); ++it)
      {
        if (ns_wrapper_.Apoll(it->iod))
        {
          abort();
        }
      }
    }
    {
      int release_cache_list_max = ((2 * 1024 * 1024) / kChunkSize) * io_list.size();
      std::pair<ChunkIndex, Cache> release_cache_list[release_cache_list_max];
      int release_cache_cnt = 0;
      for (auto it = io_list.begin(); it != io_list.end(); ++it)
      {
        size_t size = alignup(it->inblock_offset + it->size, kChunkSize);
        int chunknum = static_cast<int>(size / kChunkSize);
        memcpy(it->data, (char *)it->dma.GetBuffer() + it->inblock_offset, it->size);
        release_cache_cnt += cachelist_.RegisterToCache(chunknum, it->cindex, std::move(it->dma), false, release_cache_list + release_cache_cnt);
      }
      if (release_cache_cnt != 0) {
        Spinlock lock(ns_wrapper_.GetLockFlag());
        for (int i = 0; i < release_cache_cnt; i++) {
          ReleaseCacheList(release_cache_list[i].first, release_cache_list[i].second);
        }
      }
    }

    vefs_printf("r[%s %lu %lu]\n", GetFname().c_str(), offset, size);
    if (redirect_)
    {
      RedirectRead(offset, size, scratch);
    }
    return Status::kOk;
  }
  size_t GetLen()
  {
    return chunklist_->GetLen();
  }
  Status GetLba(size_t offset, u64 &lba)
  {
    size_t chunk_index = offset / kChunkSize;
    uint64_t lba_tmp = chunklist_->GetFromIndex(chunk_index);
    while (lba_tmp == 0)
    {
      Chunkmap::Index cindex = chunkpool_.Get();
      if (cindex.IsNull())
      {
        return Status::kIoError;
      }
      uint64_t allocated_lba = cindex.GetPos() / ns_wrapper_.GetBlockSize();
      if (chunklist_->Apply(chunk_index, allocated_lba))
      {
        assert(allocated_lba == chunklist_->GetFromIndex(chunk_index));
        lba_tmp = allocated_lba;
      }
    }
    lba = lba_tmp + (u64)((offset % kChunkSize) / ns_wrapper_.GetBlockSize());
    return Status::kOk;
  }
  size_t GetNextChunkBoundary(size_t offset)
  {
    int ci = offset / kChunkSize;
    ci++;
    return ci * kChunkSize;
  }
  void Rename(const std::string &fname)
  {
    fname_ = fname;
    inode_updated_ = true;
  }
  void HeaderWrite(HeaderBuffer &buf)
  {
    buf.AppendRaw(fname_.c_str(), fname_.length());
    buf.Append('\0');
    buf.AlignPos();

    buf.Append(chunklist_->GetLba());

    inode_updated_ = false;
  }
  Status OrganizeCacheList(std::vector<ChunkIndex> incoming_indexes, int keep_num)
  {
    CacheList::Vector release_cache_list;
    cachelist_.ReserveSlots(incoming_indexes, release_cache_list);
    //cachelist_.ShrinkIfNeeded(keep_num, release_cache_list);
    Spinlock lock(ns_wrapper_.GetLockFlag());
    Status s = Status::kOk;
    for (auto it = release_cache_list.begin(); it != release_cache_list.end(); ++it) {
      Status s_tmp = ReleaseCacheList((*it).first, (*it).second);
      if (s_tmp != Status::kOk) {
        s = s_tmp;
      }
    }
    return s;
  }
  Status ReleaseCacheList(ChunkIndex index, Cache &c)
  {
    assert(Spinlock::IsAcquired(ns_wrapper_.GetLockFlag()));
    if (!c.IsValid()) {
      return Status::kOk;
    }
    if (c.IsWriteNeeded())
      {
        AsyncIoContext ctx;
        if (index.GetPos() >= GetLen()) {
          c.ForceRelease();
        } else {
          if (CacheSync(c, index, ctx) != Status::kOk)
            {
              fprintf(stderr, "VEFS: cache awrite failed\n");
              return Status::kIoError;
            }
          RegisterWaitingContext(std::move(ctx));
        }
      }
    c.Release();
    return Status::kOk;
  }
  void RetrieveContexts()
  {
    assert(Spinlock::IsAcquired(ns_wrapper_.GetLockFlag()));
     while (true) {
      if (io_waiting_queue_.IsEmpty()) {
        return;
      }
      AsyncIoContext &ctx = io_waiting_queue_.GetHead();
      if (ns_wrapper_.ApollWithoutWaitInternal(ctx.iod) != 0)
      {
        return;
      }
      io_waiting_queue_.PopHead();
    }
  }
  void WaitIoCompletion()
  {
    while (true) {
      if (io_waiting_queue_.IsEmpty()) {
        return;
      }
      AsyncIoContext &ctx = io_waiting_queue_.GetHead();
      if (ns_wrapper_.Apoll(ctx.iod) != 0)
      {
        printf("failed to unvme_write");
        abort();
      }
      io_waiting_queue_.PopHead();
    }
  }
  void CacheListSync()
  {
    CacheList::Vector sync_cache_list;
    cachelist_.CacheListSync(sync_cache_list);
    Spinlock lock(ns_wrapper_.GetLockFlag());
    for (auto it = sync_cache_list.begin(); it != sync_cache_list.end(); ++it) {
      ReleaseCacheList((*it).first, (*it).second);
    }
  }
  void SyncChunkList()
  {
    if (IsChunkListUpdated())
    {
      while (true)
      {
        SharedDmaBuffer dma(dmabuf_allocator_, ns_wrapper_, kChunkSize);
        uint64_t lba;
        bool needs_additional_write = ChunkListWrite(dma.GetBuffer(), lba);
        unvme_iod_t iod = ns_wrapper_.Awrite(dma.GetBuffer(), lba,
                                             kChunkSize / ns_wrapper_.GetBlockSize());
        if (!iod)
        {
          printf("failed to unvme_write");
          abort();
        }
        {
          Spinlock lock(ns_wrapper_.GetLockFlag());
          RegisterWaitingContext(AsyncIoContext{
                                                .iod = iod,
                                                .dma = std::move(dma),
                                                .time = ve_gettime(),
            });
        }
        if (!needs_additional_write)
        {
          break;
        }
      }
    }
    assert(!IsChunkListUpdated());
  }
  bool IsUpdated()
  {
    return inode_updated_;
  }
  void DumpChunkList()
  {
    chunklist_->Dump();
  }
  bool IsChunkListUpdated()
  {
    return chunklist_->IsUpdated();
  }
  bool ChunkListWrite(void *buf, uint64_t &lba)
  {
    return chunklist_->Write(buf, lba);
  }
  void RegisterWaitingContext(AsyncIoContext &&ctx)
  {
    assert(Spinlock::IsAcquired(ns_wrapper_.GetLockFlag()));
    io_wait_cnt_++;
    if (io_waiting_queue_.IsFull()) {
      io_wait_ctxfull_cnt_++;
      while(io_waiting_queue_.IsFull()) {
        RetrieveContexts();
      }
    }
    io_waiting_queue_.Push(std::move(ctx));
  }
  void Delete()
  {
    inode_updated_ = false;
    WaitIoCompletion();
    Truncate(0);
    chunkmap_.Release(Chunkmap::Index::CreateFromPos(chunklist_->GetLba() * ns_wrapper_.GetBlockSize()));
    uint64_t dummy_lba;
    uint64_t buf[kChunkSize / sizeof(uint64_t)];
    chunklist_->Write(reinterpret_cast<void *>(buf), dummy_lba);
    delete chunklist_;
    chunklist_ = nullptr;
  }
  std::string &GetFname()
  {
    return fname_;
  }

  std::atomic<int> &GetLock()
  {
    return lock_;
  }

private:
  Inode(std::string fname, ChunkList *chunklist, Chunkmap &chunkmap, UnvmeWrapper &ns_wrapper) : lock_(0), chunkmap_(chunkmap), ns_wrapper_(ns_wrapper), chunkpool_(chunkmap_)
  {
    fname_ = fname;
    chunklist_ = chunklist;
    inode_updated_ = true;
  }
  Status CacheSync(Cache &cache, ChunkIndex index, Inode::AsyncIoContext &ctx)
  {
    SharedDmaBuffer dma;
    size_t buf_offset;
    cache.MarkSynced(dma, buf_offset);
    u64 lba;
    if (GetLba(index.GetPos(), lba) != Status::kOk)
    {
      return Status::kIoError;
    }
    unvme_iod_t iod = ns_wrapper_.AwriteInternal((void *)((char *)dma.GetBuffer() + buf_offset), lba, kChunkSize / ns_wrapper_.GetBlockSize());
    if (!iod)
    {
      fprintf(stderr, "VEFS: cache awrite failed\n");
      return Status::kIoError;
    }
    ctx = Inode::AsyncIoContext{
        .iod = iod,
        .dma = std::move(dma),
        .time = ve_gettime(),
    };
    return Status::kOk;
  }
  void RedirectRead(uint64_t offset, size_t size, char *scratch)
  {
    void *buf = malloc(size);
    int fd = open((GetFname()).c_str(), O_RDWR | O_CREAT);
    pread(fd, buf, size, offset);
    for (size_t i = 0; i < size; i++)
    {
      if (((char *)buf)[i] != scratch[i])
      {
        printf("check failed at %ld (%s %lu %zu)\n", i, GetFname().c_str(), offset, size);
        exit(1);
      }
    }
    free(buf);
    close(fd);
  }
  void RedirectWrite(size_t offset, const void *data, size_t size)
  {
    std::string fname = GetFname();
    int fd = open(fname.c_str(), O_RDWR | O_CREAT);
    pwrite(fd, data, size, offset);
    close(fd);
  }
  bool IsAbleToConcatIoBlock(ChunkIndex prev_cindex, ChunkIndex cur_cindex, size_t prev_iosize, size_t cur_iosize, uint64_t prev_lba, uint64_t cur_lba)
  {
    return ChunkIndex::CreateFromPos(prev_cindex.GetPos() + prev_iosize) == cur_cindex &&
           prev_lba + prev_iosize / ns_wrapper_.GetBlockSize() == cur_lba &&
           prev_iosize + cur_iosize <= 2 * 1024 * 1024;
  }
  Status AppendInChunk(size_t offset, const char *data, size_t size) {
    cachelist_.Refresh(ChunkIndex::CreateFromPos(offset), data, offset - AlignChunk(offset), size);
    return Status::kOk;
  }
  Status WriteInternal(size_t offset, const char *data, size_t size)
  {
    //    printf("WARNING: slow path\n");
    size_t oldsize = GetLen();
    size_t end = offset + size;

    size_t aoffset = AlignChunk(offset);

    struct DmaContext
    {
      ChunkIndex cindex;
      SharedDmaBuffer dma;
    };

    size_t dma_ctx_array_size = (AlignChunkUp(end) - aoffset) / kChunkSize;
    DmaContext dma_list[dma_ctx_array_size];
    size_t dma_ctx_num = 0;

    {
      for (size_t coffset = aoffset; coffset < AlignChunkUp(end); coffset += 2 * 1024 * 1024)
      {
        size_t size = 2 * 1024 * 1024;
        if (coffset + size > AlignChunkUp(end))
        {
          size = AlignChunkUp(end) - coffset;
        }
        dma_list[dma_ctx_num] = DmaContext{
            ChunkIndex::CreateFromPos(coffset),
            SharedDmaBuffer(dmabuf_allocator_, ns_wrapper_, size)};
        dma_ctx_num++;
      }
    }
    struct IoContext
    {
      uint64_t lba;
      ChunkIndex cindex;
      DmaContext *dma;
      size_t indmabuf_offset;
      unvme_iod_t iod;
    };

    int io_list_max = 0;
    for (size_t coffset = offset; coffset < end; coffset = GetNextChunkBoundary(coffset)) {
      io_list_max++;
    }
    int io_list_cnt = 0;

    {
      IoContext io_list[io_list_max];
      for (size_t coffset = offset; coffset < end; coffset = GetNextChunkBoundary(coffset)) {
        size_t caoffset = AlignChunk(coffset);
        if (oldsize <= caoffset) {
          // no cache and no data on the storage, thus we can skip the following step
          continue;
        }
        
        size_t boundary = GetNextChunkBoundary(coffset);
        size_t io_size = boundary - coffset;
        if (boundary > end)
          {
            io_size = end - coffset;
          }
                
        ChunkIndex cindex = ChunkIndex::CreateFromPos(coffset);
        
        bool whole_overwrite = (coffset == caoffset && io_size == kChunkSize);
        
        size_t dma_list_index = (coffset - aoffset) / (2 * 1024 * 1024);
        size_t indmabuf_offset = (caoffset - aoffset) % (2 * 1024 * 1024);

        DmaContext *dma_ctx = &dma_list[dma_list_index];

        if (!cachelist_.CheckIfExistAndIncCnt(cindex)) {
          uint64_t lba;
          if (GetLba(cindex.GetPos(), lba) != Status::kOk) {
            fprintf(stderr, "vefs: getlba error\n");
            return Status::kIoError;
          }
          if (!whole_overwrite) {
            io_list[io_list_cnt]
              = std::move(IoContext{
                                    lba,
                                    cindex,
                                    dma_ctx,
                                    indmabuf_offset,
                                    nullptr,
                });
            io_list_cnt++;
          }
        } else {
          if (!whole_overwrite) {
            cachelist_.Apply(cindex, (char *)dma_ctx->dma.GetBuffer() + indmabuf_offset, 0, kChunkSize);
          }
          cachelist_.ForceRelease(cindex); // new cache will be registered from buffer later
        }
      }

      if (io_list_cnt != 0) {
        {
          Spinlock lock(ns_wrapper_.GetLockFlag());
          
          for (int i = 0; i < io_list_cnt; i++) {
            IoContext &ctx = io_list[i];
            ctx.iod = ns_wrapper_.AreadInternal((void *)((char *)ctx.dma->dma.GetBuffer() + ctx.indmabuf_offset), ctx.lba, kChunkSize / ns_wrapper_.GetBlockSize());
          }
        }
      
        for (int i = 0; i < io_list_cnt; i++) {
          IoContext &ctx = io_list[i];
          if (ns_wrapper_.Apoll(ctx.iod)) {
            printf("unvme apoll failed\n");
            abort();
          }
        }
      }

      {
        size_t indma_offset = offset % kChunkSize;
        const char *cdata = data;
        size_t csize = size;
        for (size_t dma_list_index = 0; dma_list_index < dma_ctx_num; dma_list_index++)
          {
            size_t copy_size = dma_list[dma_list_index].dma.GetSize() - indma_offset;
            if (csize < copy_size)
              {
                copy_size = csize;
              }
            memcpy((char *)dma_list[dma_list_index].dma.GetBuffer() + indma_offset, cdata, copy_size);
            indma_offset = 0;
            cdata += copy_size;
            csize -= copy_size;
          }
        assert(csize == 0);
      }
    }

    {
      int release_cache_list_max = ((2 * 1024 * 1024) / kChunkSize) * dma_ctx_num;
      std::pair<ChunkIndex, Cache> release_cache_list[release_cache_list_max];
      int release_cache_cnt = 0;
      for (size_t dma_list_index = 0; dma_list_index < dma_ctx_num; dma_list_index++)
      {
        release_cache_cnt += cachelist_.RegisterToCache(dma_list[dma_list_index].dma.GetSize() / kChunkSize, dma_list[dma_list_index].cindex, std::move(dma_list[dma_list_index].dma), true, release_cache_list + release_cache_cnt);
      }

      CleanupCacheListsAndContexts(release_cache_cnt, release_cache_list);
    }

    return Status::kOk;
  }
  void CleanupCacheListsAndContexts(int release_cache_cnt, std::pair<ChunkIndex, Cache> *release_cache_list) {
    if (!io_waiting_queue_.IsEmpty() || release_cache_cnt != 0) {
      Spinlock lock(ns_wrapper_.GetLockFlag());
      if (!io_waiting_queue_.IsEmpty()) {
        RetrieveContexts();
      }
      for (int i = 0; i < release_cache_cnt; i++) {
        ReleaseCacheList(release_cache_list[i].first, release_cache_list[i].second);
      }
    }
  }
  template <class T>
  T AlignChunk(T val)
  {
    return align(val, kChunkSize);
  }
  template <class T>
  T AlignChunkUp(T val)
  {
    return alignup(val, kChunkSize);
  }

  std::atomic<int> lock_;

  std::string fname_;
  ChunkList *chunklist_;

  Chunkmap &chunkmap_;
  UnvmeWrapper &ns_wrapper_;
  StaticAllocator<DmaBufferWrapper> dmabuf_allocator_;

  class ChunkPool {
  public:
    ChunkPool() = delete;
    ChunkPool(Chunkmap &chunkmap) : chunkmap_(chunkmap) {
    }
    ~ChunkPool() {
      for(int i = offset_; i < kSize; i++) {
        chunkmap_.Release(array_[i]);
      }
    }
    Chunkmap::Index Get() {
      if (offset_ == kSize) {
        chunkmap_.FindUnused(kSize, array_);
        offset_ = 0;
      }
      int i = offset_;
      offset_++;
      return array_[i];
    }
    static const int kSize = 32;
  private:
    int offset_ = kSize;
    Chunkmap &chunkmap_;
    Chunkmap::Index array_[kSize];
  } chunkpool_;

  bool inode_updated_;
  CacheList cachelist_;

  class IoWaitingQueue {
  public:
    bool IsEmpty() {
      return head_ == tail_;
    }
    void Push(AsyncIoContext &&ctx) {
      // do not push when the buffer is full
      assert(!IsFull());
      new (&buf_[tail_])AsyncIoContext(std::move(ctx));
      tail_ = Next(tail_);
    }
    bool IsFull() {
      return Next(tail_) == head_;
    }
    AsyncIoContext &GetHead() {
      assert(!IsEmpty());
      return buf_[head_];
    }
    void PopHead() {
      assert(!IsEmpty());
      buf_[head_].~AsyncIoContext();
      head_ = Next(head_);
    }
  private:
    int Next(const int v) {
      return (v + 1) % kSize;
    }
    #if 1
    static const int kSize = 4096;
    #else
    static const int kSize = 256;
    #endif
    int head_ = 0;
    int tail_ = 0;
    AsyncIoContext buf_[kSize];
  } io_waiting_queue_;
  int64_t io_wait_ctxfull_cnt_ = 0;
  int64_t io_wait_cnt_ = 0;

  static size_t AlignUp(size_t len)
  {
    return ((len + 3) / 4) * 4;
  }
};
