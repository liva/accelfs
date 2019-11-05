#pragma once

#include <deque>
#include <string>
#include <list>
#include <vector>
#include <utility>
#include <deque>
#include <atomic>
#include "misc.h"
#include "chunkmap.h"
#include "unvme_wrapper.h"
#include "cache.h"
#include "chunklist.h"

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
    vfio_dma_t *dma;
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
      vfio_dma_t *dma;
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

        vfio_dma_t *dma;
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
    }

    return inode;
  }
  void Release()
  {
    for (auto it = cache_list_.begin(); it != cache_list_.end(); ++it)
    {
      Cache &cache = (*it).second;
      if (!cache.IsValid())
      {
        continue;
      }
      assert(!cache.IsWriteNeeded());
      ApollIod(cache);
      ns_wrapper_.Free(cache.Release());
    }
    cache_list_.clear();
  }
  ~Inode()
  {
    assert(!inode_updated_);
    assert(io_waiting_queue_.empty());
    if (cl_ != nullptr)
    {
      delete cl_;
    }
  }
  Status Truncate(size_t len)
  {
    if (len > cl_->MaxLen())
    {
      return Status::kIoError;
    }
    std::vector<uint64_t> release_list;
    cl_->Truncate(len, release_list);
    for (auto it = release_list.begin(); it != release_list.end(); ++it)
    {
      Chunkmap::Index cindex = Chunkmap::Index::CreateFromPos(*it * ns_wrapper_.GetBlockSize());
      chunkmap_.Release(cindex);
    }
    for (auto it = cache_list_.begin(); it != cache_list_.end(); ++it)
    {
      Cache &c = (*it).second;
      if (!c.IsValid())
      {
        continue;
      }
      ChunkIndex cindex = (*it).first;
      if (cindex.GetPos() > len)
      {
        ApollIod(c);
        ns_wrapper_.Free(c.ForceRelease());
      }
    }
    return Status::kOk;
  }
  size_t GetLen()
  {
    return cl_->GetLen();
  }
  Status GetLba(size_t offset, u64 &lba)
  {
    size_t chunk_index = offset / kChunkSize;
    uint64_t lba_tmp = cl_->GetFromIndex(chunk_index);
    while (lba_tmp == 0)
    {
      Chunkmap::Index cindex = chunkmap_.FindUnused();
      if (cindex.IsNull())
      {
        return Status::kIoError;
      }
      uint64_t allocated_lba = cindex.GetPos() / ns_wrapper_.GetBlockSize();
      if (cl_->Apply(chunk_index, allocated_lba))
      {
        assert(allocated_lba == cl_->GetFromIndex(chunk_index));
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

    buf.Append(cl_->GetLba());

    inode_updated_ = false;
  }
  Cache *FindFromCacheList(ChunkIndex cindex)
  {
    auto it = cache_list_.begin();
    for (; it != cache_list_.end(); ++it)
    {
      if ((*it).second.IsValid() && (*it).first == cindex)
      {
        break;
      }
    }
    if (it != cache_list_.end())
    {
      Cache *cache = &((*it).second);
      cache->SetTicket(cache_ticket_);
      cache_ticket_++;
      return cache;
    }
    return nullptr;
  }
  Status PrepareCache(ChunkIndex cindex, bool createnew_ifmissing)
  {
    Cache *c = FindFromCacheList(cindex);
    if (c)
    {
      return Status::kOk;
    }
    // could not find
    // create one
    u64 lba;
    // even if createnew_ifmissing==true, calling GetLba() is needed to prepare hierachical chunk managemet structure
    if (GetLba(cindex.GetPos(), lba) != Status::kOk)
    {
      return Status::kIoError;
    }
    vfio_dma_t *dma = ns_wrapper_.AllocChunk();
    if (!dma)
    {
      printf("allocation failure\n");
      exit(1);
    }
    unvme_iod_t iod = nullptr;
    if (!createnew_ifmissing)
    {
      iod = ns_wrapper_.Aread(dma->buf, lba, kChunkSize / ns_wrapper_.GetBlockSize());
    }
    auto it = cache_list_.begin();
    for (; it != cache_list_.end(); ++it)
    {
      if (!(*it).second.IsValid())
      {
        (*it).first = cindex;
        (*it).second.Reset(Cache(cache_ticket_, dma, iod));
        break;
      }
    }
    if (it == cache_list_.end())
    {
      cache_list_.push_back(std::move(std::make_pair(cindex, Cache(cache_ticket_, dma, iod))));
    }
    cache_ticket_++;

    return Status::kOk;
  }
  Status ShrinkCacheListIfNeeded(int keep_num)
  {
    if (keep_num < 32)
    {
      keep_num = 32;
    }
    uint64_t border_ticket = cache_ticket_ - keep_num;
    std::vector<std::pair<ChunkIndex, Cache>> tmp_buf;
    for (int i = 0; i < cache_list_.size(); i++)
    {
      Cache &c = cache_list_[i].second;
      if (c.IsValid() && c.GetTicket() <= border_ticket)
      {
        ChunkIndex index = cache_list_[i].first;

        // flush current cache
        if (c.IsWriteNeeded())
        {
          AsyncIoContext ctx;
          if (CacheSync(c, index, ctx) != Status::kOk)
          {
            fprintf(stderr, "VEFS: cache awrite failed\n");
            return Status::kIoError;
          }
          RegisterWaitingContext(ctx);
        }
        ApollIod(c);
        ns_wrapper_.Free(c.Release());
      }
    }
    return Status::kOk;
  }
  void RetrieveContexts()
  {
    while (true)
    {
      Spinlock lock(io_waiting_queue_lock_);
      if (io_waiting_queue_.empty())
      {
        return;
      }
      auto it = io_waiting_queue_.begin();
      Inode::AsyncIoContext ctx = *it;
      if (ns_wrapper_.ApollWithoutWait(ctx.iod) != 0)
      {
        break;
      }
      ctx = *it;
      io_waiting_queue_.pop_front();
      if (ctx.dma)
      {
        ns_wrapper_.Free(ctx.dma);
      }
    }
  }
  void WaitIoCompletion()
  {
    std::deque<Inode::AsyncIoContext> queue;
    {
      Spinlock lock(io_waiting_queue_lock_);
      queue = io_waiting_queue_;
      io_waiting_queue_.clear();
    }
    for (auto it = queue.begin(); it != queue.end(); ++it)
    {
      if (ns_wrapper_.Apoll((*it).iod))
      {
        printf("failed to unvme_write");
        abort();
      }
      if ((*it).dma)
      {
        ns_wrapper_.Free((*it).dma);
      }
    }
  }
  void CacheListSync()
  {
    for (auto it = cache_list_.begin(); it != cache_list_.end(); ++it)
    {
      ChunkIndex index = (*it).first;
      Cache &c = (*it).second;
      if (!c.IsValid())
      {
        continue;
      }
      if (c.IsWriteNeeded())
      {
        AsyncIoContext ctx;
        if (CacheSync(c, index, ctx) != Status::kOk)
        {
          printf("failed to sync cache");
          exit(1);
        }
        RegisterWaitingContext(ctx);
      }
    }
  }
  void SyncChunkList()
  {
    if (IsChunkListUpdated())
    {
      while (true)
      {
        vfio_dma_t *dma = ns_wrapper_.AllocChunk();
        if (!dma)
        {
          printf("allocation failure\n");
          exit(1);
        }
        uint64_t lba;
        bool needs_additional_write = ChunkListWrite(dma->buf, lba);
        unvme_iod_t iod = ns_wrapper_.Awrite(dma->buf, lba,
                                             kChunkSize / ns_wrapper_.GetBlockSize());
        if (!iod)
        {
          printf("failed to unvme_write");
          abort();
        }
        RegisterWaitingContext(AsyncIoContext{
            .iod = iod,
            .dma = dma,
            .time = ve_gettime(),
        });
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
    cl_->Dump();
  }
  bool IsChunkListUpdated()
  {
    return cl_->IsUpdated();
  }
  bool ChunkListWrite(void *buf, uint64_t &lba)
  {
    return cl_->Write(buf, lba);
  }
  void RegisterWaitingContext(AsyncIoContext ctx)
  {
    Spinlock lock(io_waiting_queue_lock_);
    io_waiting_queue_.push_back(ctx);
  }
  void Delete()
  {
    inode_updated_ = false;
    WaitIoCompletion();
    Truncate(0);
    chunkmap_.Release(Chunkmap::Index::CreateFromPos(cl_->GetLba() * ns_wrapper_.GetBlockSize()));
    uint64_t dummy_lba;
    uint64_t buf[kChunkSize / sizeof(uint64_t)];
    cl_->Write(reinterpret_cast<void *>(buf), dummy_lba);
    delete cl_;
    cl_ = nullptr;
  }
  std::string &GetFname()
  {
    return fname_;
  }

  std::atomic<int> &GetLock()
  {
    return lock_;
  }
  void ApollIod(Cache &c)
  {
    unvme_iod_t iod = c.ReleaseIod();
    if (iod != nullptr)
    {
      if (ns_wrapper_.Apoll(iod))
      {
        abort();
      }
    }
  }

private:
  Inode(std::string fname, ChunkList *cl, Chunkmap &chunkmap, UnvmeWrapper &ns_wrapper) : lock_(0), chunkmap_(chunkmap), ns_wrapper_(ns_wrapper), io_waiting_queue_lock_(0)
  {
    fname_ = fname;
    cl_ = cl;
    inode_updated_ = true;
  }
  Status CacheSync(Cache &cache, ChunkIndex index, Inode::AsyncIoContext &ctx)
  {
    ApollIod(cache);
    vfio_dma_t *dma = cache.MarkSynced(ns_wrapper_.AllocChunk());
    u64 lba;
    if (GetLba(index.GetPos(), lba) != Status::kOk)
    {
      return Status::kIoError;
    }
    unvme_iod_t iod = ns_wrapper_.Awrite(dma->buf, lba, kChunkSize / ns_wrapper_.GetBlockSize());
    if (!iod)
    {
      fprintf(stderr, "VEFS: cache awrite failed\n");
      return Status::kIoError;
    }
    ctx = Inode::AsyncIoContext{
        .iod = iod,
        .dma = dma,
        .time = ve_gettime(),
    };
    return Status::kOk;
  }

  std::atomic<int> lock_;

  std::string fname_;
  ChunkList *cl_;

  Chunkmap &chunkmap_;
  UnvmeWrapper &ns_wrapper_;
  std::deque<AsyncIoContext> io_waiting_queue_;
  std::atomic<int> io_waiting_queue_lock_;

  bool inode_updated_;

  std::vector<std::pair<ChunkIndex, Cache>> cache_list_;
  uint64_t cache_ticket_ = 0;
  static size_t AlignUp(size_t len)
  {
    return ((len + 3) / 4) * 4;
  }
};
