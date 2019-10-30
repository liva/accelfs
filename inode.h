#pragma once

#include <deque>
#include <string>
#include <list>
#include <unordered_map>
#include <vector>
#include <utility>
#include <deque>
#include <atomic>
#include "misc.h"
#include "chunkmap.h"
#include "cache.h"

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
    std::vector<u64> chunks;
    chunks.push_back(cindex.GetPos() / ns_wrapper.GetBlockSize());
    return new Inode(fname, chunks, 0, chunkmap, ns_wrapper);
  }
  static Inode *CreateFromBuffer(HeaderBuffer &buf, Chunkmap &chunkmap, UnvmeWrapper &ns_wrapper)
  {
    auto fname = buf.GetString();
    size_t chunk_num = buf.Get<__typeof__(chunk_num)>();
    __typeof__(Inode::chunks_) chunks;
    for (size_t i = 0; i < chunk_num; i++)
    {
      auto chunk = buf.Get<__typeof__(Inode::chunks_[0])>();
      chunks.push_back(chunk);
    }
    auto len = buf.Get<__typeof__(Inode::len_)>();
    Inode *inode = new Inode(fname, chunks, len, chunkmap, ns_wrapper);
    inode->inode_updated_ = false;
    return inode;
  }
  void Release()
  {
    for (auto it = cache_list_.begin(); it != cache_list_.end(); ++it)
    {
      Cache *cache = (*it).second;
      assert(!cache->IsWriteNeeded());
      ns_wrapper_.Free(cache->Release());
      delete cache;
    }
    cache_list_.clear();
  }
  ~Inode()
  {
    assert(!inode_updated_);
    assert(io_waiting_queue_.empty());
  }
  Status Truncate(size_t len)
  {
    assert(len_ / kChunkSize + 1 == chunks_.size());
    size_t new_chunknum = len / kChunkSize + 1;
    size_t old_chunknum = chunks_.size();
    if (new_chunknum < old_chunknum)
    {
      // shrinking
      for (size_t i = old_chunknum - 1; i != new_chunknum - 1; i--)
      {
        u64 released_chunk = chunks_.back();
        chunks_.pop_back();
        Chunkmap::Index cindex = Chunkmap::Index::CreateFromPos(released_chunk * ns_wrapper_.GetBlockSize());
        chunkmap_.Release(cindex);
      }
    }
    else if (old_chunknum < new_chunknum)
    {
      // expanding
      for (size_t i = old_chunknum; i != new_chunknum; i++)
      {
        Chunkmap::Index cindex = chunkmap_.FindUnused();
        if (cindex.IsNull())
        {
          return Status::kIoError;
        }
        chunks_.push_back(cindex.GetPos() / ns_wrapper_.GetBlockSize());
      }
    }
    len_ = len;
    assert(len_ / kChunkSize + 1 == chunks_.size());
    inode_updated_ = true;
    return Status::kOk;
  }
  size_t GetLen()
  {
    return len_;
  }
  std::vector<u64> &GetChunkList()
  {
    return chunks_;
  }
  bool CheckOffset(size_t offset)
  {
    int chunk_index = offset / kChunkSize;
    return (offset <= len_) && (chunk_index < chunks_.size());
  }
  u64 GetLba(size_t offset)
  {
    assert(CheckOffset(offset));
    int chunk_index = offset / kChunkSize;
    return chunks_[chunk_index] + (u64)((offset % kChunkSize) / ns_wrapper_.GetBlockSize());
  }
  size_t GetNextChunkBoundary(size_t offset)
  {
    assert(CheckOffset(offset));
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
    size_t chunk_num = chunks_.size();

    buf.AppendRaw(fname_.c_str(), fname_.length());
    buf.Append('\0');
    buf.AlignPos();

    buf.Append(chunk_num);

    buf.AppendRaw(chunks_.data(), chunk_num * sizeof(__typeof__(chunks_[0])));

    buf.Append(len_);

    inode_updated_ = false;
  }
  Cache *FindFromCacheList(ChunkIndex cindex)
  {
    auto it = cache_list_.begin();
    for (; it != cache_list_.end(); ++it)
    {
      if ((*it).first == cindex)
      {
        break;
      }
    }
    if (it != cache_list_.end())
    {
      Cache *cache = (*it).second;
      cache->SetTicket(cache_ticket_);
      cache_ticket_++;
      return cache;
    }
    return nullptr;
  }
  Status PrepareCache(ChunkIndex cindex, bool createnew_ifmissing, std::deque<unvme_iod_t> &queue)
  {
    Cache *c = FindFromCacheList(cindex);
    if (c)
    {
      return Status::kOk;
    }
    // could not find
    // create one
    vfio_dma_t *dma = ns_wrapper_.AllocChunk();
    if (!dma)
    {
      printf("allocation failure\n");
      exit(1);
    }
    if (!createnew_ifmissing)
    {
      u64 lba = GetLba(cindex.GetPos());
      u32 bnum = kChunkSize / ns_wrapper_.GetBlockSize();

      unvme_iod_t iod = ns_wrapper_.Aread(dma->buf, lba, bnum);
      if (!iod)
      {
        fprintf(stderr, "cr^ %ld %d\n", lba, bnum);
        fprintf(stderr, "VEFS: read failed\n");
        return Status::kIoError;
      }
      queue.push_back(iod);
    }
    cache_list_.push_back(std::make_pair(cindex, new Cache(dma)));

    return Status::kOk;
  }
  Status ShrinkCacheListIfNeeded()
  {
    if (cache_list_.size() < 16)
    {
      return Status::kOk;
    }
    uint64_t border_ticket = cache_ticket_ - 8;
    for (auto it = cache_list_.begin(); it != cache_list_.end();)
    {
      Cache *c = (*it).second;
      if (c->GetTicket() <= border_ticket)
      {
        ChunkIndex index = (*it).first;
        it = cache_list_.erase(it);

        // flush current cache
        if (c->IsWriteNeeded())
        {
          AsyncIoContext ctx;
          if (CacheSync(c, index, ctx) != Status::kOk)
          {
            fprintf(stderr, "VEFS: cache awrite failed\n");
            return Status::kIoError;
          }
          RegisterWaitingContext(ctx);
        }
        ns_wrapper_.Free(c->Release());
        delete c;

        continue;
      }
      ++it;
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
  bool Sync()
  {
    for (auto it = cache_list_.begin(); it != cache_list_.end(); ++it)
    {
      ChunkIndex index = (*it).first;
      Cache *c = (*it).second;
      if (c->IsWriteNeeded())
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
    WaitIoCompletion();
    return inode_updated_;
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
    for (auto it = chunks_.begin(); it != chunks_.end(); ++it)
    {
      chunkmap_.Release(Chunkmap::Index::CreateFromPos(*it * ns_wrapper_.GetBlockSize()));
    }
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
  Inode(std::string fname, std::vector<u64> &chunks, size_t len, Chunkmap &chunkmap, UnvmeWrapper &ns_wrapper) : lock_(0), chunkmap_(chunkmap), ns_wrapper_(ns_wrapper), io_waiting_queue_lock_(0)
  {
    fname_ = fname;
    chunks_ = chunks;
    len_ = len;
    inode_updated_ = true;
  }
  Status CacheSync(Cache *cache, ChunkIndex index, Inode::AsyncIoContext &ctx)
  {
    vfio_dma_t *dma = cache->MarkSynced(ns_wrapper_.AllocChunk());
    unvme_iod_t iod = ns_wrapper_.Awrite(dma->buf, GetLba(index.GetPos()), kChunkSize / ns_wrapper_.GetBlockSize());
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
  std::vector<u64> chunks_;
  size_t len_;

  Chunkmap &chunkmap_;
  UnvmeWrapper &ns_wrapper_;
  std::deque<AsyncIoContext> io_waiting_queue_;
  std::atomic<int> io_waiting_queue_lock_;

  bool inode_updated_;

  std::vector<std::pair<ChunkIndex, Cache *>> cache_list_;
  uint64_t cache_ticket_ = 0;
  static size_t AlignUp(size_t len)
  {
    return ((len + 3) / 4) * 4;
  }
};
