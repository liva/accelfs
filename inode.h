#pragma once

#include <deque>
#include <string>
#include <vector>
#include <utility>
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
  static Inode *CreateEmpty(std::string fname, int lock, Chunkmap &chunkmap, const unvme_ns_t *ns, size_t blocksize, int qid)
  {
    Chunkmap::Index cindex = chunkmap.FindUnused();
    if (cindex.IsNull())
    {
      fprintf(stderr, "no space on the stroage");
      return nullptr;
    }
    std::vector<u64> chunks;
    chunks.push_back(cindex.GetPos() / ns->blocksize);
    return new Inode(fname, chunks, lock, 0, chunkmap, ns, blocksize, qid);
  }
  static Inode *CreateFromBuffer(const char *buf, Chunkmap &chunkmap, const unvme_ns_t *ns, size_t blocksize, int &pos, int qid)
  {
    pos = 0;
    auto fname = std::string(buf + pos);
    pos += AlignUp(fname.length() + 1);
    size_t chunk_num = *((__typeof__(chunk_num) *)(buf + pos));
    pos += sizeof(chunk_num);
    __typeof__(Inode::chunks_) chunks;
    for (size_t i = 0; i < chunk_num; i++)
    {
      chunks.push_back(*((__typeof__(Inode::chunks_[0]) *)(buf + pos)));
      pos += sizeof(Inode::chunks_[0]);
    }
    __typeof__(Inode::lock_) lock = *((__typeof__(Inode::lock_) *)(buf + pos));
    pos += sizeof(Inode::lock_);
    __typeof__(Inode::len_) len = *((__typeof__(Inode::len_) *)(buf + pos));
    pos += sizeof(Inode::len_);
    Inode *inode = new Inode(fname, chunks, lock, len, chunkmap, ns, blocksize, qid);
    inode->inode_updated_ = false;
    return inode;
  }
  void Release()
  {
    if (wcache_)
    {
      unvme_free(ns_, wcache_->Release());
    }
    delete wcache_;
    wcache_ = nullptr;
    if (rcache_)
    {
      unvme_free(ns_, rcache_->Release());
    }
    delete rcache_;
    rcache_ = nullptr;
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
        Chunkmap::Index cindex = Chunkmap::Index::CreateFromPos(released_chunk * ns_->blocksize);
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
        chunks_.push_back(cindex.GetPos() / ns_->blocksize);
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
    return chunks_[chunk_index] + (u64)((offset % kChunkSize) / blocksize_);
  }
  size_t GetNextChunkBoundary(size_t offset)
  {
    assert(CheckOffset(offset));
    int ci = offset / kChunkSize;
    //u64 cc_lba = chunks_[ci] + kChunkSize / ns_->blocksize;
    ci++;
    /*for (; ci < chunks_.size(); ci++)
    {
      if (cc_lba != chunks_[ci])
      {
        break;
      }
      cc_lba += kChunkSize / ns_->blocksize;
    }*/
    return ci * kChunkSize;
  }
  void Rename(const std::string &fname)
  {
    fname_ = fname;
    inode_updated_ = true;
  }
  size_t HeaderWrite(char *buf, size_t pos_max)
  {
    size_t pos = 0;
    size_t chunk_num = chunks_.size();

    if (AlignUp(fname_.length() + 1) + sizeof(chunk_num) + sizeof(chunks_[0]) * chunk_num + sizeof(lock_) + sizeof(len_) > pos_max)
    {
      fprintf(stderr, "header overflowed\n");
      exit(1);
    }
    memcpy(buf, fname_.c_str(), fname_.length());

    buf[fname_.length()] = '\0';
    pos += AlignUp(fname_.length() + 1);

    memcpy(buf + pos, &chunk_num, sizeof(chunk_num));
    pos += sizeof(chunk_num);

    for (size_t i = 0; i < chunk_num; i++)
    {
      memcpy(buf + pos, &chunks_[i], sizeof(chunks_[i]));
      pos += sizeof(chunks_[i]);
    }

    memcpy(buf + pos, &lock_, sizeof(lock_));
    pos += sizeof(lock_);

    memcpy(buf + pos, &len_, sizeof(len_));
    pos += sizeof(len_);

    inode_updated_ = false;
    return pos;
  }
  Status CreateReadCache(vfio_dma_t *dma, u64 index)
  {
    // WIP
    // assert(rcache_->GetIndex() != index);
    if (!rcache_)
    {
      rcache_ = new Cache(index, dma, kChunkSize, false);
    }
    else
    {
      unvme_free(ns_, dma);
    }
    return Status::kOk;
  }
  Status CreateWriteCache(char *buf, u64 index)
  {
    if (wcache_ && wcache_->GetIndex() != index)
    {
      if (wcache_->IsWriteNeeded())
      {
        Inode::AsyncIoContext ctx;
        if (WriteCacheSync(ctx) != Inode::Status::kOk)
        {
          fprintf(stderr, "VEFS: cache awrite failed\n");
          return Status::kIoError;
        }
        assert(ctx.dma == nullptr);
        ctx.dma = wcache_->Release();
        RegisterWaitingContext(ctx);
      }
      else
      {
        unvme_free(ns_, wcache_->Release());
      }
      delete wcache_;
      wcache_ = nullptr;
    }
    if (!wcache_)
    {
      wcache_ = new Cache(index, unvme_alloc(ns_, blocksize_), blocksize_, true);
    }
    wcache_->Refresh(buf);
    return Status::kOk;
  }
  void ApplyWriteCache(void *buf, u64 lba, u32 nlb)
  {
    if (wcache_)
    {
      wcache_->Apply(buf, lba, nlb);
    }
  }
  bool ReuseWriteCache(char *buf, u64 lba)
  {
    if (wcache_)
    {
      if (wcache_->Apply(buf, lba, 1))
      {
        wcache_->MarkSynced();
        return true;
      }
    }
    return false;
  }
  Cache *GetWriteCache()
  {
    return wcache_;
  }
  void RetrieveContexts()
  {
    uint64_t ctime = ve_gettime();
    while (!io_waiting_queue_.empty())
    {
      auto itr = io_waiting_queue_.begin();
      if (ctime < (*itr).time + 2 * 1000 * 1000)
      {
        break;
      }
      if (unvme_apoll((*itr).iod, UNVME_TIMEOUT))
      {
        printf("failed to unvme_write");
        exit(1);
      }
      if ((*itr).dma)
      {
        unvme_free(ns_, (*itr).dma);
      }
      io_waiting_queue_.pop_front();
    }
  }
  void WaitIoCompletion()
  {
    while (!io_waiting_queue_.empty())
    {
      auto itr = io_waiting_queue_.begin();
      if (unvme_apoll((*itr).iod, UNVME_TIMEOUT))
      {
        printf("failed to unvme_write");
        exit(1);
      }
      if ((*itr).dma)
      {
        unvme_free(ns_, (*itr).dma);
      }
      io_waiting_queue_.pop_front();
    }
  }
  bool Sync()
  {
    if (wcache_ && wcache_->IsWriteNeeded())
    {
      AsyncIoContext ctx;
      if (WriteCacheSync(ctx) != Status::kOk)
      {
        printf("failed to sync cache");
        exit(1);
      }
      RegisterWaitingContext(ctx);
    }
    WaitIoCompletion();
    return inode_updated_;
  }
  void RegisterWaitingContext(AsyncIoContext ctx)
  {
    io_waiting_queue_.push_back(ctx);
  }
  void Delete()
  {
    inode_updated_ = false;
    WaitIoCompletion();
    for (auto it = chunks_.begin(); it != chunks_.end(); ++it)
    {
      chunkmap_.Release(Chunkmap::Index::CreateFromPos(*it * ns_->blocksize));
    }
  }
  std::string &GetFname()
  {
    return fname_;
  }

private:
  // TODO: qid would not work on multi thread
  Inode(std::string fname, std::vector<u64> &chunks, int lock, size_t len, Chunkmap &chunkmap, const unvme_ns_t *ns, size_t blocksize, int qid) : chunkmap_(chunkmap), ns_(ns) /*, append_cache_(ns)*/, qid_(qid)
  {
    fname_ = fname;
    chunks_ = chunks;
    lock_ = lock;
    len_ = len;
    blocksize_ = blocksize;
    inode_updated_ = true;
  }

  Status WriteCacheSync(Inode::AsyncIoContext &ctx)
  {
    std::pair<void *, u64> sinfo = wcache_->MarkSynced();
    unvme_iod_t iod = unvme_awrite(ns_, qid_, sinfo.first, sinfo.second, 1);
    if (!iod)
    {
      fprintf(stderr, "VEFS: cache awrite failed\n");
      return Status::kIoError;
    }
    ctx = Inode::AsyncIoContext{
        .iod = iod,
        .dma = nullptr,
        .time = ve_gettime(),
    };
    return Status::kOk;
  }

  std::string fname_;
  std::vector<u64> chunks_;
  int lock_;
  size_t len_;

  Chunkmap &chunkmap_;
  const unvme_ns_t *ns_;
  size_t blocksize_;
  std::deque<AsyncIoContext> io_waiting_queue_;
  const int qid_;

  bool inode_updated_;

  Cache *wcache_ = nullptr;
  Cache *rcache_ = nullptr;
  static size_t AlignUp(size_t len)
  {
    return ((len + 3) / 4) * 4;
  }
};
