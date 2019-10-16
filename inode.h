#pragma once

#include <deque>
#include <string>
#include <vector>
#include "misc.h"
#include "chunkmap.h"

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
    u64 cc_lba = chunks_[ci] + kChunkSize / ns_->blocksize;
    ci++;
    for (; ci < chunks_.size(); ci++)
    {
      if (cc_lba != chunks_[ci])
      {
        break;
      }
      cc_lba += kChunkSize / ns_->blocksize;
    }
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
  Status RefreshCache(char *buf, u64 index)
  {
    if (!cache_.dma_)
    {
      cache_.dma_ = unvme_alloc(ns_, ns_->blocksize);
    }
    else if (cache_.index_ != index)
    {
      if (CacheWrite(true) != Status::kOk)
      {
        return Status::kIoError;
      }
    }
    cache_.index_ = index;
    memcpy(cache_.dma_->buf, buf, ns_->blocksize);
    cache_.needs_written_ = true;
    return Status::kOk;
  }
  Status CacheWrite(bool no_memcpy)
  {
    if (!cache_.dma_ || !cache_.needs_written_)
    {
      return Status::kOk;
    }
    unvme_iod_t iod = unvme_awrite(ns_, qid_, cache_.dma_->buf, cache_.index_, 1);
    if (!iod)
    {
      fprintf(stderr, "VEFS: cache awrite failed\n");
      return Status::kIoError;
    }
    cache_.needs_written_ = false;
    Inode::AsyncIoContext ctx = {
        .iod = iod,
        .dma = cache_.dma_,
        .time = ve_gettime(),
    };
    vfio_dma_t *ndma = unvme_alloc(ns_, ns_->blocksize);
    if (!no_memcpy)
    {
      memcpy(ndma->buf, cache_.dma_->buf, ns_->blocksize);
    }
    cache_.dma_ = ndma;
    RegisterWaitingContext(ctx);
    return Status::kOk;
  }
  bool ReuseCache(char *buf, u64 lba)
  {
    if (cache_.dma_ && cache_.index_ == lba)
    {
      memcpy(buf, cache_.dma_->buf, blocksize_);
      cache_.needs_written_ = false;
      return true;
    }
    return false;
  }
  void ApplyCache(void *buf, u64 lba, u32 nlb)
  {
    if (!cache_.dma_)
    {
      return;
    }
    if (lba <= cache_.index_ && cache_.index_ < lba + nlb)
    {
      memcpy(reinterpret_cast<char *>(buf) + (cache_.index_ - lba) * blocksize_, cache_.dma_->buf, blocksize_);
    }
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
      unvme_free(ns_, (*itr).dma);
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
      unvme_free(ns_, (*itr).dma);
      io_waiting_queue_.pop_front();
    }
  }
  bool Sync()
  {
    if (CacheWrite(false) != Status::kOk)
    {
      printf("failed to CacheWrite");
      exit(1);
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
  /*  Status Write(const void *data, size_t offset, size_t len)
  {
    if (!append_cache_->Appendable(offset))
    {
      // TODO: support random write
      return Status::kIoError;
    }
    while (true)
    {
      size_t wsize = append_cache_->Write(data, offset, wsize);
      offset += wsize;
      len -= wsize;
      if (len == 0)
      {
        return Status::kOk;
      }
      // WIP: Flush();
    }
  }*/

private:
  // TODO: qid would not work on multi thread
  Inode(std::string fname, std::vector<u64> &chunks, int lock, size_t len, Chunkmap &chunkmap, const unvme_ns_t *ns, size_t blocksize, int qid) : chunkmap_(chunkmap), ns_(ns) /*, append_cache_(ns)*/, qid_(qid), cache_(ns_)
  {
    fname_ = fname;
    chunks_ = chunks;
    lock_ = lock;
    len_ = len;
    blocksize_ = blocksize;
    inode_updated_ = true;
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

  struct Cache
  {
    u64 index_;
    vfio_dma_t *dma_;
    bool needs_written_;
    const unvme_ns_t *ns_;
    Cache() = delete;
    Cache(const unvme_ns_t *ns) : ns_(ns)
    {
      dma_ = nullptr;
      needs_written_ = false;
    }
    ~Cache()
    {
      assert(!needs_written_);
      if (dma_)
      {
        unvme_free(ns_, dma_);
      }
    }
  };

  Cache cache_;
  static size_t AlignUp(size_t len)
  {
    return ((len + 3) / 4) * 4;
  }

  /*  struct AppendCache
  {
    void *data_[8];
    size_t offset_;
    size_t size_;
    size_t kCacheSize = 4096;
    AppendCache(const unvme_ns_t *ns) : ns_(ns)
    {
      // WIP: read it before
      for (int i = 0; i < 8; i++)
      {
        data_[i] = unvme_alloc(ns_, kCacheSize);
      }
      size_ = 0;
      offset_ = 0;
    }
    ~AppendCache()
    {
      for (int i = 0; i < 8; i++)
      {
        unvme_free(ns_, data_[i]);
      }
    }
    bool Appendable(size_t offset)
    {
      return (size_ == 0) || ((offset_ <= offset) && (offset <= offset_ + size_));
    }
    size_t Write(const void *data, size_t offset, size_t len)
    {
      assert(Appendable(offset));

      assert(len == GetWritableSizeWithoutFlush(offset, len));
      memcpy(data_ + (offset - offset_), data, len);
    }
    const unvme_ns_t *ns_;
  };
  AppendCache append_cache_;*/
  struct Block
  {
    void *buf;
  };
};
