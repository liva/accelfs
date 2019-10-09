#pragma once

#include <unvme.h>
#include <unvme_nvme.h>
#include <string>
#include <list>
#include <memory>
#include <atomic>
#include <deque>
#include <vepci.h>
#include <vector>
#include <string.h>

#include <stdint.h>
extern uint64_t tmp_var;
static inline uint64_t ve_gettime()
{
  uint64_t ret;
  void *vehva = ((void *)0x000000001000);
  asm volatile("lhm.l %0,0(%1)"
               : "=r"(ret)
               : "r"(vehva));
  return ((uint64_t)1000 * ret) / 800;
}

//#define nvme_printf(...) printf(__VA_ARGS__)
#define nvme_printf(...)

static const size_t kFileSizeMax = 2L * 1024 * 1024 /* * 1024*/;

class Vefs;

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
  // TODO: qid would not work on multi thread
  Inode(std::string fname, u32 index, int lock, size_t len, int deleted, const unvme_ns_t *ns, size_t blocksize, int qid) : ns_(ns) /*, append_cache_(ns)*/, qid_(qid), cache_(ns_)
  {
    fname_ = fname;
    index_ = index;
    lock_ = lock;
    len_ = len;
    deleted_ = deleted;
    blocksize_ = blocksize;
    inode_updated_ = true;
  }
  Inode(const Inode &) = delete;
  Inode &operator=(const Inode &) = delete;
  Inode(Inode &&) = delete;
  Inode() = delete;
  Inode &operator=(Inode &&) = delete;
  void Recreate(std::string fname, int lock)
  {
    assert(IsDeleted());
    fname_ = fname;
    lock_ = lock;
    len_ = 0;
    deleted_ = 0;
    inode_updated_ = true;
  }
  static Inode *CreateFromBuffer(const char *buf, const unvme_ns_t *ns, size_t blocksize, int &pos, int qid)
  {
    pos = 0;
    auto fname = std::string(buf + pos);
    pos += AlignUp(fname.length() + 1);
    __typeof__(Inode::index_) index = *((__typeof__(Inode::index_) *)(buf + pos));
    pos += sizeof(Inode::index_);
    __typeof__(Inode::lock_) lock = *((__typeof__(Inode::lock_) *)(buf + pos));
    pos += sizeof(Inode::lock_);
    __typeof__(Inode::len_) len = *((__typeof__(Inode::len_) *)(buf + pos));
    pos += sizeof(Inode::len_);
    __typeof__(Inode::deleted_) deleted = *((__typeof__(Inode::deleted_) *)(buf + pos));
    pos += sizeof(Inode::deleted_);
    Inode *inode = new Inode(fname, index, lock, len, deleted, ns, blocksize, qid);
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
    if (len >= kFileSizeMax)
    {
      fprintf(stderr, "VEFS: file size erorr\n");
      return Status::kIoError;
    }
    len_ = len;
    inode_updated_ = true;
    return Status::kOk;
  }
  u32 GetIndex()
  {
    return index_;
  }
  size_t GetLen()
  {
    return len_;
  }
  u32 GetLba(size_t offset)
  {
    return index_ + (u32)(offset / blocksize_);
  }
  void Rename(const std::string &fname)
  {
    assert(!IsDeleted());
    fname_ = fname;
    inode_updated_ = true;
  }
  size_t HeaderWrite(char *buf, size_t pos_max)
  {
    size_t pos = 0;

    if (AlignUp(fname_.length() + 1) + sizeof(index_) + sizeof(lock_) + sizeof(len_) + sizeof(deleted_) > pos_max)
    {
      fprintf(stderr, "header overflowed\n");
      exit(1);
    }
    memcpy(buf, fname_.c_str(), fname_.length());

    buf[fname_.length()] = '\0';
    pos += AlignUp(fname_.length() + 1);

    memcpy(buf + pos, &index_, sizeof(index_));
    pos += sizeof(index_);

    memcpy(buf + pos, &lock_, sizeof(lock_));
    pos += sizeof(lock_);

    memcpy(buf + pos, &len_, sizeof(len_));
    pos += sizeof(len_);

    memcpy(buf + pos, &deleted_, sizeof(deleted_));
    pos += sizeof(deleted_);

    inode_updated_ = false;
    return pos;
  }
  Status RefreshCache(char *buf, u32 index)
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
    cache_.needs_written = true;
    return Status::kOk;
  }
  Status CacheWrite(bool no_memcpy)
  {
    if (!cache_.dma_ || !cache_.needs_written)
    {
      return Status::kOk;
    }
    unvme_iod_t iod = unvme_awrite(ns_, qid_, cache_.dma_->buf, cache_.index_, 1);
    if (!iod)
    {
      fprintf(stderr, "VEFS: cache awrite failed\n");
      return Status::kIoError;
    }
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
  bool ReuseCache(char *buf, u32 lba)
  {
    if (cache_.dma_ && cache_.index_ == lba)
    {
      memcpy(buf, cache_.dma_->buf, blocksize_);
      cache_.needs_written = false;
      return true;
    }
    return false;
  }
  void ApplyCache(void *buf, u32 lba, u32 nlb)
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
  bool IsDeleted()
  {
    return deleted_ == 1;
  }
  void Delete()
  {
    assert(!IsDeleted());
    deleted_ = 1;
    inode_updated_ = true;
  }
  std::string &GetFname()
  {
    assert(!IsDeleted());
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
  std::string fname_;
  u32 index_;
  int lock_;
  size_t len_;
  int deleted_;

  const unvme_ns_t *ns_;
  size_t blocksize_;
  std::deque<AsyncIoContext> io_waiting_queue_;
  const int qid_;

  bool inode_updated_;

  struct Cache
  {
    u32 index_;
    vfio_dma_t *dma_;
    bool needs_written;
    const unvme_ns_t *ns_;
    Cache() = delete;
    Cache(const unvme_ns_t *ns) : ns_(ns)
    {
      dma_ = nullptr;
      needs_written = false;
    }
    ~Cache()
    {
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

class Vefs
{
public:
  enum class Status
  {
    kOk,
    kIoError,
    kTryAgain,
  };
  Vefs()
      : ns_(unvme_open("b3:00.0")),
        qcnt_(0),
        header_lock_(0)
  {
    if (kHeaderBlockSize % ns_->blocksize != 0)
    {
      fprintf(stderr, "unsupported blocksize\n");
      exit(1);
    }
    printf("%s qc=%d/%d qs=%d/%d bc=%#lx bs=%d maxbpio=%d\n", ns_->device, ns_->qcount,
           ns_->maxqcount, ns_->qsize, ns_->maxqsize, ns_->blockcount,
           ns_->blocksize, ns_->maxbpio);

    if (!HeaderRead())
    {
      HeaderWriteSync();
    }
  }
  ~Vefs()
  {
    HeaderWriteSync();
    for (auto itr = inodes_.begin(); itr != inodes_.end(); ++itr)
    {
      Inode *inode = *itr;
      if (inode != nullptr)
      {
        assert(!inode->Sync());
        delete inode;
      }
    }
    //HardWrite();
    unvme_close(ns_);
  }
  static Vefs *Get()
  {
    if (!vefs_)
    {
      vefs_.reset(new Vefs);
    }
    return vefs_.get();
  }
  const unvme_ns_t *GetNs()
  {
    return ns_;
  }

  Status Truncate(Inode *inode, size_t size)
  {
    if (inode->Truncate(size) != Inode::Status::kOk)
    {
      return Status::kIoError;
    }
    return Status::kOk;
  }

  size_t GetLen(Inode *inode)
  {
    return inode->GetLen();
  }

  void Sync(Inode *inode)
  {
    if (inode->Sync())
    {
      HeaderLock();
      HeaderWriteSync();
      HeaderUnlock();
    }
    HardWrite();
  }

  void HardWrite()
  {
    vfio_dma_t *dma = unvme_alloc(ns_, ns_->blocksize); // dummy
    u32 cdw10_15[6];                                    // dummy
    int stat = unvme_cmd(ns_, GetQnum(), NVME_CMD_FLUSH, ns_->id, dma->buf, 512, cdw10_15, 0);
    if (stat)
    {
      printf("failed to sync");
    }
    unvme_free(ns_, dma);
  }

  Status Append(Inode *inode, const void *data, size_t size)
  {
    /*    size_t oldsize = inode->GetLen();
    size_t end = offset + size;
    if (end > oldsize)
    {
      if (inode->Truncate(end) != Inode::Status::kOk)
      {
        return Status::kIoError;
      }
    }

    if (inode->Write(data, oldsize, size) == Inode::Status::kOk)
    {
      return Status::kOk;
    }
    else
    {
      return Status::kIoError;
    }*/
    return Write(inode, inode->GetLen(), data, size);
  }

  Status Write(Inode *inode, size_t offset, const void *data, size_t size)
  {
    /*
    printf("\nw[%s %lu %lu]\n", inode->GetFname().c_str(), offset, size);
    for (size_t i = 0; i < size; i++)
    {
      printf("%02x", ((char *)data)[i]);
    }
    printf("\n");
    */
    inode->RetrieveContexts();
    nvme_printf("w> %ld %ld\n", offset, size);
    size_t oldsize = inode->GetLen();
    size_t end = offset + size;
    if (end > oldsize)
    {
      if (inode->Truncate(end) != Inode::Status::kOk)
      {
        return Status::kIoError;
      }
    }

    size_t aligned_offset = Align(offset);
    size_t ndsize = AlignUp(end - aligned_offset);

    u32 lba = inode->GetLba(offset);
    u32 nlb = (u32)(ndsize / ns_->blocksize);
    if (lba >= ns_->blockcount)
    {
      fprintf(stderr, "VEFS: tried to access beyond the block count\n");
      return Status::kIoError;
    }

    nvme_printf("w^ %d (=%ld/bs) %d (=%ld/bs)\n", lba, offset, nlb, size);
    vfio_dma_t *dma = unvme_alloc(ns_, ndsize);
    if (!dma)
    {
      fprintf(stderr, "VEFS: buffer allocation error\n");
      return Status::kTryAgain;
    }

    // pre-read from storage
    do
    {
      if (oldsize == 0)
      {
        break;
      }
      std::vector<size_t> preload_offset;
      size_t offset_list[2] = {offset, end};
      for (size_t toffset : offset_list)
      {
        if ((toffset % ns_->blocksize) == 0)
        {
          // aligned
          continue;
        }
        size_t aligned_toffset = Align(toffset);
        if (Align(oldsize - 1) < aligned_toffset)
        {
          // data is not written in this block
          continue;
        }
        preload_offset.push_back(toffset);
      }
      if (preload_offset.size() == 2 && inode->GetLba(preload_offset[0]) == inode->GetLba(preload_offset[1]))
      {
        preload_offset.pop_back();
      }

      std::vector<unvme_iod_t> preload_iod;
      for (size_t toffset : preload_offset)
      {
        char *tbuf = reinterpret_cast<char *>(dma->buf) + Align(toffset) - aligned_offset;
        if (inode->ReuseCache(tbuf, inode->GetLba(toffset)))
        {
          continue;
        }
        unvme_iod_t iod = unvme_aread(ns_, GetQnum(), tbuf, inode->GetLba(toffset), 1);
        if (!iod)
        {
          fprintf(stderr, "VEFS: aread failed\n");
          return Status::kIoError;
        }
        preload_iod.push_back(iod);
      }
      for (unvme_iod_t iod : preload_iod)
      {
        if (unvme_apoll(iod, UNVME_TIMEOUT))
        {
          fprintf(stderr, "VEFS: aread failed\n");
          return Status::kIoError;
        }
      }
    } while (0);

    memcpy((u8 *)dma->buf + offset - aligned_offset, data, size);

    if (inode->GetLba(inode->GetLen()) == lba + nlb - 1)
    {
      if (inode->RefreshCache(reinterpret_cast<char *>(dma->buf) + ndsize - ns_->blocksize, lba + nlb - 1) != Inode::Status::kOk)
      {
        fprintf(stderr, "VEFS: cache awrite failed\n");
        return Status::kIoError;
      }
      nlb--;
    }

    if (nlb != 0)
    {
      unvme_iod_t iod = unvme_awrite(ns_, GetQnum(), dma->buf, lba, nlb);
      if (!iod)
      {
        fprintf(stderr, "VEFS: awrite failed\n");
        return Status::kIoError;
      }
      Inode::AsyncIoContext ctx = {
          .iod = iod,
          .dma = dma,
          .time = ve_gettime(),
      };
      inode->RegisterWaitingContext(ctx);
    }

    return Status::kOk;
  }

  Status
  Read(Inode *inode, uint64_t offset, size_t n, char *scratch)
  {
    //printf("\nr[%s %lu %lu]\n", inode->GetFname().c_str(), offset, n);
    inode->RetrieveContexts();
    size_t noffset = Align(offset);
    size_t ndsize = AlignUp(n + offset - noffset);

    u32 lba = inode->GetLba(noffset);
    u32 nlb = (u32)(ndsize / ns_->blocksize);
    if (lba >= ns_->blockcount)
    {
      nvme_printf("tried to access %d\n", lba);
      return Status::kIoError;
    }

    nvme_printf("r^ %d (=%ld/bs) %d (=%ld/bs)\n", lba, offset, nlb, ndsize);
    vfio_dma_t *dma = unvme_alloc(ns_, ndsize);
    if (!dma)
    {
      nvme_printf("allocation failure\n");
      return Status::kTryAgain;
    }
    if (ReadWithIoBuf(inode, noffset, ndsize, (char *)dma->buf) != Status::kOk)
    {
      return Status::kIoError;
    }
    memcpy(scratch, (u8 *)dma->buf + offset - noffset, n);
    unvme_free(ns_, dma);
    /*for (size_t i = 0; i < n; i++)
    {
      printf("%02x", scratch[i]);
    }
    printf("\n");*/

    return Status::kOk;
  }

  Status ReadWithIoBuf(Inode *inode, uint64_t offset, size_t n, char *scratch)
  {
    if (offset + n >= kFileSizeMax)
    {
      nvme_printf("IO Error\n");
      return Status::kIoError;
    }

    size_t noffset = Align(offset);
    if (noffset != offset)
    {
      return Status::kIoError;
    }
    size_t ndsize = AlignUp(n + offset - noffset);
    if (ndsize != n)
    {
      return Status::kIoError;
    }

    u32 lba = inode->GetLba(noffset);
    u32 nlb = (u32)(ndsize / ns_->blocksize);
    if (lba >= ns_->blockcount)
    {
      nvme_printf("tried to access %d\n", lba);
      return Status::kIoError;
    }

    nvme_printf("r^ %d (=%ld/bs) %d (=%ld/bs)\n", lba, offset, nlb, ndsize);
    unvme_read(ns_, GetQnum(), scratch, lba, nlb);

    inode->ApplyCache(scratch, lba, nlb);
    return Status::kOk;
  }

  u16 GetBlockSize() { return ns_->blocksize; }

  Inode *GetInode(const std::string &fname)
  {
    HeaderLock();
    for (auto itr = inodes_.begin(); itr != inodes_.end(); ++itr)
    {
      Inode *inode = *itr;
      if (inode == nullptr)
      {
        break;
      }
      if (inode->IsDeleted())
      {
        continue;
      }
      if (inode->GetFname() == fname)
      {
        HeaderUnlock();
        return inode;
      }
    }
    HeaderUnlock();
    return nullptr;
  }

  void Dump()
  {
    HeaderLock();
    // printf("Dump>> %p %s %d %lu %d\n", &(*it), (*it).fname.c_str(),
    // (*it).index, (*it).len, (*it).lock);
    HeaderUnlock();
  }

  void Delete(Inode *inode)
  {
    // printf("Vefs::Delete %s\n", inode->fname.c_str());
    inode->Delete();
  }

  void Rename(Inode *inode, const std::string &fname)
  {
    if (DoesExist(fname))
    {
      Create(fname, false)->Delete();
    }
    inode->Rename(fname);
  }

  bool DoesExist(const std::string &fname)
  {
    bool flag = false;
    HeaderLock();

    for (auto itr = inodes_.begin(); itr != inodes_.end(); ++itr)
    {
      Inode *rd = *itr;
      if (rd == nullptr)
      {
        break;
      }
      if (!rd->IsDeleted() && rd->GetFname() == fname)
      {
        flag = true;
        break;
      }
    }

    HeaderUnlock();
    return flag;
  }
  Inode *Create(const std::string &fname, bool lock)
  {
    Inode *rd = nullptr;
    Inode *deleted_rd = nullptr;
    u32 maxindex = 0;

    HeaderLock();

    for (auto itr = inodes_.begin(); itr != inodes_.end(); ++itr)
    {
      rd = *itr;
      if (maxindex < rd->GetIndex())
      {
        maxindex = rd->GetIndex();
      }
      if (rd->IsDeleted())
      {
        if (deleted_rd == nullptr)
        {
          deleted_rd = rd;
        }
      }
      else if (rd->GetFname() == fname)
      {
        break;
      }
      rd = nullptr;
    }

    if (rd == nullptr)
    {
      if (deleted_rd != nullptr)
      {
        rd = deleted_rd;
        deleted_rd->Recreate(fname, (lock ? 1 : 0));
      }
      else
      {
        u32 index = kHeaderBlockSize / ns_->blocksize + (maxindex + 1) * GetBlockNumFromSize(kFileSizeMax);
        if (index + GetBlockNumFromSize(kFileSizeMax) > ns_->blockcount)
        {
          fprintf(stderr, "tried to create files more than storage size");
          rd = nullptr;
        }
        else
        {
          rd = new Inode(fname, index, (lock ? 1 : 0), 0, 0, ns_, ns_->blocksize, GetQnum());
          inodes_.push_back(rd);
        }
      }
    }
    HeaderUnlock();
    return rd;
  }
  Status GetChildren(const std::string &dir,
                     std::vector<std::string> *result)
  {
    result->clear();
    for (auto itr = inodes_.begin(); itr != inodes_.end(); ++itr)
    {
      Inode *inode = *itr;
      if (inode == nullptr)
      {
        break;
      }
      if (inode->IsDeleted())
      {
        continue;
      }
      const std::string &filename = inode->GetFname();

      if (filename.size() >= dir.size() + 1 && filename[dir.size()] == '/' && (memcmp(filename.data(), dir.data(), dir.size()) == 0))
      {
        result->push_back(filename.substr(dir.size() + 1));
      }
    }
    return Status::kOk;
  }

private:
  int GetQnum()
  {
    if (qnum_ == -1)
    {
      qnum_ = qcnt_.fetch_add(1);
      if ((u32)qnum_ >= ns_->maxqcount)
      {
        fprintf(stderr, "error: not enough queues for threads\n");
        abort();
      }
    }
    return qnum_;
  }
  void HeaderWriteSync()
  {
    Inode::AsyncIoContext ctx = HeaderWrite();
    if (unvme_apoll(ctx.iod, UNVME_TIMEOUT))
    {
      printf("failed to unvme_write");
      exit(1);
    }
    unvme_free(ns_, ctx.dma);
  }
  Inode::AsyncIoContext HeaderWrite()
  {
    HeaderWriteSub();
    vfio_dma_t *dma = unvme_alloc(ns_, kHeaderBlockSize);
    if (!dma)
    {
      nvme_printf("allocation failure\n");
    }
    memcpy(dma->buf, header_buf_, kHeaderBlockSize);
    unvme_iod_t iod = unvme_awrite(ns_, GetQnum(), dma->buf, 0,
                                   kHeaderBlockSize / ns_->blocksize);
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
  void HeaderLock()
  {
    while (header_lock_.fetch_or(1) == 1)
    {
      asm volatile("" ::
                       : "memory");
    }
  }
  bool HeaderRead()
  {
    vfio_dma_t *dma = unvme_alloc(ns_, kHeaderBlockSize);
    if (!dma)
    {
      nvme_printf("allocation failure\n");
    }
    if (unvme_read(ns_, GetQnum(), dma->buf, 0, kHeaderBlockSize / ns_->blocksize))
    {
      printf("failed to unvme_read");
      exit(1);
    }
    memcpy(header_buf_, dma->buf, kHeaderBlockSize);

    unvme_free(ns_, dma);

    int pos = 0;
    pos += strlen(kVersionString);
    if (strncmp(header_buf_, kVersionString, strlen(kVersionString)))
    {
      fprintf(stderr, "header version mismatch\n");
      return false;
    }
    while (header_buf_[pos] != '\0')
    {
      int len = 0;
      Inode *inode = Inode::CreateFromBuffer(header_buf_ + pos, ns_, ns_->blocksize, len, GetQnum());
      pos += len;
    }
    return true;
  }

  void HeaderWriteSub()
  {
    int pos = 0;
    memset(header_buf_, 0, kHeaderBlockSize);
    memcpy(header_buf_, kVersionString, strlen(kVersionString));
    pos += strlen(kVersionString);
    for (auto itr = inodes_.begin(); itr != inodes_.end(); ++itr)
    {
      Inode *inode = *itr;
      if (inode != nullptr)
      {
        pos += inode->HeaderWrite(header_buf_ + pos, kHeaderBlockSize - pos);
      }
    }
    header_buf_[pos] = '\0';
  }

  void HeaderUnlock() { header_lock_ = 0; }
  template <class T>
  T Align(T val)
  {
    return (val / ns_->blocksize) * ns_->blocksize;
  }
  template <class T>
  T AlignUp(T val)
  {
    return Align(val + ns_->blocksize - 1);
  }
  u32 GetBlockNumFromSize(size_t size)
  {
    return (size + ns_->blocksize - 1) / ns_->blocksize;
  }
  const unvme_ns_t *ns_;
  std::atomic<uint> qcnt_;
  std::list<Inode *> inodes_;
  std::atomic<uint> header_lock_;
  static std::unique_ptr<Vefs> vefs_;
  static thread_local int qnum_;

  static const int kHeaderBlockSize = kFileSizeMax;
  static const char *kVersionString;
  char header_buf_[kHeaderBlockSize];
};
