#pragma once

#include <unvme.h>
#include <unvme_nvme.h>
#include <string>
#include <list>
#include <memory>
#include <atomic>
#include <vector>
#include <vepci.h>

#include <stdint.h>
extern uint64_t tmp_var;
static inline uint64_t ve_get()
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

class Vefs;

class Inode
{
public:
  struct AsyncIoContext
  {
    unvme_iod_t iod;
    void *buf;
  };
  Inode(std::string fname, u32 index, int lock, size_t len, const unvme_ns_t *ns, size_t blocksize) : ns_(ns)
  {
    fname_ = fname;
    index_ = index;
    lock_ = lock;
    len_ = len;
    blocksize_ = blocksize;
  }
  Inode(const Inode &) = delete;
  Inode &operator=(const Inode &) = delete;
  Inode(Inode &&) = default;
  Inode() = default;
  Inode &operator=(Inode &&) = default;
  static Inode CreateFromBuffer(const char *buf, const unvme_ns_t *ns, size_t blocksize, int &pos)
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
    return Inode(fname, index, lock, len, ns, blocksize);
  }
  ~Inode()
  {
    Sync();
    if (rsize_ > 0)
    {
      unvme_free(ns_, rbuf_);
    }
  }
  void Truncate(size_t len)
  {
    len_ = len;
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
    fname_ = fname;
  }
  size_t HeaderWrite(char *buf, size_t pos_max)
  {
    size_t pos = 0;

    if (AlignUp(fname_.length() + 1) + sizeof(index_) + sizeof(lock_) + sizeof(len_) > pos_max)
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
    return pos;
  }
  void RefreshCache(char *buf, u32 index)
  {
    if (!cache_)
    {
      cache_ = std::shared_ptr<Cache>(new Cache(index, blocksize_));
    }
    memcpy(cache_->buf_, buf, blocksize_);
    cache_->index_ = index;
  }
  bool ReuseCache(char *buf, u32 lba)
  {
    if (cache_ && cache_->index_ == lba)
    {
      memcpy(buf, cache_->buf_, blocksize_);
      return true;
    }
    return false;
  }
  void Sync()
  {
    auto itr = io_waiting_queue_.begin();
    while (itr != io_waiting_queue_.end())
    {
      if (unvme_apoll((*itr).iod, UNVME_TIMEOUT))
      {
        printf("failed to unvme_write");
        exit(1);
      }
      unvme_free(ns_, (*itr).buf);
      itr = io_waiting_queue_.erase(itr);
    }
  }
  void RegisterWaitingContext(AsyncIoContext ctx)
  {
    io_waiting_queue_.push_back(ctx);
  }
  std::string &GetFname()
  {
    return fname_;
  }

  void *AllocBuffer(size_t size)
  {
    if (size > rsize_)
    {
      if (rsize_ != 0)
      {
        unvme_free(ns_, rbuf_);
      }
      rbuf_ = unvme_alloc(ns_, size);
      rsize_ = size;
    }
    return rbuf_;
  }

private:
  std::string fname_;
  u32 index_;
  int lock_;
  size_t len_;
  const unvme_ns_t *ns_;
  size_t blocksize_;
  std::vector<AsyncIoContext> io_waiting_queue_;

  void *rbuf_ = nullptr;
  size_t rsize_ = 0;

  struct Cache
  {
    u32 index_;
    void *buf_;
    Cache() = delete;
    Cache(u32 index, size_t blocksize)
    {
      index_ = index;
      buf_ = malloc(blocksize);
    }
    ~Cache()
    {
      free(buf_);
    }
  };
  std::shared_ptr<Cache> cache_;
  static size_t AlignUp(size_t len)
  {
    return ((len + 3) / 4) * 4;
  }
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
        blocksize_(ns_->blocksize),
        file_isize_(kFileSize / blocksize_),
        qcnt_(0),
        header_lock_(0)
  {
    if (kHeaderBlockSize % ns_->blocksize != 0)
    {
      fprintf(stderr, "unsupported blocksize\n");
      exit(1);
    }
    printf("%s qc=%d/%d qs=%d/%d bc=%#lx bs=%d\n", ns_->device, ns_->qcount,
           ns_->maxqcount, ns_->qsize, ns_->maxqsize, ns_->blockcount,
           ns_->blocksize);

    if (!HeaderRead())
    {
      HeaderWriteSync();
    }
  }
  ~Vefs()
  {
    auto it = header_data_.begin();
    while (it != header_data_.end())
    {
      it = header_data_.erase(it);
    }
    CacheSync();
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

  void Truncate(Inode *inode, size_t size)
  {
    inode->Truncate(size);
    HeaderLock();
    HeaderWriteSync();
    HeaderUnlock();
  }

  size_t GetLen(Inode *inode)
  {
    return inode->GetLen();
  }

  void Sync(Inode *inode)
  {
    inode->Sync();
    CacheSync();
  }

  void CacheSync()
  {
    void *buf = unvme_alloc(ns_, 512); // dummy
    u32 cdw10_15[6];                   // dummy
    int stat = unvme_cmd(ns_, GetQnum(), NVME_CMD_FLUSH, ns_->id, buf, 512, cdw10_15, 0);
    if (stat)
    {
      printf("failed to sync");
    }
    unvme_free(ns_, buf);
  }

  Status Append(Inode *inode, const void *data, size_t size)
  {
    return Write(inode, inode->GetLen(), data, size);
  }

  Status Write(Inode *inode, size_t offset, const void *data, size_t size)
  {
    if (offset + size >= kFileSize)
    {
      nvme_printf("IO Error\n");
      return Status::kIoError;
    }
    nvme_printf("w> %ld %ld\n", offset, size);
    size_t oldsize = inode->GetLen();
    size_t end = offset + size;
    if (end > oldsize)
    {
      inode->Truncate(end);
      HeaderLock();
      inode->RegisterWaitingContext(HeaderWrite());
      HeaderUnlock();
    }

    size_t aligned_offset = Align(offset);
    size_t ndsize = AlignUp(end - aligned_offset);

    u32 lba = inode->GetLba(offset);
    u32 nlb = (u32)(ndsize / ns_->blocksize);
    if (lba >= ns_->blockcount)
    {
      nvme_printf("tried to access %d\n", lba);
      return Status::kIoError;
    }

    nvme_printf("w^ %d (=%ld/bs) %d (=%ld/bs)\n", lba, offset, nlb, size);
    void *buf = unvme_alloc(ns_, ndsize);
    if (!buf)
    {
      nvme_printf("allocation failure\n");
      return Status::kTryAgain;
    }
    if ((offset % ns_->blocksize) != 0 || (end % ns_->blocksize) != 0)
    {
      std::vector<size_t> preload_offset;
      size_t offset_list[2] = {offset, end};
      for (size_t toffset : offset_list)
      {
        if ((toffset % ns_->blocksize) != 0)
        {
          size_t aligned_toffset = Align(toffset);

          if (inode->ReuseCache(reinterpret_cast<char *>(buf) + aligned_toffset - aligned_offset, inode->GetLba(toffset)))
          {
            continue;
          }
          if (oldsize == 0 || Align(oldsize - 1) < aligned_toffset)
          {
            continue;
          }
          preload_offset.push_back(toffset);
        }
      }
      if (preload_offset.size() == 2 && inode->GetLba(preload_offset[0]) == inode->GetLba(preload_offset[1]))
      {
        preload_offset.pop_back();
      }

      std::vector<unvme_iod_t> preload_iod;
      for (size_t toffset : preload_offset)
      {
        unvme_iod_t iod = unvme_aread(ns_, GetQnum(), reinterpret_cast<char *>(buf) + Align(toffset) - aligned_offset, inode->GetLba(toffset), 1);
        if (!iod)
        {
          printf("failed to unvme_write");
          exit(1);
        }
        preload_iod.push_back(iod);
      }
      for (unvme_iod_t iod : preload_iod)
      {
        if (unvme_apoll(iod, UNVME_TIMEOUT))
        {
          printf("failed to unvme_write");
          exit(1);
        }
      }
    }

    memcpy((u8 *)buf + offset - aligned_offset, data, size);
    unvme_iod_t iod = unvme_awrite(ns_, GetQnum(), buf, lba, nlb);
    if (!iod)
    {
      printf("failed to unvme_write");
      exit(1);
    }
    Inode::AsyncIoContext ctx = {
        .iod = iod,
        .buf = buf,
    };
    inode->RegisterWaitingContext(ctx);

    if (inode->GetLba(inode->GetLen()) == lba + nlb - 1)
    {
      inode->RefreshCache(reinterpret_cast<char *>(buf) + ndsize - ns_->blocksize, lba + nlb - 1);
    }

    return Status::kOk;
  }

  Status Read(Inode *inode, uint64_t offset, size_t n, char *scratch)
  {
    if (offset + n >= kFileSize)
    {
      nvme_printf("IO Error\n");
      return Status::kIoError;
    }
    // aurora_test
    // printf("Read> %s %d %ld %ld\n", inode->fname.c_str(), inode->index,
    // offset, n);

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
    void *buf = inode->AllocBuffer(ndsize);
    if (!buf)
    {
      nvme_printf("allocation failure\n");
      return Status::kTryAgain;
    }
    unvme_read(ns_, GetQnum(), buf, lba, nlb);
    memcpy(scratch, (u8 *)buf + offset - noffset, n);

    return Status::kOk;
  }

  Status ReadWithIoBuf(Inode *inode, uint64_t offset, size_t n, char *scratch)
  {
    if (offset + n >= kFileSize)
    {
      nvme_printf("IO Error\n");
      return Status::kIoError;
    }
    // aurora_test
    // printf("Read> %s %d %ld %ld\n", inode->fname.c_str(), inode->index,
    // offset, n);

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

    return Status::kOk;
  }

  u16 GetBlockSize() { return blocksize_; }

  Inode *GetInode(const std::string &fname)
  {
    HeaderLock();
    auto it = header_data_.begin();
    while (it != header_data_.end())
    {
      if ((*it).GetFname() == fname)
      {
        HeaderUnlock();
        return &(*it);
      }
      ++it;
    }
    HeaderUnlock();
    return nullptr;
  }

  void Dump()
  {
    HeaderLock();
    auto it = header_data_.begin();
    while (it != header_data_.end())
    {
      // printf("Dump>> %p %s %d %lu %d\n", &(*it), (*it).fname.c_str(),
      // (*it).index, (*it).len, (*it).lock);
      ++it;
    }
    HeaderUnlock();
  }

  void Delete(Inode *inode)
  {
    // printf("Vefs::Delete %s\n", inode->fname.c_str());
    HeaderLock();
    auto it = header_data_.begin();
    while (it != header_data_.end())
    {
      if (&(*it) == inode)
      {
        it = header_data_.erase(it);
      }
      else
        ++it;
    }
    HeaderWriteSync();
    HeaderUnlock();
  }

  void Rename(Inode *inode, const std::string &fname)
  {
    inode->Rename(fname);
    HeaderLock();
    HeaderWriteSync();
    HeaderUnlock();
  }

  Inode *Create(const std::string &fname, bool lock)
  {
    HeaderLock();

    u32 index = kHeaderBlockSize / ns_->blocksize;
    {
      auto it = header_data_.begin();
      while (it != header_data_.end())
      {
        if ((*it).GetFname() == fname)
        {
          HeaderUnlock();
          return &(*it);
        }
        if ((*it).GetIndex() + file_isize_ > index)
        {
          index = (*it).GetIndex() + file_isize_;
        }
        ++it;
      }
    }

    Inode d(fname, index, (lock ? 1 : 0), 0, ns_, ns_->blocksize);
    header_data_.push_back(std::move(d));
    auto it = header_data_.end();
    --it;
    Inode *rd = &(*it);

    // aurora_test
    // printf("Create>>%s %d %d %p\n", fname.c_str(), index, lock, rd);

    HeaderWriteSync();

    HeaderUnlock();
    return rd;
  }
  std::list<Inode> &GetHeader() { return header_data_; }

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
    unvme_free(ns_, ctx.buf);
  }
  Inode::AsyncIoContext HeaderWrite()
  {
    HeaderWriteSub();
    void *buf = unvme_alloc(ns_, kHeaderBlockSize);
    if (!buf)
    {
      nvme_printf("allocation failure\n");
    }
    memcpy(buf, header_buf_, kHeaderBlockSize);
    unvme_iod_t iod = unvme_awrite(ns_, GetQnum(), buf, 0,
                                   kHeaderBlockSize / ns_->blocksize);
    if (!iod)
    {
      printf("failed to unvme_write");
      exit(1);
    }
    Inode::AsyncIoContext ctx = {
        .iod = iod,
        .buf = buf,
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
    void *buf = unvme_alloc(ns_, kHeaderBlockSize);
    if (!buf)
    {
      nvme_printf("allocation failure\n");
    }
    if (unvme_read(ns_, GetQnum(), buf, 0, kHeaderBlockSize / ns_->blocksize))
    {
      printf("failed to unvme_read");
      exit(1);
    }
    memcpy(header_buf_, buf, kHeaderBlockSize);

    unvme_free(ns_, buf);

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
      Inode inode = Inode::CreateFromBuffer(header_buf_ + pos, ns_, ns_->blocksize, len);
      pos += len;
      header_data_.push_back(std::move(inode));
    }
    return true;
  }

  void HeaderWriteSub()
  {
    int pos = 0;
    memset(header_buf_, 0, kHeaderBlockSize);
    memcpy(header_buf_, kVersionString, strlen(kVersionString));
    pos += strlen(kVersionString);
    for (Inode &inode : header_data_)
    {
      pos += inode.HeaderWrite(header_buf_ + pos, kHeaderBlockSize - pos);
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
  const unvme_ns_t *ns_;
  const u16 blocksize_;
  const int file_isize_;
  std::atomic<uint> qcnt_;
  std::list<Inode> header_data_;
  std::atomic<uint> header_lock_;
  static std::unique_ptr<Vefs> vefs_;
  static thread_local int qnum_;

  static const int kHeaderBlockSize = 512 * 10;
  static const size_t kFileSize = 20L * 1024 * 1024 * 1024;
  static const char *kVersionString;
  char header_buf_[kHeaderBlockSize];
};
