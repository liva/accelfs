#pragma once

#include <string>
#include <memory>
#include <vector>
#include <deque>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>

#include "misc.h"
#include "inode.h"
#include "header.h"

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
      : ns_(ns_wrapper_.ns_),
        header_(ns_, GetQnum()),
        qcnt_(0)
  {
    if (header_.GetBlockSize() % ns_->blocksize != 0)
    {
      fprintf(stderr, "unsupported blocksize\n");
      exit(1);
    }
    printf("%s qc=%d/%d qs=%d/%d bc=%#lx bs=%d maxbpio=%d\n", ns_->device, ns_->qcount,
           ns_->maxqcount, ns_->qsize, ns_->maxqsize, ns_->blockcount,
           ns_->blocksize, ns_->maxbpio);
  }
  ~Vefs()
  {
    header_.WriteSync(GetQnum());
    header_.Release();
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
      header_.WriteSync(GetQnum());
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

  u16 GetBlockSize() { return ns_->blocksize; }

  Inode *GetInode(const std::string &fname)
  {
    return header_.GetInode(fname);
  }

  void Delete(Inode *inode)
  {
    if (kRedirect)
    {
      remove(inode->GetFname().c_str());
    }
    // printf("Vefs::Delete %s\n", inode->fname.c_str());
    inode->Delete();
    header_.Delete(inode);
  }

  void Rename(Inode *inode, const std::string &fname)
  {
    if (kRedirect)
    {
      rename(inode->GetFname().c_str(), fname.c_str());
    }
    if (DoesExist(fname))
    {
      Delete(Create(fname, false));
    }
    inode->Rename(fname);
  }

  bool DoesExist(const std::string &fname)
  {
    return header_.DoesExist(fname);
  }
  Inode *Create(const std::string &fname, bool lock)
  {
    return header_.Create(fname, lock, GetQnum());
  }
  Status GetChildren(const std::string &dir,
                     std::vector<std::string> *result)
  {
    header_.GetChildren(dir, result);
    return Status::kOk;
  }

  Status Write(Inode *inode, size_t offset, const void *data, size_t size)
  {
    debug_printf("w[%s %lu %lu]\n", inode->GetFname().c_str(), offset, size);
    if (kRedirect)
    {
      int fd = open((inode->GetFname()).c_str(), O_RDWR | O_CREAT);
      pwrite(fd, data, size, offset);
      close(fd);
    }
    const char *data_ = (const char *)data;
    inode->RetrieveContexts();
    size_t oldsize = inode->GetLen();
    size_t end = offset + size;
    if (end > oldsize)
    {
      if (inode->Truncate(end) != Inode::Status::kOk)
      {
        return Status::kIoError;
      }
    }

    std::deque<unvme_iod_t> iod_queue;
    {
      size_t coffset = offset;
      const char *cdata = data_;
      size_t csize = size;
      while (true)
      {
        if (csize == 0)
        {
          break;
        }
        size_t boundary = inode->GetNextChunkBoundary(coffset);
        size_t io_size = (coffset + csize > boundary) ? boundary - coffset : csize;

        size_t noffset = AlignChunk(coffset);
        size_t ndsize = AlignChunkUp(io_size);
        assert(ndsize == kChunkSize);
        ChunkIndex cindex = ChunkIndex::CreateFromPos(noffset);

        bool createnew_ifmissing = (coffset == noffset && io_size == kChunkSize) || (oldsize <= noffset);
        if (inode->PrepareCache(cindex, createnew_ifmissing, iod_queue) != Inode::Status::kOk)
        {
          return Status::kIoError;
        }

        coffset += io_size;
        cdata += io_size;
        csize -= io_size;
      }
    }

    for (auto it = iod_queue.begin(); it != iod_queue.end(); ++it)
    {
      unvme_iod_t iod = *it;
      if (unvme_apoll(iod, UNVME_TIMEOUT))
      {
        fprintf(stderr, "VEFS: apoll failed\n");
        return Status::kIoError;
      }
    }

    {
      size_t coffset = offset;
      const char *cdata = data_;
      size_t csize = size;
      while (true)
      {
        if (csize == 0)
        {
          inode->ShrinkCacheListIfNeeded();
          return Status::kOk;
        }
        size_t boundary = inode->GetNextChunkBoundary(coffset);
        size_t io_size = (coffset + csize > boundary) ? boundary - coffset : csize;

        size_t noffset = AlignChunk(coffset);
        size_t ndsize = AlignChunkUp(io_size);
        assert(ndsize == kChunkSize);
        ChunkIndex cindex = ChunkIndex::CreateFromPos(noffset);

        Cache *cache = inode->FindFromCacheList(cindex);
        assert(cache != nullptr);
        cache->Refresh(cdata, coffset - noffset, io_size);

        coffset += io_size;
        cdata += io_size;
        csize -= io_size;
      }
    }
  }
  Status
  Read(Inode *inode, uint64_t offset, size_t size, char *scratch)
  {
    size_t flen = inode->GetLen();
    if (offset + size > flen)
    {
      size = flen - offset;
    }
    inode->RetrieveContexts();

    std::deque<unvme_iod_t> iod_queue;
    {
      size_t coffset = offset;
      char *cdata = scratch;
      size_t csize = size;
      while (true)
      {
        if (csize == 0)
        {
          uint64_t nc_offset = AlignChunkUp(coffset);
          if (nc_offset < flen)
          {
            // prefetch
            ChunkIndex cindex = ChunkIndex::CreateFromPos(nc_offset);
            if (inode->PrepareCache(cindex, false, iod_queue) != Inode::Status::kOk)
            {
              return Status::kIoError;
            }
          }
          break;
        }
        size_t boundary = inode->GetNextChunkBoundary(coffset);
        size_t io_size = (coffset + csize > boundary) ? boundary - coffset : csize;

        size_t noffset = AlignChunk(coffset);
        size_t ndsize = AlignChunkUp(io_size);
        assert(ndsize == kChunkSize);
        ChunkIndex cindex = ChunkIndex::CreateFromPos(noffset);

        if (inode->PrepareCache(cindex, false, iod_queue) != Inode::Status::kOk)
        {
          return Status::kIoError;
        }

        coffset += io_size;
        cdata += io_size;
        csize -= io_size;
      }
    }

    for (auto it = iod_queue.begin(); it != iod_queue.end(); ++it)
    {
      unvme_iod_t iod = *it;
      if (unvme_apoll(iod, UNVME_TIMEOUT))
      {
        fprintf(stderr, "VEFS: apoll failed\n");
        return Status::kIoError;
      }
    }

    {
      size_t coffset = offset;
      char *cdata = scratch;
      size_t csize = size;
      while (true)
      {
        if (csize == 0)
        {
          inode->ShrinkCacheListIfNeeded();
          debug_printf("r[%s %lu %lu]\n", inode->GetFname().c_str(), offset, size);
          if (kRedirect)
          {
            void *buf = malloc(size);
            int fd = open((inode->GetFname()).c_str(), O_RDWR | O_CREAT);
            pread(fd, buf, size, offset);
            if (memcmp(buf, scratch, size) != 0)
            {
              printf("check failed %s %lu %zu\n", inode->GetFname().c_str(), offset, size);
              exit(1);
            }
            free(buf);
            close(fd);
          }
          return Status::kOk;
        }
        size_t boundary = inode->GetNextChunkBoundary(coffset);
        size_t io_size = (coffset + csize > boundary) ? boundary - coffset : csize;

        size_t noffset = AlignChunk(coffset);
        size_t ndsize = AlignChunkUp(io_size);
        assert(ndsize == kChunkSize);
        ChunkIndex cindex = ChunkIndex::CreateFromPos(noffset);

        Cache *cache = inode->FindFromCacheList(cindex);
        assert(cache != nullptr);
        cache->Apply(cdata, coffset - noffset, io_size);

        coffset += io_size;
        cdata += io_size;
        csize -= io_size;
      }
    }
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

  template <class T>
  T Align(T val)
  {
    return align(val, ns_->blocksize);
  }
  template <class T>
  T AlignUp(T val)
  {
    return alignup(val, ns_->blocksize);
  }
  u32 GetBlockNumFromSize(size_t size)
  {
    return getblocknum_from_size(size, ns_->blocksize);
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

  class UnvmeWrapper
  {
  public:
    UnvmeWrapper() : ns_(unvme_open("b3:00.0"))
    {
    }
    ~UnvmeWrapper()
    {
      unvme_close(ns_);
    }
    const unvme_ns_t *ns_;
  } ns_wrapper_;

  const unvme_ns_t *ns_;
  Header header_;
  std::atomic<uint> qcnt_;
  static std::unique_ptr<Vefs> vefs_;
  static thread_local int qnum_;
};
