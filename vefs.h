#pragma once

#include <string>
#include <memory>
#include <vector>
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
    int cnt = 0;
    while (true)
    {
      if (size == 0)
      {
        return Status::kOk;
      }
      size_t boundary = inode->GetNextChunkBoundary(offset);
      size_t io_size = (offset + size > boundary) ? boundary - offset : size;
      Status io_status = WriteChunk(inode, offset, data_, io_size, oldsize);
      if (io_status != Status::kOk)
      {
        return io_status;
      }
      size -= io_size;
      data_ += io_size;
      offset = boundary;
      cnt++;
      if (cnt > 1)
      {
        fprintf(stderr, "warning : performance optimization is needed\n");
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
    size_t total_size = size;
    uint64_t original_offset = offset;
    char *original_scratch = scratch;
    int cnt = 0;
    while (true)
    {
      if (size == 0)
      {
        if (kRedirect)
        {
          void *buf = malloc(total_size);
          int fd = open((inode->GetFname()).c_str(), O_RDWR | O_CREAT);
          pread(fd, buf, total_size, original_offset);
          if (memcmp(buf, original_scratch, total_size) != 0)
          {
            printf("check failed %s %lu %zu\n", inode->GetFname().c_str(), original_offset, total_size);
            exit(1);
          }
          free(buf);
          close(fd);
        }
        return Status::kOk;
      }
      size_t boundary = inode->GetNextChunkBoundary(offset);
      size_t io_size = (offset + size > boundary) ? boundary - offset : size;
      Status io_status = ReadChunk(inode, offset, io_size, scratch);
      if (io_status != Status::kOk)
      {
        return io_status;
      }
      size -= io_size;
      scratch += io_size;
      offset = boundary;
      cnt++;
      if (cnt > 1)
      {
        fprintf(stderr, "warning : performance optimization is needed\n");
      }
    }
  }

private:
  Status WriteChunk(Inode *inode, size_t offset, const void *data, size_t size, size_t oldsize);
  Status
  ReadChunk(Inode *inode, uint64_t offset, size_t n, char *scratch);
  //Status PrereadUnalignedBlock(Inode *inode, vfio_dma_t *dma, size_t offset, size_t end, size_t oldsize);
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

inline Vefs::Status
Vefs::ReadChunk(Inode *inode, uint64_t offset, size_t n, char *scratch)
{
  size_t noffset = AlignChunk(offset);
  size_t ndsize = AlignChunkUp(n + offset - noffset);
  assert(ndsize == kChunkSize);
  ChunkIndex cindex = ChunkIndex::CreateFromPos(noffset);

  Cache *cache;
  if (inode->GetCache(cindex, false, cache) != Inode::Status::kOk)
  {
    return Status::kIoError;
  }
  cache->Apply(scratch, offset - noffset, n);

  debug_printf("r[%s %lu %lu]\n", inode->GetFname().c_str(), offset, n);
  for (size_t i = 0; i < n; i++)
  {
    debug_printf("%02x", scratch[i]);
  }
  debug_printf("\n");

  return Status::kOk;
}

inline Vefs::Status Vefs::WriteChunk(Inode *inode, size_t offset, const void *data, size_t size, size_t oldsize)
{
  nvme_printf("w> %ld %ld\n", offset, size);
  debug_printf("w[%s %lu %lu]\n", inode->GetFname().c_str(), offset, size);
  for (size_t i = 0; i < size; i++)
  {
    debug_printf("%02x", ((char *)data)[i]);
  }
  debug_printf("\n");

  size_t noffset = AlignChunk(offset);
  size_t ndsize = AlignChunkUp(size + offset - noffset);
  assert(ndsize == kChunkSize);
  ChunkIndex cindex = ChunkIndex::CreateFromPos(noffset);

  Cache *cache;
  bool createnew_ifmissing = (offset == noffset && size == kChunkSize) || (oldsize <= noffset);
  if (inode->GetCache(cindex, createnew_ifmissing, cache) != Inode::Status::kOk)
  {
    return Status::kIoError;
  }
  cache->Refresh(reinterpret_cast<const char *>(data), offset - noffset, size);

  return Status::kOk;
}
