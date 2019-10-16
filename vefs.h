#pragma once

#include <string>
#include <memory>
#include <vector>
#include <string.h>

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
    // printf("Vefs::Delete %s\n", inode->fname.c_str());
    inode->Delete();
    header_.Delete(inode);
  }

  void Rename(Inode *inode, const std::string &fname)
  {
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
    while (true)
    {
      if (size == 0)
      {
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
    }
  }

private:
  Status WriteChunk(Inode *inode, size_t offset, const void *data, size_t size, size_t oldsize);
  Status
  ReadChunk(Inode *inode, uint64_t offset, size_t n, char *scratch);
  Status ReadChunkWithIoBuf(Inode *inode, uint64_t offset, size_t n, char *scratch);
  Status PrereadUnalignedBlock(Inode *inode, vfio_dma_t *dma, size_t offset, size_t end, size_t oldsize);
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
  debug_printf("\nr[%s %lu %lu]\n", inode->GetFname().c_str(), offset, n);
  inode->RetrieveContexts();
  size_t noffset = Align(offset);
  size_t ndsize = AlignUp(n + offset - noffset);

  u64 lba = inode->GetLba(noffset);
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
  if (ReadChunkWithIoBuf(inode, noffset, ndsize, (char *)dma->buf) != Status::kOk)
  {
    return Status::kIoError;
  }
  memcpy(scratch, (u8 *)dma->buf + offset - noffset, n);
  unvme_free(ns_, dma);

  for (size_t i = 0; i < n; i++)
  {
    debug_printf("%02x", scratch[i]);
  }
  debug_printf("\n");

  return Status::kOk;
}

inline Vefs::Status Vefs::ReadChunkWithIoBuf(Inode *inode, uint64_t offset, size_t n, char *scratch)
{
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

  u64 lba = inode->GetLba(noffset);
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

inline Vefs::Status Vefs::WriteChunk(Inode *inode, size_t offset, const void *data, size_t size, size_t oldsize)
{
  nvme_printf("w> %ld %ld\n", offset, size);
  debug_printf("\nw[%s %lu %lu]\n", inode->GetFname().c_str(), offset, size);
  for (size_t i = 0; i < size; i++)
  {
    debug_printf("%02x", ((char *)data)[i]);
  }
  debug_printf("\n");

  size_t end = offset + size;
  size_t ndsize = AlignUp(end - Align(offset));

  u64 lba = inode->GetLba(offset);
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

  Status preread_status = PrereadUnalignedBlock(inode, dma, offset, end, oldsize);
  if (preread_status != Status::kOk)
  {
    return preread_status;
  }

  memcpy((u8 *)dma->buf + offset - Align(offset), data, size);

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

inline Vefs::Status Vefs::PrereadUnalignedBlock(Inode *inode, vfio_dma_t *dma, size_t offset, size_t end, size_t oldsize)
{
  if (oldsize == 0)
  {
    return Status::kOk;
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
    char *tbuf = reinterpret_cast<char *>(dma->buf) + Align(toffset) - Align(offset);
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
  return Status::kOk;
}