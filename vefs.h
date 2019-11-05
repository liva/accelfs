#pragma once

#include <string>
#include <memory>
#include <vector>
#include <deque>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include "spinlock.h"
#include "misc.h"
#include "unvme_wrapper.h"
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
      : header_(ns_wrapper_)
  {
    if (header_.GetBlockSize() % ns_wrapper_.GetBlockSize() != 0)
    {
      fprintf(stderr, "unsupported blocksize\n");
      exit(1);
    }
  }
  ~Vefs()
  {
    header_.WriteSync();
    header_.Release();
  }
  void Dump()
  {
    header_.Dump();
  }
  static Vefs *Get()
  {
    if (!vefs_)
    {
      vefs_.reset(new Vefs);
    }
    return vefs_.get();
  }

  Status Truncate(Inode *inode, size_t size)
  {
    Spinlock lock(inode->GetLock());
    if (inode->Truncate(size) != Inode::Status::kOk)
    {
      return Status::kIoError;
    }
    return Status::kOk;
  }

  size_t GetLen(Inode *inode)
  {
    Spinlock lock(inode->GetLock());
    size_t len = inode->GetLen();
    if (kRedirect)
    {
      FILE *fp = fopen((inode->GetFname()).c_str(), "rb");
      fseek(fp, 0, SEEK_END);
      if (len != ftell(fp))
      {
        printf("length comparison failed %s\n", (inode->GetFname()).c_str());
        exit(1);
      }
      fclose(fp);
    }
    return len;
  }

  void Sync(Inode *inode)
  {
    MEASURE_TIME;
    bool inode_updated_flag;
    std::vector<Inode::AsyncIoContext> ctxs;
    {
      Spinlock lock(inode->GetLock());
      inode_updated_flag = inode->IsUpdated();
      inode->CacheListSync();
      inode->SyncChunkList();
    }
    if (inode_updated_flag)
    {
      header_.WriteSync();
    }
    {
      Spinlock lock(inode->GetLock());
      inode->WaitIoCompletion();
    }

    HardWrite();
  }

  void HardWrite()
  {
    ns_wrapper_.HardWrite();
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
      Inode *dinode = Create(fname, false);
      assert(dinode);
      Delete(dinode);
    }
    Spinlock lock(inode->GetLock());
    inode->Rename(fname);
  }

  bool DoesExist(const std::string &fname)
  {
    vefs_printf("de[%s]\n", fname.c_str());
    return header_.DoesExist(fname);
  }
  Inode *Create(const std::string &fname, bool lock)
  {
    vefs_printf("c[%s]\n", fname.c_str());
    return header_.Create(fname, lock);
  }
  Status GetChildren(const std::string &dir,
                     std::vector<std::string> *result)
  {
    header_.GetChildren(dir, result);
    return Status::kOk;
  }

  Status Write(Inode *inode, size_t offset, const void *data, size_t size)
  {
    MEASURE_TIME;
    Spinlock lock(inode->GetLock());
    vefs_printf("w[%s %lu %lu]\n", inode->GetFname().c_str(), offset, size);
    if (kRedirect)
    {
      std::string fname = inode->GetFname();
      int fd = open(fname.c_str(), O_RDWR | O_CREAT);
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
        if (inode->PrepareCache(cindex, createnew_ifmissing) != Inode::Status::kOk)
        {
          return Status::kIoError;
        }

        coffset += io_size;
        cdata += io_size;
        csize -= io_size;
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
          inode->ShrinkCacheListIfNeeded(0);
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
        inode->ApollIod(*cache);
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
    MEASURE_TIME;
    Spinlock lock(inode->GetLock());
    size_t flen = inode->GetLen();
    if (offset > flen)
    {
      return Status::kOk;
    }
    if (offset + size > flen)
    {
      size = flen - offset;
    }
    inode->RetrieveContexts();

    size_t coffset = offset;
    char *cdata = scratch;
    size_t prefetch_offset = AlignChunkUp(offset + size * 2);
    if (prefetch_offset > flen)
    {
      prefetch_offset = flen;
    }
    size_t csize = prefetch_offset - offset; //size + kChunkSize;
    {
      while (csize != 0)
      {
        size_t boundary = inode->GetNextChunkBoundary(coffset);
        size_t io_size = (coffset + csize > boundary) ? boundary - coffset : csize;

        size_t noffset = AlignChunk(coffset);
        size_t ndsize = AlignChunkUp(io_size);
        assert(ndsize == kChunkSize);
        ChunkIndex cindex = ChunkIndex::CreateFromPos(noffset);

        if (inode->PrepareCache(cindex, false) != Inode::Status::kOk)
        {
          return Status::kIoError;
        }

        coffset += io_size;
        cdata += io_size;
        csize -= io_size;
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
          inode->ShrinkCacheListIfNeeded((prefetch_offset - AlignChunk(offset)) / kChunkSize);
          vefs_printf("r[%s %lu %lu]\n", inode->GetFname().c_str(), offset, size);
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
        inode->ApollIod(*cache);
        cache->Apply(cdata, coffset - noffset, io_size);

        coffset += io_size;
        cdata += io_size;
        csize -= io_size;
      }
    }
  }

private:
  template <class T>
  T Align(T val)
  {
    return align(val, ns_wrapper_.GetBlockSize());
  }
  template <class T>
  T AlignUp(T val)
  {
    return alignup(val, ns_wrapper_.GetBlockSize());
  }
  u32 GetBlockNumFromSize(size_t size)
  {
    return getblocknum_from_size(size, ns_wrapper_.GetBlockSize());
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

  UnvmeWrapper ns_wrapper_;
  Header header_;
  static std::unique_ptr<Vefs> vefs_;
};
