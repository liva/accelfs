/**
 * Copyright 2020 NEC Laboratories Europe GmbH
 * All rights reserved
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *    1. Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *    2. Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 * 
 *    3. Neither the name of NEC Laboratories Europe GmbH nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY NEC Laboratories Europe GmbH AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL NEC Laboratories 
 * Europe GmbH OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO,  PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

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
    header_.Release();
    header_.WriteSync();
  }
  void Dump()
  {
    header_.Dump();
  }
  static Vefs *Get()
  {
    if (!vefs_)
    {
      Reset();
    }
    return vefs_.get();
  }
  static void Exit()
  {
    vefs_.reset(nullptr);
  }
  static void Reset()
  {
    vefs_->Exit();
    vefs_.reset(new Vefs);
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
    if (redirect_)
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
    SoftSync(inode);

    HardWrite();
  }
  void SoftSync(Inode *inode)
  {
    MEASURE_TIME;
    bool inode_updated_flag;
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
  }

  void HardWrite()
  {
    MEASURE_TIME;
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
    if (redirect_)
    {
      remove(inode->GetFname().c_str());
    }
    header_.Delete(inode);
  }

  void Rename(Inode *inode, const std::string &fname)
  {
    if (redirect_)
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
    if (inode->Write(offset, data, size) == Inode::Status::kOk)
    {
      return Status::kOk;
    }
    else
    {
      return Status::kIoError;
    }
  }
  Status
  Read(Inode *inode, uint64_t offset, size_t size, char *scratch)
  {
    if (inode->Read(offset, size, scratch) == Inode::Status::kOk)
    {
      return Status::kOk;
    }
    else
    {
      return Status::kIoError;
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
