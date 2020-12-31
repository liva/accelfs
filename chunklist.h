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
#include <stdint.h>
#include <assert.h>
#include <map>
#include <vector>
#include <algorithm>

template <int kLevel, size_t kLen>
class ChunkListChild
{
public:
    ChunkListChild() = delete;
    ChunkListChild(uint64_t lba) : lba_(lba)
    {
        updated_ = true;
        for (int i = 0; i < kLen; i++)
        {
            buf_[i] = nullptr;
        }
    }
    ~ChunkListChild()
    {
        assert(!updated_);
    }
    void Dump()
    {
        printf("%c", updated_ ? 't' : 'f');
        for (int i = 0; i < kLen; i++)
        {
            if (buf_[i] != nullptr)
            {
                printf("[");
                buf_[i]->Dump();
                printf("]");
            }
            else
            {
                printf("[0]");
            }
        }
    }
    bool IsUpdated()
    {
        bool flag = updated_;
        for (int i = 0; i < kLen; i++)
        {
            if (buf_[i] != nullptr)
            {
                flag |= buf_[i]->IsUpdated();
            }
        }
        return flag;
    }
    uint64_t GetFromIndex(uint64_t index)
    {
        CheckIndex(index);
        if (buf_[index / Child::GetEntryNum()] == nullptr)
        {
            return 0;
        }
        return buf_[index / Child::GetEntryNum()]->GetFromIndex(index % Child::GetEntryNum());
    }
  void Apply(uint64_t index, uint64_t *lba_array)
  {
    CheckIndex(index);
    if (buf_[index / Child::GetEntryNum()] == nullptr)
      {
        buf_[index / Child::GetEntryNum()] = new Child(*lba_array);
        lba_array++;
        updated_ = true;
      }
    buf_[index / Child::GetEntryNum()]->Apply(index % Child::GetEntryNum(), lba_array);
    return;
  }
    bool Apply(uint64_t index, uint64_t lba)
    {
        CheckIndex(index);
        if (buf_[index / Child::GetEntryNum()] == nullptr)
        {
            buf_[index / Child::GetEntryNum()] = new Child(lba);
            updated_ = true;
            return false;
        }
        return buf_[index / Child::GetEntryNum()]->Apply(index % Child::GetEntryNum(), lba);
    }
  int ReportUnallocatedCnt(uint64_t index) {
    CheckIndex(index);
    if (buf_[index / Child::GetEntryNum()] == nullptr) {
      return kLevel + 1;
    }
    return buf_[index / Child::GetEntryNum()]->ReportUnallocatedCnt(index % Child::GetEntryNum());
  }
    void Write(uint64_t *buf, uint64_t &lba)
    {
        if (updated_)
        {
            for (int i = 0; i < kLen; i++)
            {
                buf[i] = (buf_[i] == nullptr) ? 0 : buf_[i]->GetLba();
            }
            updated_ = false;
            lba = lba_;
        }
        else
        {
            for (int i = 0; i < kLen; i++)
            {
                if (buf_[i] != nullptr && buf_[i]->IsUpdated())
                {
                    buf_[i]->Write(buf, lba);
                    return;
                }
            }
        }
    }
    static void AnalyzeForConstruction(uint64_t top_lba, std::map<uint64_t, void *> &buf_map, std::vector<uint64_t> &required_lba_list)
    {
        auto it = buf_map.find(top_lba);
        if (it == buf_map.end())
        {
            required_lba_list.push_back(top_lba);
            return;
        }

        uint64_t *buf = (uint64_t *)(it->second);
        for (int i = 0; i < kLen; i++)
        {
            uint64_t child_lba = buf[i];
            if (child_lba != 0)
            {
                Child::AnalyzeForConstruction(child_lba, buf_map, required_lba_list);
            }
        }
    }
    static ChunkListChild *CreateFromBuffer(uint64_t top_lba, std::map<uint64_t, void *> &buf_map)
    {
        ChunkListChild *clc = new ChunkListChild(top_lba);
        uint64_t *buf = (uint64_t *)buf_map[top_lba];
        clc->updated_ = false;
        for (int i = 0; i < kLen; i++)
        {
            uint64_t child_lba = buf[i];
            if (child_lba != 0)
            {
                clc->buf_[i] = Child::CreateFromBuffer(child_lba, buf_map);
            }
        }
        return clc;
    }
    void Shrink(size_t cnum, std::vector<uint64_t> &release_list)
    {
        for (int i = 0; i < kLen; i++)
        {
            if (buf_[i] != nullptr)
            {
                buf_[i]->Shrink(cnum, release_list);
                if (cnum == 0)
                {
                    buf_[i]->ReleaseLba(release_list);
                    delete buf_[i];
                    buf_[i] = nullptr;
                    updated_ = true;
                }
            }
            cnum -= std::min(cnum, Child::GetEntryNum());
        }
    }
    static size_t GetEntryNum()
    {
        return kLen * Child::GetEntryNum();
    }
    uint64_t GetLba() const
    {
        return lba_;
    }
    void ReleaseLba(std::vector<uint64_t> &release_list)
    {
        updated_ = false;
        release_list.push_back(lba_);
        return;
    }

private:
    void CheckIndex(uint64_t index)
    {
        assert(index < kLen * Child::GetEntryNum());
    }
    const uint64_t lba_;
    using Child = ChunkListChild<kLevel - 1, kLen>;
    Child *buf_[kLen];
    bool updated_;
};

template <size_t kLen>
class ChunkListChild<0, kLen>
{
public:
    ChunkListChild() = delete;
    ChunkListChild(uint64_t lba) : lba_(lba)
    {
        updated_ = true;
        for (int i = 0; i < kLen; i++)
        {
            buf_[i] = 0;
        }
    }
    ~ChunkListChild()
    {
        assert(!updated_);
    }
    void Dump()
    {
        printf("%c", updated_ ? 't' : 'f');
    }
    bool IsUpdated()
    {
        return updated_;
    }
    uint64_t GetFromIndex(uint64_t index)
    {
        assert(index < kLen);
        return buf_[index];
    }
  void Apply(uint64_t index, uint64_t *lba_array)
    {
        assert(index < kLen);
        if (buf_[index] == 0) {
          buf_[index] = *lba_array;
          updated_ = true;
        }
    }
    bool Apply(uint64_t index, uint64_t lba)
    {
        assert(index < kLen);
        assert(buf_[index] == 0);
        buf_[index] = lba;
        updated_ = true;
        return true;
    }
  int ReportUnallocatedCnt(uint64_t index)  {
    assert(index < kLen);
    return buf_[index] == 0 ? 1 : 0;
  }
    void Write(uint64_t *buf, uint64_t &lba)
    {
        memcpy(buf, buf_, kLen * sizeof(uint64_t));
        updated_ = false;
        lba = lba_;
    }
    static void AnalyzeForConstruction(uint64_t top_lba, std::map<uint64_t, void *> &buf_map, std::vector<uint64_t> &required_lba_list)
    {
        auto it = buf_map.find(top_lba);
        if (it == buf_map.end())
        {
            required_lba_list.push_back(top_lba);
        }
    }
    static ChunkListChild *CreateFromBuffer(uint64_t top_lba, std::map<uint64_t, void *> &buf_map)
    {
        ChunkListChild *clc = new ChunkListChild(top_lba);
        uint64_t *buf = (uint64_t *)buf_map[top_lba];
        clc->Read(buf);
        return clc;
    }
    void Read(uint64_t *buf)
    {
        updated_ = false;
        memcpy(buf_, buf, kLen * sizeof(uint64_t));
    }
    void Shrink(size_t cnum, std::vector<uint64_t> &release_list)
    {
        for (int i = cnum; i < kLen; i++)
        {
            uint64_t lba = buf_[i];
            if (lba != 0)
            {
                release_list.push_back(lba);
                buf_[i] = 0;
                updated_ = true;
            }
        }
    }
    static size_t GetEntryNum()
    {
        return kLen;
    }
    uint64_t GetLba() const
    {
        return lba_;
    }
    void ReleaseLba(std::vector<uint64_t> &release_list)
    {
        updated_ = false;
        release_list.push_back(lba_);
        return;
    }

private:
    uint64_t lba_;
    uint64_t buf_[kLen];
    bool updated_;
};

class ChunkList
{
public:
    ChunkList() = delete;
    ChunkList(uint64_t lba) : child0_(lba)
    {
        // create new
        len_ = 0;
        child1_ = nullptr;
        child2_ = nullptr;
        child3_ = nullptr;
        updated_ = true;
    }
    ~ChunkList()
    {
        if (child1_ != nullptr)
        {
            delete child1_;
        }
        if (child2_ != nullptr)
        {
            delete child2_;
        }
        if (child3_ != nullptr)
        {
            delete child3_;
        }
        assert(!updated_);
    }
    void Dump()
    {
        printf("ChunkList %p\n", this);
        printf("0>%c", (updated_ ? 't' : 'f'));
        child0_.Dump();
        printf("\n");
        if (child1_ != nullptr)
        {
            printf("1>");
            child1_->Dump();
            printf("\n");
        }
        if (child2_ != nullptr)
        {
            printf("2>");
            child2_->Dump();
            printf("\n");
        }
        if (child3_ != nullptr)
        {
            printf("3>");
            child3_->Dump();
            printf("\n");
        }
    }
    bool IsUpdated()
    {
        bool flag = updated_ || child0_.IsUpdated();
        if (child1_ != nullptr)
        {
            flag |= child1_->IsUpdated();
        }
        if (child2_ != nullptr)
        {
            flag |= child2_->IsUpdated();
        }
        if (child3_ != nullptr)
        {
            flag |= child3_->IsUpdated();
        }
        return flag;
    }
    uint64_t GetLba() const
    {
        return child0_.GetLba();
    }
    uint64_t GetLen()
    {
        return len_;
    }
    uint64_t GetFromIndex(uint64_t index)
    {
        CheckIndex(index);
        if (index < Child0::GetEntryNum())
        {
            return child0_.GetFromIndex(index);
        }
        index -= Child0::GetEntryNum();
        if (index < Child1::GetEntryNum())
        {
            if (child1_ == nullptr)
            {
                return 0;
            }
            return child1_->GetFromIndex(index);
        }
        index -= Child1::GetEntryNum();
        if (index < Child2::GetEntryNum())
        {
            if (child2_ == nullptr)
            {
                return 0;
            }
            return child2_->GetFromIndex(index);
        }
        index -= Child2::GetEntryNum();
        if (index < Child3::GetEntryNum())
        {
            if (child3_ == nullptr)
            {
                return 0;
            }
            return child3_->GetFromIndex(index);
        }
        abort();
    }
    void Apply(uint64_t index, uint64_t *lba_array)
    {
        CheckIndex(index);
        if (index < Child0::GetEntryNum())
        {
            child0_.Apply(index, lba_array);
            return;
        }
        index -= Child0::GetEntryNum();
        if (index < Child1::GetEntryNum())
        {
            if (child1_ == nullptr)
            {
                child1_ = new Child1(*lba_array);
                lba_array++;
                updated_ = true;
            }
            child1_->Apply(index, lba_array);
            return;
        }
        index -= Child1::GetEntryNum();
        if (index < Child2::GetEntryNum())
        {
            if (child2_ == nullptr)
            {
                child2_ = new Child2(*lba_array);
                lba_array++;
                updated_ = true;
            }
            child2_->Apply(index, lba_array);
            return;
        }
        index -= Child2::GetEntryNum();
        if (index < Child3::GetEntryNum())
        {
            if (child3_ == nullptr)
            {
                child3_ = new Child3(*lba_array);
                lba_array++;
                updated_ = true;
            }
            child3_->Apply(index, lba_array);
            return;
        }
        abort();
    }
    // return value: success or require another lba
    bool Apply(uint64_t index, uint64_t lba)
    {
        CheckIndex(index);
        if (index < Child0::GetEntryNum())
        {
            return child0_.Apply(index, lba);
        }
        index -= Child0::GetEntryNum();
        if (index < Child1::GetEntryNum())
        {
            if (child1_ == nullptr)
            {
                child1_ = new Child1(lba);
                updated_ = true;
                return false;
            }
            return child1_->Apply(index, lba);
        }
        index -= Child1::GetEntryNum();
        if (index < Child2::GetEntryNum())
        {
            if (child2_ == nullptr)
            {
                child2_ = new Child2(lba);
                updated_ = true;
                return false;
            }
            return child2_->Apply(index, lba);
        }
        index -= Child2::GetEntryNum();
        if (index < Child3::GetEntryNum())
        {
            if (child3_ == nullptr)
            {
                child3_ = new Child3(lba);
                updated_ = true;
                return false;
            }
            return child3_->Apply(index, lba);
        }
        abort();
    }
  int ReportUnallocatedCnt(uint64_t index) {
        CheckIndex(index);
        if (index < Child0::GetEntryNum())
        {
            return child0_.ReportUnallocatedCnt(index);
        }
        index -= Child0::GetEntryNum();
        if (index < Child1::GetEntryNum())
        {
            if (child1_ == nullptr)
            {
              return 2;
            }
            return child1_->ReportUnallocatedCnt(index);
        }
        index -= Child1::GetEntryNum();
        if (index < Child2::GetEntryNum())
        {
            if (child2_ == nullptr)
            {
              return 3;
            }
            return child2_->ReportUnallocatedCnt(index);
        }
        index -= Child2::GetEntryNum();
        if (index < Child3::GetEntryNum())
        {
            if (child3_ == nullptr)
            {
              return 4;
            }
            return child3_->ReportUnallocatedCnt(index);
        }
        abort();
  }
  void Truncate(size_t len, std::vector<uint64_t> &release_list)
    {
        if (len < len_)
        {
            Shrink(len, release_list);
        }
        len_ = len;
        updated_ = true;
        assert(len_ <= MaxLen());
    }
  void Expand(size_t len)
    {
        assert(len <= MaxLen() && len_ < len);
        len_ = len;
        updated_ = true;
    }
    size_t MaxLen()
    {
        return (Child0::GetEntryNum() + Child1::GetEntryNum() + Child2::GetEntryNum() + Child3::GetEntryNum()) * kChunkSize;
    }
    bool Write(void *buf, uint64_t &lba)
    {
        assert(IsUpdated());
        uint64_t *_buf = (uint64_t *)buf;
        if (updated_ || child0_.IsUpdated())
        {
            _buf[0] = len_;
            child0_.Write(_buf + 1, lba);
            _buf[1 + Child0::GetEntryNum() + 0] = (child1_ == nullptr) ? 0 : child1_->GetLba();
            _buf[1 + Child0::GetEntryNum() + 1] = (child2_ == nullptr) ? 0 : child2_->GetLba();
            _buf[1 + Child0::GetEntryNum() + 2] = (child3_ == nullptr) ? 0 : child3_->GetLba();
            updated_ = false;
        }
        else if (child1_ != nullptr && child1_->IsUpdated())
        {
            child1_->Write(_buf, lba);
        }
        else if (child2_ != nullptr && child2_->IsUpdated())
        {
            child2_->Write(_buf, lba);
        }
        else if (child3_ != nullptr && child3_->IsUpdated())
        {
            child3_->Write(_buf, lba);
        }
        else
        {
            abort();
        }
        return IsUpdated();
    }
    static void AnalyzeForConstruction(uint64_t top_lba, std::map<uint64_t, void *> &buf_map, std::vector<uint64_t> &required_lba_list)
    {
        auto it = buf_map.find(top_lba);
        if (it == buf_map.end())
        {
            required_lba_list.push_back(top_lba);
            return;
        }

        int index = 1 + Child0::GetEntryNum();
        uint64_t *buf = (uint64_t *)(it->second);
        {
            uint64_t child1_lba = buf[index];
            if (child1_lba != 0)
            {
                Child1::AnalyzeForConstruction(child1_lba, buf_map, required_lba_list);
            }
            index++;
        }
        {
            uint64_t child2_lba = buf[index];
            if (child2_lba != 0)
            {
                Child2::AnalyzeForConstruction(child2_lba, buf_map, required_lba_list);
            }
            index++;
        }
        {
            uint64_t child3_lba = buf[index];
            if (child3_lba != 0)
            {
                Child3::AnalyzeForConstruction(child3_lba, buf_map, required_lba_list);
            }
            index++;
        }
    }
    static ChunkList *CreateFromBuffer(uint64_t top_lba, std::map<uint64_t, void *> &buf_map)
    {
        ChunkList *cl = new ChunkList(top_lba);
        uint64_t *_buf = (uint64_t *)buf_map[top_lba];
        cl->updated_ = false;
        cl->len_ = _buf[0];
        cl->child0_.Read(_buf + 1);
        int index = 1 + Child0::GetEntryNum();
        {
            uint64_t child1_lba = _buf[index];
            if (child1_lba != 0)
            {
                cl->child1_ = Child1::CreateFromBuffer(child1_lba, buf_map);
            }
            index++;
        }
        {
            uint64_t child2_lba = _buf[index];
            if (child2_lba != 0)
            {
                cl->child2_ = Child2::CreateFromBuffer(child2_lba, buf_map);
            }
            index++;
        }
        {
            uint64_t child3_lba = _buf[index];
            if (child3_lba != 0)
            {
                cl->child3_ = Child3::CreateFromBuffer(child3_lba, buf_map);
            }
            index++;
        }
        return cl;
    }

private:
    void Shrink(size_t len, std::vector<uint64_t> &release_list)
    {
        uint64_t cnum = GetChunkNumFromLen(len);
        child0_.Shrink(cnum, release_list);
        cnum -= std::min(cnum, Child0::GetEntryNum());

        if (child1_ != nullptr)
        {
            child1_->Shrink(cnum, release_list);
            if (cnum == 0)
            {
                child1_->ReleaseLba(release_list);
                delete child1_;
                child1_ = nullptr;
                updated_ = true;
            }
        }
        cnum -= std::min(cnum, Child1::GetEntryNum());

        if (child2_ != nullptr)
        {
            child2_->Shrink(cnum, release_list);
            if (cnum == 0)
            {
                child2_->ReleaseLba(release_list);
                delete child2_;
                child2_ = nullptr;
                updated_ = true;
            }
        }
        cnum -= std::min(cnum, Child2::GetEntryNum());

        if (child3_ != nullptr)
        {
            child3_->Shrink(cnum, release_list);
            if (cnum == 0)
            {
                child3_->ReleaseLba(release_list);
                delete child3_;
                child3_ = nullptr;
                updated_ = true;
            }
        }
        cnum -= std::min(cnum, Child3::GetEntryNum());
    }
    void CheckIndex(uint64_t index)
    {
        assert(index < GetChunkNumFromLen(len_));
    }
    size_t GetChunkNumFromLen(size_t len)
    {
        return (len + kChunkSize - 1) / kChunkSize;
    }
    uint64_t len_;
    bool updated_;
    using Child0 = ChunkListChild<0, kChunkSize / sizeof(uint64_t) - 4>;
    using Child1 = ChunkListChild<0, kChunkSize / sizeof(uint64_t)>;
    using Child2 = ChunkListChild<1, kChunkSize / sizeof(uint64_t)>;
    using Child3 = ChunkListChild<2, kChunkSize / sizeof(uint64_t)>;
    Child0 child0_;
    Child1 *child1_;
    Child2 *child2_;
    Child3 *child3_;
};

#if 0
bool vector_find(std::vector<uint64_t> &vec, uint64_t num)
{
    for (auto it = vec.begin(); it != vec.end(); ++it)
    {
        if (*it == num)
        {
            return true;
        }
    }
    return false;
}
void test()
{
    using Child0 = ChunkListChild<0, kChunkSize / sizeof(uint64_t) - 4>;
    using Child1 = ChunkListChild<0, kChunkSize / 8>;
    using Child2 = ChunkListChild<1, kChunkSize / 8>;
    using Child3 = ChunkListChild<2, kChunkSize / 8>;
    {
        ChunkList cl(123);
        assert(cl.GetLba() == 123);
        uint64_t lba;
        uint64_t buf[kChunkSize / 8];
        cl.Write(buf, lba);
        assert(lba == 123);
    }
    {
        ChunkList cl(123);
        uint64_t lba;
        assert(cl.IsUpdated());
        uint64_t buf[kChunkSize / 8];
        cl.Write(buf, lba);
        assert(lba == 123);
    }
    {
        ChunkList cl(123);
        uint64_t lba;
        std::vector<uint64_t> release_list;
        cl.Truncate(kChunkSize * 3 - 1, release_list);
        assert(cl.IsUpdated());
        assert(release_list.empty());
        assert(cl.GetFromIndex(1) == 0);
        cl.Truncate(0, release_list);
        assert(release_list.empty());
        uint64_t buf[kChunkSize / 8];
        cl.Write(buf, lba);
        assert(lba == 123);
    }
    {
        ChunkList cl(123);
        uint64_t lba;
        std::vector<uint64_t> release_list;
        cl.Truncate(kChunkSize * 3 - 1, release_list);
        cl.Apply(1, 10);
        assert(cl.GetFromIndex(1) == 10);
        cl.Truncate(0, release_list);
        assert(release_list.size() == 1);
        assert(vector_find(release_list, 10));
        cl.Truncate(kChunkSize * 3 - 1, release_list);
        assert(cl.GetFromIndex(1) == 0);
        uint64_t buf[kChunkSize / 8];
        cl.Write(buf, lba);
        assert(lba == 123);
    }
    {
        ChunkList cl(123);
        uint64_t lba;
        std::vector<uint64_t> release_list;
        cl.Truncate(kChunkSize * 3 - 1, release_list);
        cl.Apply(1, 10);
        uint64_t buf[kChunkSize / 8];
        for (int i = 0; i < kChunkSize / 8; i++)
        {
            buf[i] = 0xFFFFFF;
        }
        cl.Write(buf, lba);
        assert(lba == 123);
        assert(buf[0] == kChunkSize * 3 - 1);
        assert(buf[2] != 0xFFFFFF);
        buf[2] = 0;
        for (int i = 1; i < kChunkSize / 8; i++)
        {
            assert(buf[i] == 0);
        }
    }
    {
        ChunkList cl(123);
        uint64_t lba;
        std::vector<uint64_t> release_list;
        cl.Truncate(kChunkSize * 3 - 1, release_list);
        cl.Apply(1, 10);
        uint64_t buf[kChunkSize / 8];
        for (int i = 0; i < kChunkSize / 8; i++)
        {
            buf[i] = 0xFFFFFF;
        }
        cl.Write(buf, lba);
        assert(lba == 123);
        std::map<uint64_t, void *> buf_map;
        buf_map.insert(std::make_pair(123, buf));
        std::vector<uint64_t> required_lba_list;
        ChunkList::AnalyzeForConstruction(123, buf_map, required_lba_list);
        assert(required_lba_list.empty());
    }
    {
        ChunkList cl(123);
        uint64_t lba;
        std::vector<uint64_t> release_list;
        cl.Truncate(kChunkSize * 3 - 1, release_list);
        cl.Apply(1, 10);
        uint64_t buf[kChunkSize / 8];
        for (int i = 0; i < kChunkSize / 8; i++)
        {
            buf[i] = 0xFFFFFF;
        }
        cl.Write(buf, lba);
        assert(lba == 123);
        std::map<uint64_t, void *> buf_map;
        buf_map.insert(std::make_pair(123, buf));
        ChunkList *cl2 = ChunkList::CreateFromBuffer(123, buf_map);
        assert(cl2->GetLba() == 123);
        assert(cl2->GetLen() == kChunkSize * 3 - 1);
        assert(!cl2->IsUpdated());
        assert(cl2->GetFromIndex(0) == 0);
        assert(cl2->GetFromIndex(1) == 10);
        delete cl2;
    }

    // level1
    {
        ChunkList cl(123);
        uint64_t lba;
        std::vector<uint64_t> release_list;
        cl.Truncate(kChunkSize * Child0::GetEntryNum() + 100, release_list);
        assert(release_list.empty());
        assert(cl.GetFromIndex(Child0::GetEntryNum()) == 0);
        cl.Truncate(0, release_list);
        assert(release_list.empty());
        uint64_t buf[kChunkSize / 8];
        assert(!cl.Write(buf, lba));
        assert(lba == 123);
    }
    {
        ChunkList cl(123);
        uint64_t lba;
        uint64_t buf[kChunkSize / 8];
        std::vector<uint64_t> release_list;
        cl.Truncate(kChunkSize * Child0::GetEntryNum() + 100 - 1, release_list);
        cl.Write(buf, lba);
        assert(!cl.IsUpdated());
        assert(!cl.Apply(Child0::GetEntryNum(), 10));
        assert(cl.IsUpdated());
        cl.Truncate(0, release_list);
        assert(release_list.size() == 1);
        assert(vector_find(release_list, 10));
        assert(!cl.Write(buf, lba));
        assert(lba == 123);
    }
    {
        ChunkList cl(123);
        uint64_t lba;
        uint64_t buf[kChunkSize / 8];
        std::vector<uint64_t> release_list;
        cl.Truncate(kChunkSize * Child0::GetEntryNum() + 100 - 1, release_list);
        cl.Apply(Child0::GetEntryNum(), 10);
        cl.Apply(1, 11);
        cl.Truncate(0, release_list);
        assert(release_list.size() == 2);
        assert(vector_find(release_list, 10));
        assert(vector_find(release_list, 11));
        assert(!cl.Write(buf, lba));
        assert(lba == 123);
    }
    {
        ChunkList cl(123);
        uint64_t lba;
        uint64_t buf[kChunkSize / 8];
        std::vector<uint64_t> release_list;
        cl.Truncate(kChunkSize * Child0::GetEntryNum() + 100 - 1, release_list);
        cl.Apply(Child0::GetEntryNum(), 10);
        cl.Apply(1, 11);
        cl.Truncate(kChunkSize * 3, release_list);
        assert(release_list.size() == 1);
        assert(vector_find(release_list, 10));
        assert(!cl.Write(buf, lba));
        assert(lba == 123);
    }
    {
        ChunkList cl(123);
        uint64_t lba;
        uint64_t buf[kChunkSize / 8];
        std::vector<uint64_t> release_list;
        cl.Truncate(kChunkSize * Child0::GetEntryNum() + 100 - 1, release_list);
        cl.Apply(Child0::GetEntryNum(), 10);
        assert(cl.Apply(Child0::GetEntryNum(), 11));
        assert(cl.GetFromIndex(Child0::GetEntryNum()) == 11);
        assert(cl.Apply(kChunkSize / sizeof(uint64_t) - 5, 12));
        assert(cl.GetFromIndex(kChunkSize / sizeof(uint64_t) - 5) == 12);
        cl.Truncate(0, release_list);
        assert(release_list.size() == 3);
        assert(vector_find(release_list, 10));
        assert(vector_find(release_list, 11));
        assert(vector_find(release_list, 12));
        cl.Truncate(kChunkSize * Child0::GetEntryNum() + 100 - 1, release_list);
        assert(cl.GetFromIndex(Child0::GetEntryNum()) == 0);
        assert(!cl.Write(buf, lba));
        assert(lba == 123);
    }
    {
        ChunkList cl(123);
        uint64_t lba;
        uint64_t buf[kChunkSize / 8];
        std::vector<uint64_t> release_list;
        cl.Truncate(kChunkSize * Child0::GetEntryNum() + kChunkSize * 2 - 1, release_list);
        cl.Apply(Child0::GetEntryNum(), 10);
        cl.Apply(Child0::GetEntryNum(), 11);
        cl.Apply(kChunkSize / sizeof(uint64_t) - 3, 12);
        cl.Truncate(kChunkSize * Child0::GetEntryNum() + kChunkSize - 1, release_list);
        assert(release_list.size() == 1);
        assert(vector_find(release_list, 12));
        assert(cl.Write(buf, lba));
        assert(lba == 123);
        assert(!cl.Write(buf, lba));
        assert(lba == 10);
    }
    {
        ChunkList cl(123);
        uint64_t lba1, lba2;
        uint64_t buf1[kChunkSize / 8], buf2[kChunkSize / 8];
        std::vector<uint64_t> release_list;
        cl.Truncate(kChunkSize * Child0::GetEntryNum() + kChunkSize * 2 - 1, release_list);
        cl.Apply(Child0::GetEntryNum(), 10);
        cl.Apply(Child0::GetEntryNum(), 11);
        cl.Write(buf1, lba1);
        cl.Write(buf2, lba2);
        std::map<uint64_t, void *> buf_map;
        buf_map.insert(std::make_pair(lba1, buf1));
        std::vector<uint64_t> required_lba_list;
        ChunkList::AnalyzeForConstruction(123, buf_map, required_lba_list);
        assert(required_lba_list.size() == 1);
        assert(vector_find(required_lba_list, 10));
        required_lba_list.clear();
        buf_map.insert(std::make_pair(lba2, buf2));
        ChunkList::AnalyzeForConstruction(123, buf_map, required_lba_list);
        assert(required_lba_list.empty());
    }
    {
        ChunkList cl(123);
        uint64_t lba1, lba2;
        uint64_t buf1[kChunkSize / 8], buf2[kChunkSize / 8];
        std::vector<uint64_t> release_list;
        cl.Truncate(kChunkSize * Child0::GetEntryNum() + kChunkSize * 2 - 1, release_list);
        cl.Apply(Child0::GetEntryNum(), 10);
        cl.Apply(Child0::GetEntryNum(), 11);
        cl.Write(buf1, lba1);
        cl.Write(buf2, lba2);
        std::map<uint64_t, void *> buf_map;
        buf_map.insert(std::make_pair(lba1, buf1));
        buf_map.insert(std::make_pair(lba2, buf2));
        ChunkList *cl2 = ChunkList::CreateFromBuffer(123, buf_map);
        assert(cl2->GetLba() == 123);
        assert(cl2->GetLen() == kChunkSize * Child0::GetEntryNum() + kChunkSize * 2 - 1);
        assert(!cl2->IsUpdated());
        assert(cl2->GetFromIndex(0) == 0);
        assert(cl2->GetFromIndex(Child0::GetEntryNum()) == 11);
        delete cl2;
    }

    // level2
    {
        ChunkList cl(123);
        uint64_t lba;
        std::vector<uint64_t> release_list;
        cl.Truncate(kChunkSize * (Child0::GetEntryNum() + Child1::GetEntryNum()) + 100, release_list);
        assert(release_list.empty());
        assert(cl.GetFromIndex(Child0::GetEntryNum() + Child1::GetEntryNum()) == 0);
        cl.Truncate(0, release_list);
        assert(release_list.empty());
        uint64_t buf[kChunkSize / 8];
        assert(!cl.Write(buf, lba));
        assert(lba == 123);
    }
    {
        ChunkList cl(123);
        uint64_t lba;
        uint64_t buf[kChunkSize / 8];
        std::vector<uint64_t> release_list;
        cl.Truncate(kChunkSize * (Child0::GetEntryNum() + Child1::GetEntryNum()) + 100 - 1, release_list);
        cl.Write(buf, lba);
        assert(!cl.IsUpdated());
        assert(!cl.Apply((Child0::GetEntryNum() + Child1::GetEntryNum()), 10));
        assert(cl.IsUpdated());
        cl.Truncate(0, release_list);
        assert(release_list.size() == 1);
        assert(vector_find(release_list, 10));
        assert(!cl.Write(buf, lba));
        assert(lba == 123);
    }
    {
        ChunkList cl(123);
        uint64_t lba;
        uint64_t buf[kChunkSize / 8];
        std::vector<uint64_t> release_list;
        cl.Truncate(kChunkSize * (Child0::GetEntryNum() + Child1::GetEntryNum()) + 100 - 1, release_list);
        cl.Apply((Child0::GetEntryNum() + Child1::GetEntryNum()), 10);
        cl.Apply(Child0::GetEntryNum(), 11);
        cl.Truncate(0, release_list);
        assert(release_list.size() == 2);
        assert(vector_find(release_list, 10));
        assert(vector_find(release_list, 11));
        assert(!cl.Write(buf, lba));
        assert(lba == 123);
    }
    {
        ChunkList cl(123);
        uint64_t lba;
        uint64_t buf[kChunkSize / 8];
        std::vector<uint64_t> release_list;
        cl.Truncate(kChunkSize * (Child0::GetEntryNum() + Child1::GetEntryNum()) + 100 - 1, release_list);
        cl.Apply((Child0::GetEntryNum() + Child1::GetEntryNum()), 10);
        cl.Apply(Child0::GetEntryNum(), 11);
        assert(!cl.Apply((Child0::GetEntryNum() + Child1::GetEntryNum()), 12));
        assert(cl.Apply((Child0::GetEntryNum() + Child1::GetEntryNum()), 13));
        cl.Truncate(0, release_list);
        assert(release_list.size() == 4);
        assert(vector_find(release_list, 10));
        assert(vector_find(release_list, 11));
        assert(vector_find(release_list, 12));
        assert(vector_find(release_list, 13));
        assert(!cl.Write(buf, lba));
        assert(lba == 123);
    }
    {
        ChunkList cl(123);
        uint64_t lba;
        uint64_t buf[kChunkSize / 8];
        std::vector<uint64_t> release_list;
        cl.Truncate(kChunkSize * (Child0::GetEntryNum() + Child1::GetEntryNum() + 1) + 100 - 1, release_list);
        cl.Apply((Child0::GetEntryNum() + Child1::GetEntryNum()), 10);
        assert(!cl.Apply((Child0::GetEntryNum() + Child1::GetEntryNum()), 11));
        assert(cl.Apply((Child0::GetEntryNum() + Child1::GetEntryNum()), 12));
        assert(cl.Apply((Child0::GetEntryNum() + Child1::GetEntryNum() + 1), 13));
        cl.Truncate(0, release_list);
        assert(release_list.size() == 4);
        assert(vector_find(release_list, 10));
        assert(vector_find(release_list, 11));
        assert(vector_find(release_list, 12));
        assert(vector_find(release_list, 13));
        assert(!cl.Write(buf, lba));
        assert(lba == 123);
    }
    {
        ChunkList cl(123);
        uint64_t lba;
        uint64_t buf[kChunkSize / 8];
        std::vector<uint64_t> release_list;
        cl.Truncate(kChunkSize * (Child0::GetEntryNum() + Child1::GetEntryNum() + 1) + 100 - 1, release_list);
        cl.Apply((Child0::GetEntryNum() + Child1::GetEntryNum()), 10);
        assert(!cl.Apply((Child0::GetEntryNum() + Child1::GetEntryNum()), 11));
        assert(cl.Apply((Child0::GetEntryNum() + Child1::GetEntryNum()), 12));
        assert(cl.Apply((Child0::GetEntryNum() + Child1::GetEntryNum() + 1), 13));
        cl.Truncate(kChunkSize * (Child0::GetEntryNum() + Child1::GetEntryNum()) + 100 - 1, release_list);
        assert(release_list.size() == 1);
        assert(vector_find(release_list, 13));
        assert(cl.Write(buf, lba));
        assert(lba == 123);
        assert(cl.Write(buf, lba));
        assert(lba == 10);
        assert(!cl.Write(buf, lba));
        assert(lba == 11);
    }
    {
        ChunkList cl(123);
        uint64_t lba1, lba2, lba3;
        uint64_t buf1[kChunkSize / 8], buf2[kChunkSize / 8], buf3[kChunkSize / 8];
        std::vector<uint64_t> release_list;
        cl.Truncate(kChunkSize * (Child0::GetEntryNum() + Child1::GetEntryNum() + 1) + kChunkSize * 2 - 1, release_list);
        cl.Apply((Child0::GetEntryNum() + Child1::GetEntryNum() + 1), 10);
        cl.Apply((Child0::GetEntryNum() + Child1::GetEntryNum() + 1), 11);
        cl.Apply((Child0::GetEntryNum() + Child1::GetEntryNum() + 1), 12);
        cl.Apply((Child0::GetEntryNum() + Child1::GetEntryNum()), 13);
        cl.Write(buf1, lba1);
        assert(lba1 == 123);
        cl.Write(buf2, lba2);
        cl.Write(buf3, lba3);
        std::map<uint64_t, void *> buf_map;
        buf_map.insert(std::make_pair(lba1, buf1));
        std::vector<uint64_t> required_lba_list;
        ChunkList::AnalyzeForConstruction(123, buf_map, required_lba_list);
        assert(required_lba_list.size() == 1);
        assert(vector_find(required_lba_list, 10));
        required_lba_list.clear();
        buf_map.insert(std::make_pair(lba2, buf2));
        ChunkList::AnalyzeForConstruction(123, buf_map, required_lba_list);
        assert(required_lba_list.size() == 1);
        assert(vector_find(required_lba_list, 11));
        required_lba_list.clear();
        buf_map.insert(std::make_pair(lba3, buf3));
        ChunkList::AnalyzeForConstruction(123, buf_map, required_lba_list);
        assert(required_lba_list.empty());
    }
    {
        ChunkList cl(123);
        uint64_t lba1, lba2, lba3, lba4;
        uint64_t buf1[kChunkSize / 8], buf2[kChunkSize / 8], buf3[kChunkSize / 8], buf4[kChunkSize / 8];
        std::vector<uint64_t> release_list;
        cl.Truncate(kChunkSize * (Child0::GetEntryNum() + Child1::GetEntryNum() * 2) + kChunkSize * 2 - 1, release_list);
        cl.Apply((Child0::GetEntryNum() + Child1::GetEntryNum() + 1), 10);
        cl.Apply((Child0::GetEntryNum() + Child1::GetEntryNum() + 1), 11);
        cl.Apply((Child0::GetEntryNum() + Child1::GetEntryNum() + 1), 12);
        assert(!cl.Apply((Child0::GetEntryNum() + Child1::GetEntryNum() * 2), 13));
        assert(cl.Apply((Child0::GetEntryNum() + Child1::GetEntryNum() * 2), 14));
        cl.Write(buf1, lba1);
        cl.Write(buf2, lba2);
        cl.Write(buf3, lba3);
        cl.Write(buf4, lba4);
        assert(lba4 == 13);
        std::map<uint64_t, void *> buf_map;
        buf_map.insert(std::make_pair(lba1, buf1));
        std::vector<uint64_t> required_lba_list;
        ChunkList::AnalyzeForConstruction(123, buf_map, required_lba_list);
        assert(required_lba_list.size() == 1);
        assert(vector_find(required_lba_list, 10));
        required_lba_list.clear();
        buf_map.insert(std::make_pair(lba2, buf2));
        ChunkList::AnalyzeForConstruction(123, buf_map, required_lba_list);
        assert(required_lba_list.size() == 2);
        assert(vector_find(required_lba_list, 11));
        assert(vector_find(required_lba_list, 13));
        required_lba_list.clear();
        buf_map.insert(std::make_pair(lba3, buf3));
        buf_map.insert(std::make_pair(lba4, buf4));
        ChunkList::AnalyzeForConstruction(123, buf_map, required_lba_list);
        assert(required_lba_list.empty());
    }
}
#endif
