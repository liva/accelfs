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

#include <stdint.h>
#include <assert.h>
#include "vefs_autogen_conf.h"

template <class Key, class Value>
class SimpleHashCache
{
public:
  struct Container
  {
    Key k;
    Value v;
    int signature;
  };
  class Iterator
  {
  public:
    Iterator() = delete;
    Iterator(SimpleHashCache &cache) : cache_(&cache), index_(FindNextIndex(&cache, 0))
    {
    }
    Iterator Next()
    {
      return Iterator(*cache_, FindNextIndex(cache_, index_ + 1));
    }
    bool IsEnd()
    {
      return index_ == kEntryNum;
    }
    Key GetKey()
    {
      assert(!IsEnd());
      return cache_->GetFromIndex(index_).k;
    }
    Iterator &operator=(Iterator it)
    {
      cache_ = it.cache_;
      index_ = it.index_;
      return *this;
    }

  private:
    Iterator(SimpleHashCache &cache, const int index) : cache_(&cache), index_(index >= kEntryNum ? kEntryNum : index)
    {
    }
    static int FindNextIndex(SimpleHashCache *cache, int i)
    {
      while (i < kEntryNum && !cache->GetFromIndex(i).v.IsValid())
      {
        i++;
      }
      return i;
    }
    SimpleHashCache *cache_;
    int index_;
  };
  SimpleHashCache() {
    for(int i = 0; i < kEntryNum; i++) {
      container_[i].signature = 0xdeadbeef;
    }
  }
  void Put(Container &&obj, Container &old)
  {
    Container &c = container_[GetIndexFromKey(obj.k)];
    if (c.v.IsValid())
    {
      old.k = c.k;
      old.v.Reset(std::move(c.v));
    }
    c.k = obj.k;
    c.v.Reset(std::move(obj.v));
  }
  Value Get(Key key)
  {
    Container &ele = container_[GetIndexFromKey(key)];
    if (ele.v.IsValid() && key.Get() == ele.k.Get())
    {
      return Value(std::move(ele.v));
    }
    return Value();
  }
  static int GetIndexFromKey(Key key)
  {
    return key.Get() % kEntryNum;
  }
  int GetNum()
  {
    int num = 0;
    for (int i = 0; i < kEntryNum; i++)
    {
      if (container_[i].v.IsValid())
      {
        num++;
      }
      assert(container_[i].signature == 0xdeadbeef);
    }
    return num;
  }

#ifdef LARGE_CACHE
  static const int kEntryNum = 4096;
#else
  static const int kEntryNum = 256;
#endif
private:
  Container &GetFromIndex(int i)
  {
    return container_[i];
  }
  Container container_[kEntryNum];
};

#if 0
class TestKey {
public:
  TestKey() {
  }
  TestKey(uint64_t i) : i_(i) {
  }
  uint64_t Get() {
    return i_;
  }
private:
  uint64_t i_;
};

class TestValue {
public:
  TestValue() {
    valid_ = false;
  }
  TestValue(uint64_t v):v_(v) {
    valid_ = true;
  }
  bool IsValid() {
    return valid_;
  }
  uint64_t Get() {
    return v_;
  }
private:
  uint64_t v_;
  bool valid_;
};

int main() {
  {
    SimpleHashCache<TestKey, TestValue> h;
    assert(h.Get(TestKey(0)) == nullptr);
  }
  {
    SimpleHashCache<TestKey, TestValue> h;
    for(int i = 0; i < h.kEntryNum; i ++) {
      h.Put(TestKey(i), TestValue(i));
    }
    for(int i = 0; i < h.kEntryNum; i ++) {
      assert(h.Get(TestKey(i))->Get() == i);
    }
  }
  {
    SimpleHashCache<TestKey, TestValue> h;
    for(int i = 0; i < h.kEntryNum * 2; i ++) {
      h.Put(TestKey(i), TestValue(i));
    }
    for(int i = 0; i < h.kEntryNum; i ++) {
      assert(h.Get(TestKey(i)) == nullptr);
    }
    for(int i = h.kEntryNum; i < h.kEntryNum * 2; i ++) {
      assert(h.Get(TestKey(i))->Get() == i);
    }
  }
  {
    SimpleHashCache<TestKey, TestValue> h;
    SimpleHashCache<TestKey, TestValue>::Iterator it(h);
    assert(it.IsEnd());
    assert(it.Next().IsEnd());
  }
  {
    SimpleHashCache<TestKey, TestValue> h;
    h.Put(TestKey(1), TestValue(1));
    SimpleHashCache<TestKey, TestValue>::Iterator it(h);
    assert(it->second.Get() == 1);
    assert(it.Next().IsEnd());
  }
  return 0;
}
#endif
