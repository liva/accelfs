#include <stdint.h>
#include <assert.h>

template <class Key, class Value>
class SimpleHashCache
{
public:
  struct Container
  {
    Key k;
    Value v;
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
      return index_ == 256;
    }
    Container *operator->()
    {
      return &cache_->GetFromIndex(index_);
    }
    Iterator &operator=(Iterator it)
    {
      cache_ = it.cache_;
      index_ = it.index_;
      return *this;
    }

  private:
    Iterator(SimpleHashCache &cache, const int index) : cache_(&cache), index_(index >= 256 ? 256 : index)
    {
    }
    static int FindNextIndex(SimpleHashCache *cache, int i)
    {
      while (i < 256 && !cache->GetFromIndex(i).v.IsValid())
      {
        i++;
      }
      return i;
    }
    SimpleHashCache *cache_;
    int index_;
  };
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
  Value *Get(Key key)
  {
    Container &ele = container_[GetIndexFromKey(key)];
    if (ele.v.IsValid() && key.Get() == ele.k.Get())
    {
      return &ele.v;
    }
    return nullptr;
  }
  static int GetIndexFromKey(Key key)
  {
    return key.Get() % 256;
  }
  Container &GetFromIndex(int i)
  {
    return container_[i];
  }

private:
  Container container_[256];
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
    for(int i = 0; i < 256; i ++) {
      h.Put(TestKey(i), TestValue(i));
    }
    for(int i = 0; i < 256; i ++) {
      assert(h.Get(TestKey(i))->Get() == i);
    }
  }
  {
    SimpleHashCache<TestKey, TestValue> h;
    for(int i = 0; i < 512; i ++) {
      h.Put(TestKey(i), TestValue(i));
    }
    for(int i = 0; i < 256; i ++) {
      assert(h.Get(TestKey(i)) == nullptr);
    }
    for(int i = 256; i < 512; i ++) {
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
