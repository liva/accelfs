#include <stdint.h>
#include <assert.h>
#include <utility>

template <class Key, class Value>
class SimpleHashCache
{
public:
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
    std::pair<Key, Value> *operator->()
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
      while (i < 256 && !cache->GetFromIndex(i).second.IsValid())
      {
        i++;
      }
      return i;
    }
    SimpleHashCache *cache_;
    int index_;
  };
  void Put(Key key, Value value)
  {
    assert(value.IsValid());
    container_[GetIndexFromKey(key)].first = key;
    Value &v = container_[GetIndexFromKey(key)].second;
    v.Reset(std::move(value));
  }
  Value *Get(Key key)
  {
    std::pair<Key, Value> &ele = container_[GetIndexFromKey(key)];
    if (ele.second.IsValid() && key.Get() == ele.first.Get())
    {
      return &ele.second;
    }
    return nullptr;
  }
  static int GetIndexFromKey(Key key)
  {
    return key.Get() % 256;
  }
  std::pair<Key, Value> &GetFromIndex(int i)
  {
    return container_[i];
  }

private:
  std::pair<Key, Value> container_[256];
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
