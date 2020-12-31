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
#include <stdlib.h>
#include <new>

template <class T>
class StaticAllocator
{
public:
  StaticAllocator()
  {
    for (int i = 0; i < kBufferMax; i++) {
      _map[i] = false;
    }
  }
  ~StaticAllocator()
  {
    if (_cnt != 0) {
      assert(false);
    }
    if (_next) {
      delete _next;
    }
    if (_max) {
      fprintf(stderr, "DEBUG: StaticAllocator could not handle every allocation requests with its own buffer. The maximum allocation count was %d\n", _max);
    }
  }
  template <class... Args>
  T *Alloc(Args... args)
  {
    if (_next) {
      int max = _cnt;
      StaticAllocator *cur = _next;
      while(cur) {
        max += cur->_cnt;
        cur = cur->_next;
      }
      if (max > _max) {
        _max = max;
      }
    }
    void *buf = AllocBuf();
    return new(buf) T(args...);
  }
  void Free(T *data)
  {
    if ((char *)data < _buf || _buf + GetAlignedStructSize() * kBufferMax < (char *)data) {
      if (_next) {
        _next->Free(data);
      } else {
        assert(false);
      }
    } else {
      data->~T();
      int i = ((char *)data - _buf) / GetAlignedStructSize();
      assert(_map[i]);
      _map[i] = false;
      _cnt--;
      if (i < _cur) {
        _cur = i;
      }
    }
    if (_cnt < kBufferMax / 2 && _next && _next->_cnt == 0 && !_next->_next) {
      delete _next; 
      _next = nullptr;
    }
  }
  int GetDepth() {
    return 1 + (_next ? _next->GetDepth() : 0);
  }
private:
  void *AllocBuf() {
    if (_cnt == kBufferMax) {
      if (!_next) {
        _next = new StaticAllocator<T>();
      }
      return _next->AllocBuf();
    }
    for (int i = _cur; i < kBufferMax; i++) {
      if (!_map[i]) {
        _map[i] = true;
        _cur = i + 1;
        _cnt++;
        return _buf + GetAlignedStructSize() * i;
      }
    }
    fprintf(stderr, "StaticAllocator error\n");
    _exit(-1);
  }
  static constexpr size_t GetAlignedStructSize() {
    return ((sizeof(T) + 7) / 8) * 8;
  }
  static const int kBufferMax = 4096;
  bool _map[kBufferMax];
  int _cur = 0;
  int _cnt = 0;
  StaticAllocator *_next = nullptr;
  char _buf[GetAlignedStructSize() * kBufferMax];
  int _max = 0;
};

#ifdef __TEST__
#include <assert.h>
#include <stdio.h>


static inline void test1()
{
    printf("%s %s\n", __FILE__, __func__);
    StaticAllocator<int> *sa = new StaticAllocator<int>();
    const int kMax = 128;
    int *var[kMax];
    for (int i = 0; i < kMax; i++)
    {
        var[i] = sa->Alloc();
        *var[i] = i;
    }
    for (int i = 0; i < kMax; i++)
    {
        assert(*var[i] == i);
    }
    delete sa;
}

static inline void test2()
{
    printf("%s %s\n", __FILE__, __func__);
    StaticAllocator<int> *sa = new StaticAllocator<int>();
    const int kMax = 128;
    for (int i = 0; i < kMax * 2; i++)
    {
        sa->Alloc();
    }
    delete sa;
}

static inline void test3()
{
    printf("%s %s\n", __FILE__, __func__);
    StaticAllocator<int> *sa = new StaticAllocator<int>();
    const int kMax = 128;
    int *var[kMax*2];
    for (int i = 0; i < kMax*2; i++)
    {
        var[i] = sa->Alloc();
    }
    for (int i = 0; i < kMax*2; i++)
    {
      sa->Free(var[i]);
    }
    delete sa;
}

static inline void test4()
{
    printf("%s %s\n", __FILE__, __func__);
    StaticAllocator<int> *sa = new StaticAllocator<int>();
    const int kMax = 128;
    int *var[kMax*2];
    for (int i = 0; i < kMax*2; i++)
    {
        var[i] = sa->Alloc();
    }
    for (int i = kMax*2-1; i >= 0; i--)
    {
      sa->Free(var[i]);
    }
    delete sa;
}

static inline void test5()
{
    printf("%s %s\n", __FILE__, __func__);
    StaticAllocator<int> *sa = new StaticAllocator<int>();
    const int kMax = 128;
    int *var[kMax*2];
    for (int i = 0; i < kMax*2; i++)
    {
        var[i] = sa->Alloc();
        *var[i] = i;
    }
    sa->Free(var[0]);
    *sa->Alloc() = kMax*2;
    for (int i = 1; i < kMax*2; i++)
    {
        assert(*var[i] == i);
    }
    delete sa;
}

static inline void test6() {
    printf("%s %s\n", __FILE__, __func__);
  StaticAllocator<int> *sa = new StaticAllocator<int>();
  const int kMax = 128;
  assert(sa->GetDepth() == 1);
  int *var[kMax*2];
  for(int i = 0 ; i < kMax + 5; i++) {
    var[i] = sa->Alloc();
  }
  for(int i = kMax / 2 - 1; i < kMax + 4; i++) {
    sa->Free(var[i]);
    assert(sa->GetDepth() == 2);
  }
  sa->Free(var[kMax + 4]);
  assert(sa->GetDepth() == 1);
  delete sa;
}

class Hoge {
public:
  Hoge() = delete;
  Hoge(int i) {
    _tmp = i;
  }
  ~Hoge() {
   }
  Hoge &operator=(const int i) {
    _tmp = i;
    return *this;
  }
  bool operator==(int i) {
    return _tmp == i;
  }
  int _tmp = 0;
};

static inline void test7() {
  printf("%s %s\n", __FILE__, __func__);
  StaticAllocator<Hoge> *sa = new StaticAllocator<Hoge>();
  Hoge *h = sa->Alloc(3);
  assert(h->_tmp == 3);
  delete sa;
}

static inline void static_allocator_main()
{
    test1();
    test2();
    test3();
    test4();
    test5();
    test6();
    test7();
}

#endif
