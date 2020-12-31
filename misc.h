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

#include <vepci.h>
#include <stdint.h>
#include <stdlib.h>
#include <vector>
#include <string>
#include "_misc.h"

extern bool debug_time_;
extern bool redirect_;
extern bool header_dump_;

extern bool debug_flag; //debug

//#pragma clang optimize off

static const bool kDebug = false;

static inline uint64_t ve_gettime()
{
    uint64_t ret;
    void *vehva = ((void *)0x000000001000);
    asm volatile("":::"memory");
    asm volatile("lhm.l %0,0(%1)"
                 : "=r"(ret)
                 : "r"(vehva));
    asm volatile("":::"memory");
    return ((uint64_t)1000 * ret) / 800;
}

struct TimeInfo;
extern std::vector<TimeInfo *> time_list_;
struct TimeInfo
{
    TimeInfo(std::string fname, int line)
    {
        if (debug_time_)
        {
            fname_ = fname;
            line_ = line;
            time_list_.push_back(this);
        }
    }

    std::string fname_;
    int line_;
    uint64_t time_ = 0;
    uint64_t count_ = 0;
};

class TimeMeasure
{
public:
    TimeMeasure(TimeInfo &ti) : ti_(ti)
    {
        if (debug_time_)
        {
            time_ = ve_gettime();
        }
    }
    ~TimeMeasure()
    {
        if (debug_time_)
        {
            ti_.time_ += ve_gettime() - time_;
            ti_.count_++;
        }
    }

private:
    TimeInfo &ti_;
    uint64_t time_;
};

#define _GENVAR(x, y) x##y
#define GENVAR(x, y) _GENVAR(x, y)
#if 0
#define MEASURE_TIME                                                     \
    static TimeInfo GENVAR(ti, __LINE__)(__PRETTY_FUNCTION__, __LINE__); \
    TimeMeasure GENVAR(tm, __LINE__)(GENVAR(ti, __LINE__));
#else
#define MEASURE_TIME
#endif

void DumpTime();

template <class T, class U>
inline T align(T val, U blocksize)
{
    return (val / blocksize) * blocksize;
}
template <class T, class U>
inline T alignup(T val, U blocksize)
{
    return align(val + blocksize - 1, blocksize);
}
template <class T, class U>
inline T getblocknum_from_size(T size, U blocksize)
{
    return (size + blocksize - 1) / blocksize;
}

//#define vefs_printf(...) printf(__VA_ARGS__)
#define vefs_printf(...)

class HeaderBuffer
{
public:
    HeaderBuffer()
    {
    }
    void ResetPos()
    {
        pos_ = 0;
    }
    void AppendFromBuffer(HeaderBuffer &buf)
    {
        AppendRaw(buf.buf_.data() + buf.pos_, buf.buf_.size() - buf.pos_);
        buf.pos_ = buf.buf_.size();
    }
    void AlignPos()
    {
        pos_ = ((pos_ + 3) / 4) * 4;
    }
    template <class T>
    void AppendRaw(T *data, size_t len)
    {
        buf_.resize(pos_ + len);
        memcpy(buf_.data() + pos_, data, len);
        pos_ += len;
    }
    template <class T>
    void Append(T i)
    {
        AppendRaw(&i, sizeof(T));
    }
    template <class T>
    bool CompareRaw(T *data, size_t len)
    {
        if (pos_ + len > buf_.size())
        {
            fprintf(stderr, "header is not terminated\n");
            exit(1);
        }
        if (memcmp(buf_.data() + pos_, data, len) == 0)
        {
            pos_ += len;
            return true;
        }
        return false;
    }
    template <class T>
    bool Compare(T i)
    {
        return CompareRaw(&i, sizeof(i));
    }
    std::string GetString()
    {
        std::string str = std::string(buf_.data() + pos_);
        pos_ += ((str.length() + 1 + 3) / 4) * 4;
        if (pos_ > buf_.size())
        {
            fprintf(stderr, "header is not terminated\n");
            exit(1);
        }
        return str;
    }
    template <class T>
    T Get()
    {
        if (pos_ + sizeof(T) > buf_.size())
        {
            fprintf(stderr, "header is not terminated\n");
            exit(1);
        }
        T i = *(reinterpret_cast<T *>(buf_.data() + pos_));
        pos_ += sizeof(T);
        return i;
    }
    size_t Output(void *buf, size_t maxsize)
    {
        size_t size;
        if (maxsize >= buf_.size() - pos_)
        {
            // output all
            size = buf_.size() - pos_;
        }
        else
        {
            size = maxsize;
        }
        memcpy(buf, buf_.data() + pos_, size);
        pos_ += size;
        return size;
    }

    //private:
    int pos_ = 0;
    std::vector<char> buf_;
};
