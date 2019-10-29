#pragma once

#include <atomic>

class Spinlock
{
public:
    Spinlock(std::atomic<int> &lock) : lock_(lock)
    {
        while (lock_.fetch_or(1) == 1)
        {
            asm volatile("" ::
                             : "memory");
        }
    }
    ~Spinlock()
    {
        lock_ = 0;
    }

private:
    std::atomic<int> &lock_;
};
