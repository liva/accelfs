#pragma once
#include <stdint.h>
#include <stdio.h>

class RtcTaker
{
public:
    RtcTaker() : x_(get())
    {
    }
    void PrintMeasuredTime()
    {
        Print(get() - x_);
    }
#ifdef __ve__
    static inline uint64_t get()
    {
        uint64_t ret;
        void *vehva = ((void *)0x000000001000);
        asm volatile("" ::
                         : "memory");
        asm volatile("lhm.l %0,0(%1)"
                     : "=r"(ret)
                     : "r"(vehva));
        asm volatile("" ::
                         : "memory");
        // the "800" is due to the base frequency of Tsubasa
        return ((uint64_t)1000 * ret) / 800;
    }
#else

    static inline uint64_t get()
    {
        uint32_t aux;
        uint64_t rax, rdx;
        asm volatile("" ::
                         : "memory");
        asm volatile("rdtscp\n"
                     : "=a"(rax), "=d"(rdx), "=c"(aux)
                     :
                     :);
        asm volatile("" ::
                         : "memory");
        return ((rdx << 32) + rax) * 10 / 26;
    }

#endif
protected:
    virtual void Print(uint64_t time) = 0;

private:
    const uint64_t x_;
};

class TimeTaker
{
public:
    TimeTaker(const char *const string) : time_taker_(string)
    {
    }
    ~TimeTaker()
    {
        time_taker_.PrintMeasuredTime();
    }

private:
    class TimeTakerInternal final : public RtcTaker
    {
    public:
        TimeTakerInternal(const char *const string) : string_(string)
        {
        }
        virtual void Print(uint64_t time) override
        {
            printf(">>>%s : %luns\n", string_, time);
        }
        const char *const string_;
    };
    TimeTakerInternal time_taker_;
};