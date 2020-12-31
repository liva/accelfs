#include "../static_allocator.h"
#include <assert.h>
#include <stdio.h>

void test1()
{
    printf("%s %s\n", __FILE__, __func__);
    StaticAllocator<int> *sa = new StaticAllocator<int>();
    const int kMax = 100;
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

void test2()
{
    printf("%s %s\n", __FILE__, __func__);
    StaticAllocator<int> *sa = new StaticAllocator<int>();
    const int kMax = 100;
    for (int i = 0; i < kMax; i++)
    {
        sa->Alloc();
    }
    try
    {
        sa->Alloc();
        assert(false);
    }
    catch (...)
    {
    }
    delete sa;
}

void test3()
{
    printf("%s %s\n", __FILE__, __func__);
    StaticAllocator<int> *sa = new StaticAllocator<int>();
    const int kMax = 100;
    int *var[kMax];
    for (int i = 0; i < kMax; i++)
    {
        var[i] = sa->Alloc();
    }
    sa->Free(var[0]);
    sa->Alloc();
    delete sa;
}

void test4()
{
    printf("%s %s\n", __FILE__, __func__);
    StaticAllocator<int> *sa = new StaticAllocator<int>();
    const int kMax = 100;
    int *var[kMax];
    for (int i = 0; i < kMax; i++)
    {
        var[i] = sa->Alloc();
        *var[i] = i;
    }
    sa->Free(var[0]);
    *sa->Alloc() = kMax;
    for (int i = 1; i < kMax; i++)
    {
        assert(*var[i] == i);
    }
    delete sa;
}

void static_allocator_main()
{
    test1();
    test2();
    test3();
    test4();
}

int main()
{
    static_allocator_main();
    return 0;
}