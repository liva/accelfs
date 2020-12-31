#include "threadlocal.h"

thread_local int ThreadLocalCtrl::index_ = -1;
std::atomic<int> ThreadLocalCtrl::current_index_(0);
