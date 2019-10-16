#include "vefs.h"

thread_local int Vefs::qnum_ = -1;
std::unique_ptr<Vefs> Vefs::vefs_;
const char *Header::kVersionString = "VEDIOF14";
