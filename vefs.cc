#include "vefs.h"

std::unique_ptr<Vefs> Vefs::vefs_;
const char *Header::kVersionString = "VEDIOF14";
std::vector<TimeInfo *> time_list_;

void DumpTime()
{
    printf("/--/--/--/--/--/--/--/--/--/--/--/--\n");
    for (auto it = time_list_.begin(); it != time_list_.end(); ++it)
    {
        TimeInfo *ti = *it;
        printf("%ld\t%ld\t%ld %s:%d\n", ti->time_, ti->count_, ti->time_ / ti->count_, ti->fname_.c_str(), ti->line_);
    }
}