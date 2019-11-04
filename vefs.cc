#include "vefs.h"

std::unique_ptr<Vefs> Vefs::vefs_;
const char *Header::kVersionString = "VEDIOF15";
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

bool debug_flag = false; //debug

std::deque<Inode::AsyncIoContext> Header::Write()
{
    std::deque<Inode::AsyncIoContext> ctxs;

    size_t info_size;
    do
    {
        HeaderBuffer tmp_buf;
        char tmp_cbuf[kChunkSize];
        tmp_buf.AppendRaw(kVersionString, strlen(kVersionString));
        u64 next_header = 0;
        tmp_buf.Append(next_header);
        tmp_buf.ResetPos();
        info_size = tmp_buf.Output(tmp_cbuf, kChunkSize);
    } while (0);

    Spinlock lock(lock_);
    HeaderBuffer content_buf;
    for (auto itr = inodes_.begin(); itr != inodes_.end(); ++itr)
    {
        Inode *inode = *itr;
        {
            Spinlock ilock(inode->GetLock());
            inode->HeaderWrite(content_buf);
        }
    }
    content_buf.Append('\0');
    content_buf.ResetPos();

    u64 hchunk_pos = kHeaderStartPos;
    size_t extra_chunk = 0;
    while (true)
    {
        vfio_dma_t *dma = ns_wrapper_.AllocChunk();
        if (!dma)
        {
            printf("allocation failure\n");
            exit(1);
        }
        size_t output_size = content_buf.Output(reinterpret_cast<char *>(dma->buf) + info_size, kChunkSize - info_size);
        bool output_completed = (output_size != kChunkSize - info_size);

        HeaderBuffer buf;
        buf.AppendRaw(kVersionString, strlen(kVersionString));
        u64 next_header = 0;
        if (!output_completed)
        {
            assert(header_exchunks_.size() >= extra_chunk);
            if (extra_chunk == header_exchunks_.size())
            {
                auto c = chunkmap_.FindUnused();
                if (c.IsNull())
                {
                    printf("failed to allocate an extra chunk for header\n");
                    exit(1);
                }
                header_exchunks_.push_back(c);
            }
            next_header = header_exchunks_[extra_chunk].GetPos();
            extra_chunk++;
        }
        buf.Append(next_header);
        buf.ResetPos();
        buf.Output(dma->buf, kChunkSize);

        unvme_iod_t iod = ns_wrapper_.Awrite(dma->buf, GetBlockNumFromSize(hchunk_pos),
                                             GetBlockNumFromSize(kChunkSize));
        if (!iod)
        {
            printf("failed to unvme_write");
            abort();
        }
        ctxs.push_back(Inode::AsyncIoContext{
            .iod = iod,
            .dma = dma,
            .time = ve_gettime(),
        });
        if (output_completed)
        {
            break;
        }
        hchunk_pos = next_header;
    }
    assert(extra_chunk <= header_exchunks_.size());
    for (size_t i = extra_chunk; i < header_exchunks_.size(); i++)
    {
        chunkmap_.Release(header_exchunks_[i]);
    }
    header_exchunks_.resize(extra_chunk);

    // for chunkmap
    auto chunkmap_ctxs = ChunkmapWrite();
    for (auto it = chunkmap_ctxs.begin(); it != chunkmap_ctxs.end(); ++it)
    {
        ctxs.push_back(*it);
    }

    updated_ = false;
    return ctxs;
}

bool Header::Read()
{
    bool error = false;

    vfio_dma_t *dma = ns_wrapper_.AllocChunk();
    if (!dma)
    {
        printf("allocation failure\n");
        exit(1);
    }
    header_exchunks_.clear();
    u64 hchunk_pos = kHeaderStartPos;
    HeaderBuffer buf;
    while (true)
    {
        if (ns_wrapper_.Read(dma->buf, GetBlockNumFromSize(hchunk_pos), GetBlockNumFromSize(kChunkSize)))
        {
            printf("failed to unvme_read");
            exit(1);
        }
        HeaderBuffer tmp_buf;
        tmp_buf.AppendRaw(dma->buf, kChunkSize);
        tmp_buf.ResetPos();

        if (!tmp_buf.CompareRaw(kVersionString, strlen(kVersionString)))
        {
            fprintf(stderr, "header version mismatch\n");
            error = true;
            break;
        }

        u64 next_header_pos = tmp_buf.Get<u64>();
        buf.AppendFromBuffer(tmp_buf);
        if (next_header_pos == 0)
        {
            break;
        }
        hchunk_pos = next_header_pos;
        header_exchunks_.push_back(Chunkmap::Index::CreateFromPos(hchunk_pos));
    }
    buf.ResetPos();
    if (!error)
    {
        while (!buf.Compare('\0'))
        {
            Inode *inode = Inode::CreateFromBuffer(buf, chunkmap_, ns_wrapper_);
            if (inode == nullptr)
            {
                fprintf(stderr, "io error\n");
                error = true;
                break;
            }
            inodes_.push_back(inode);
        }
        ChunkmapRead();
    }
    ns_wrapper_.Free(dma);
    return !error;
}
