#include "file_gc.h"

#include <boost/algorithm/string/predicate.hpp>
#include <filesystem>
#include <fstream>
#include <unordered_set>
#include <vector>

#include "common.h"
#include "replayer.h"

namespace eloqstore
{
void GetRetainedFiles(std::unordered_set<FileId> &result,
                      const std::vector<uint64_t> &tbl,
                      uint8_t pages_per_file_shift)
{
    for (uint64_t val : tbl)
    {
        if (MappingSnapshot::IsFilePageId(val))
        {
            FilePageId fp_id = MappingSnapshot::DecodeId(val);
            FileId file_id = fp_id >> pages_per_file_shift;
            result.emplace(file_id);
        }
    }
};

FileGarbageCollector::~FileGarbageCollector()
{
    Stop();
}

void FileGarbageCollector::Start(uint16_t n_workers)
{
    assert(workers_.empty());
    workers_.reserve(n_workers);
    for (int i = 0; i < n_workers; i++)
    {
        workers_.emplace_back(&FileGarbageCollector::GCRoutine, this);
    }
    LOG(INFO) << "file garbage collector started";
}

void FileGarbageCollector::Stop()
{
    if (workers_.empty())
    {
        return;
    }
    // Send stop signal to all workers.
    std::vector<GcTask> stop_tasks;
    stop_tasks.resize(workers_.size());
    tasks_.enqueue_bulk(stop_tasks.data(), stop_tasks.size());
    for (auto &w : workers_)
    {
        w.join();
    }
    workers_.clear();
    LOG(INFO) << "file garbage collector stopped";
}

bool FileGarbageCollector::AddTask(TableIdent tbl_id,
                                   uint64_t ts,
                                   FileId max_file_id,
                                   std::unordered_set<FileId> retained_files)
{
    return tasks_.enqueue(
        GcTask(std::move(tbl_id), ts, max_file_id, std::move(retained_files)));
}

bool ReadFileContent(fs::path path, std::string &result)
{
    std::ifstream file(path, std::ios::binary);
    if (!file)
    {
        return false;
    }
    size_t size = fs::file_size(path);
    result.resize(size);
    file.read(result.data(), size);
    return true;
}

void FileGarbageCollector::GCRoutine()
{
    while (true)
    {
        GcTask req;
        tasks_.wait_dequeue(req);
        if (req.IsStopSignal())
        {
            break;
        }
        fs::path path = req.tbl_id_.StorePath(options_->store_path);
        KvError err = Execute(options_,
                              path,
                              req.mapping_ts_,
                              req.max_file_id_,
                              std::move(req.retained_files_));
    }
}

KvError FileGarbageCollector::Execute(const KvOptions *opts,
                                      const fs::path &dir_path,
                                      uint64_t mapping_ts,
                                      FileId max_file_id,
                                      std::unordered_set<FileId> retained_files)
{
    std::vector<uint64_t> archives;
    archives.reserve(opts->num_retained_archives + 1);
    std::vector<FileId> gc_data_files;
    gc_data_files.reserve(128);

    // Scan all archives and data files.
    for (auto &ent : fs::directory_iterator{dir_path})
    {
        const std::string name = ent.path().filename();
        if (boost::algorithm::ends_with(name, TmpSuffix))
        {
            // Skip temporary files.
            continue;
        }
        auto ret = ParseFileName(name);
        if (ret.first == FileNameManifest)
        {
            if (!ret.second.empty())
            {
                uint64_t ts = std::stoull(ret.second.data());
                if (ts <= mapping_ts)
                {
                    archives.emplace_back(ts);
                }
            }
        }
        else if (ret.first == FileNameData)
        {
            FileId file_id = std::stoull(ret.second.data());
            if (file_id < max_file_id)
            {
                gc_data_files.emplace_back(file_id);
            }
        }
    }

    // Clear expired archives
    if (archives.size() > opts->num_retained_archives)
    {
        std::sort(archives.begin(), archives.end(), std::greater<uint64_t>());
        while (archives.size() > opts->num_retained_archives)
        {
            uint64_t ts = archives.back();
            archives.pop_back();
            fs::path path = dir_path;
            path.append(ArchiveName(ts));
            if (fs::remove(path))
            {
                LOG(INFO) << "GC on partition " << dir_path << " removed "
                          << path;
            }
            else
            {
                LOG(ERROR) << "can not remove " << path;
            }
        }
    }

    // Get all currently used data files by archives and manifest.
    Replayer replayer(opts);
    std::string buffer;
    fs::path path = dir_path;
    path.append(FileNameManifest);
    for (uint64_t ts : archives)
    {
        path.replace_filename(ArchiveName(ts));
        if (!ReadFileContent(path, buffer))
        {
            return KvError::IoFail;
        }
        MemStoreMgr::Manifest manifest(buffer);
        KvError err = replayer.Replay(&manifest);
        if (err != KvError::NoError)
        {
            if (err == KvError::Corrupted)
            {
                bool ok = fs::remove(path);
                LOG(ERROR) << "found corrupted archive " << path
                           << ", removed=" << ok;
                continue;
            }
            return err;
        }
        GetRetainedFiles(
            retained_files, replayer.mapping_tbl_, opts->pages_per_file_shift);
    }

    // Clear unused data files by any archive.
    for (FileId file_id : gc_data_files)
    {
        if (!retained_files.contains(file_id))
        {
            path.replace_filename(DataFileName(file_id));
            if (!fs::remove(path))
            {
                LOG(ERROR) << "can not remove " << path;
            }
        }
    }
    return KvError::NoError;
}

FileGarbageCollector::GcTask::GcTask(TableIdent tbl_id,
                                     uint64_t ts,
                                     FileId max_file_id,
                                     std::unordered_set<FileId> retained_files)
    : tbl_id_(std::move(tbl_id)),
      mapping_ts_(ts),
      max_file_id_(max_file_id),
      retained_files_(std::move(retained_files))
{
}

bool FileGarbageCollector::GcTask::IsStopSignal() const
{
    return !tbl_id_.IsValid();
}
}  // namespace eloqstore