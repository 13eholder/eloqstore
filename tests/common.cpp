#include "common.h"

#include <cassert>
#include <catch2/catch_test_macros.hpp>
#include <cstdlib>
#include <filesystem>

eloqstore::EloqStore *InitStore(const eloqstore::KvOptions &opts)
{
    static std::unique_ptr<eloqstore::EloqStore> eloq_store = nullptr;

    if (eloq_store)
    {
        const eloqstore::KvOptions &old_opts = eloq_store->Options();
        if (old_opts == opts)
        {
            // Fast path: reuse the existing store
            if (eloq_store->IsStopped())
            {
                eloqstore::KvError err = eloq_store->Start();
                CHECK(err == eloqstore::KvError::NoError);
            }
            return eloq_store.get();
        }
        // Required options not equal to the options of the existing store, so
        // we need to stop and remove it.
        eloq_store->Stop();
        for (const std::string &db_path : old_opts.store_path)
        {
            std::filesystem::remove_all(db_path);
        }
        if (!old_opts.cloud_store_path.empty())
        {
            std::string command = "rclone delete ";
            command.append(old_opts.cloud_store_path);
            int res = system(command.c_str());
        }
    }

    for (const std::string &db_path : opts.store_path)
    {
        std::filesystem::remove_all(db_path);
    }
    if (!opts.cloud_store_path.empty())
    {
        std::string command = "rclone delete ";
        command.append(opts.cloud_store_path);
        int res = system(command.c_str());
    }

    eloq_store = std::make_unique<eloqstore::EloqStore>(opts);
    eloqstore::KvError err = eloq_store->Start();
    CHECK(err == eloqstore::KvError::NoError);
    return eloq_store.get();
}
