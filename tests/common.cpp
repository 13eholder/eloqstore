#include "common.h"

#include <cassert>
#include <catch2/catch_test_macros.hpp>
#include <cstdlib>
#include <filesystem>

kvstore::EloqStore *InitStore(const kvstore::KvOptions &opts)
{
    static std::unique_ptr<kvstore::EloqStore> eloqstore = nullptr;

    if (eloqstore)
    {
        if (eloqstore->Options() == opts)
        {
            // Fast path: reuse the existing store
            if (eloqstore->IsStopped())
            {
                kvstore::KvError err = eloqstore->Start();
                CHECK(err == kvstore::KvError::NoError);
            }
            return eloqstore.get();
        }
        // Required options not equal to the options of the existing store, so
        // we need to stop and remove it.
        eloqstore->Stop();
        for (const std::string &db_path : eloqstore->Options().store_path)
        {
            std::filesystem::remove_all(db_path);
        }
    }

    for (const std::string &db_path : opts.store_path)
    {
        std::filesystem::remove_all(db_path);
    }

    eloqstore = std::make_unique<kvstore::EloqStore>(opts);
    kvstore::KvError err = eloqstore->Start();
    CHECK(err == kvstore::KvError::NoError);
    return eloqstore.get();
}
