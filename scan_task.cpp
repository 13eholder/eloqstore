#include "scan_task.h"

#include <cassert>
#include <cstdint>
#include <memory>

#include "error.h"
#include "index_page_manager.h"
#include "page_mapper.h"

namespace kvstore
{
ScanTask::ScanTask() : iter_(nullptr, Options())
{
}

KvError ScanTask::Scan(const TableIdent &tbl_id,
                       std::string_view begin_key,
                       std::string_view end_key,
                       bool begin_inclusive,
                       size_t page_entries,
                       size_t page_size,
                       std::vector<KvEntry> &result,
                       bool &has_remaining)
{
    result.clear();
    has_remaining = false;
    size_t result_size = 0;

    auto [meta, err] = index_mgr->FindRoot(tbl_id);
    CHECK_KV_ERR(err);
    if (meta->root_page_ == nullptr)
    {
        return KvError::NotFound;
    }
    auto mapping = meta->mapper_->GetMappingSnapshot();

    uint32_t page_id;
    err = index_mgr->SeekIndex(
        mapping.get(), tbl_id, meta->root_page_, begin_key, page_id);
    CHECK_KV_ERR(err);
    assert(page_id != UINT32_MAX);
    uint32_t file_page = mapping->ToFilePage(page_id);
    auto [page, err_load] = LoadDataPage(tbl_id, page_id, file_page);
    CHECK_KV_ERR(err_load);
    data_page_ = std::move(page);

    iter_.Reset(&data_page_, Options()->data_page_size);
    iter_.Seek(begin_key);
    if (iter_.Key().empty() && (err = Next(mapping.get())) != KvError::NoError)
    {
        goto End;
    }
    if (!begin_inclusive && Comp()->Compare(iter_.Key(), begin_key) == 0 &&
        (err = Next(mapping.get())) != KvError::NoError)
    {
        goto End;
    }

    while (end_key.empty() || Comp()->Compare(iter_.Key(), end_key) < 0)
    {
        // Check entries number limit.
        if (result.size() == page_entries)
        {
            has_remaining = true;
            break;
        }

        // Fetch value
        std::string value;
        if (iter_.IsOverflow())
        {
            auto ret = GetOverflowValue(tbl_id, mapping.get(), iter_.Value());
            err = ret.second;
            if (err != KvError::NoError)
            {
                assert(err != KvError::EndOfFile);
                break;
            }
            value = std::move(ret.first);
        }
        else
        {
            value = iter_.Value();
        }

        // Check result size limit.
        size_t entry_size =
            iter_.Key().size() + value.size() + sizeof(uint64_t);
        if (result_size > 0 && result_size + entry_size > page_size)
        {
            has_remaining = true;
            break;
        }
        result_size += entry_size;

        result.emplace_back(iter_.Key(), std::move(value), iter_.Timestamp());

        err = Next(mapping.get());
        if (err != KvError::NoError)
        {
            break;
        }
    }
End:
    data_page_.Clear();
    return err == KvError::EndOfFile ? KvError::NoError : err;
}

KvError ScanTask::Next(MappingSnapshot *m)
{
    if (!iter_.HasNext())
    {
        uint32_t page_id = data_page_.NextPageId();
        data_page_.Clear();
        if (page_id == UINT32_MAX)
        {
            // EndOfFile will just break the scan process
            return KvError::EndOfFile;
        }
        uint32_t file_page = m->ToFilePage(page_id);
        auto [page, err] = LoadDataPage(*m->tbl_ident_, page_id, file_page);
        CHECK_KV_ERR(err);
        data_page_ = std::move(page);

        iter_.Reset(&data_page_, Options()->data_page_size);
        assert(iter_.HasNext());
    }
    iter_.Next();
    assert(!iter_.Key().empty());
    return KvError::NoError;
}

}  // namespace kvstore