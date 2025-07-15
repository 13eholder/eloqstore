#pragma once

#include "write_task.h"

namespace eloqstore
{
class ArchiveTask : public WriteTask
{
public:
    TaskType Type() const override
    {
        return TaskType::Archive;
    }
    KvError CreateArchive();
};
}  // namespace eloqstore