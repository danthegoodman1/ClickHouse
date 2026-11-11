#include "Operations.h"

#include <Interpreters/DatabaseCatalog.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/StorageMergeTree.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace LibchWorker
{

void plan(const Engine & engine, const libch::v1::Plan & request, libch::v1::Response & response)
{
    const auto & name = request.table();
    if (name.database().empty() || name.table().empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "A plan names its table and the table's database");
    auto storage = DatabaseCatalog::instance().getTable(StorageID(name.database(), name.table()), engine.globalContext());
    auto * table = typeid_cast<StorageMergeTree *>(storage.get());
    if (!table)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} is a {} table, and a plan takes a `MergeTree` table", storage->getStorageID().getNameForLogs(), storage->getName());

    auto & planned = *response.mutable_planned();
    for (const auto & merge : table->planMerges(request.max_merges()))
    {
        auto & candidate = *planned.add_candidates();
        candidate.set_partition_id(MergeTreePartInfo::fromPartName(merge.sources.front(), table->format_version).getPartitionId());
        for (const auto & source : merge.sources)
            candidate.add_sources(source);
        candidate.set_rows(merge.rows);
        candidate.set_bytes(merge.bytes);
    }
    response.set_status(libch::v1::OK);
}

}

}
