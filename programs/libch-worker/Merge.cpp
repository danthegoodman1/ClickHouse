#include "Operations.h"

#include <Disks/DiskObjectStorage/MetadataStorages/InodeDB/InodeDBTableCoordinator.h>
#include <Disks/IDisk.h>
#include <Disks/IStoragePolicy.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/MergeTree/ActiveDataPartSet.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageMergeTree.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/Stopwatch.h>

#include <algorithm>
#include <optional>
#include <unordered_set>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int BAD_ARGUMENTS;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsSeconds old_parts_lifetime;
}

namespace FailPoints
{
    extern const char libch_merge_unchecked_sources[];
}

namespace LibchWorker
{

namespace
{

/// What a table's directory holds, by name alone.
struct Directory
{
    /// The parts no other part contains, by partition and then by block.
    std::vector<MergeTreePartInfo> active;
    std::unordered_set<String> names;
    /// The versions of the mutation entries.
    std::vector<Int64> mutations;
    bool has_patch_parts = false;
};

/// Lists a table's directory afresh. The caller has read the block number counter before, so the listing holds
/// every part numbered at or below the counter, or a part that covers it.
Directory listDirectory(const StorageMergeTree & table)
{
    static constexpr std::string_view mutation_prefix = "mutation_";
    static constexpr std::string_view mutation_suffix = ".txt";

    Stopwatch watch;
    Directory directory;
    ActiveDataPartSet set(table.format_version);
    for (const auto & disk : table.getStoragePolicy()->getDisks())
    {
        disk->refresh(0);
        for (auto it = disk->iterateDirectory(table.getRelativeDataPath()); it->isValid(); it->next())
        {
            const String name = it->name();
            if (auto info = MergeTreePartInfo::tryParsePartName(name, table.format_version))
            {
                directory.names.insert(name);
                directory.has_patch_parts |= info->isPatch();
                set.tryAddPart(*info);
            }
            else if (name.starts_with(mutation_prefix) && name.ends_with(mutation_suffix))
            {
                Int64 version = 0;
                if (tryParse(version, name.substr(mutation_prefix.size(), name.size() - mutation_prefix.size() - mutation_suffix.size())))
                    directory.mutations.push_back(version);
            }
        }
    }
    directory.active = set.getPartInfos();

    /// As a shared writer's refresh: a listing this slow may have missed a part whose covering part's sources
    /// other writers removed while it ran.
    if (watch.elapsedSeconds() * 2 > static_cast<double>((*table.getSettings())[MergeTreeSetting::old_parts_lifetime].totalSeconds()))
        throw Exception(ErrorCodes::ABORTED, "Listing the parts of {} took {:.1f} s, more than half of `old_parts_lifetime`",
            table.getStorageID().getNameForLogs(), watch.elapsedSeconds());
    return directory;
}

/// Why a merge of named sources committed nothing.
struct Refusal
{
    libch::v1::Status status;
    std::string reason;
};

Refusal refused(std::string reason)
{
    return {libch::v1::REFUSED, std::move(reason)};
}

Refusal claimLost(std::string reason)
{
    return {libch::v1::CLAIM_LOST, std::move(reason)};
}

/// Checks a merge's named sources against the table's directory, and says why they cannot merge. Sources that
/// pass are active parts of one partition, adjacent in block order, at or below the table's counter, with no
/// mutation pending for them.
std::optional<Refusal> checkSources(StorageMergeTree & table, const std::string & partition_id, const Strings & sources)
{
    if (sources.size() < 2)
        return refused("A merge takes two or more source parts");

    bool unchecked = false;
    fiu_do_on(FailPoints::libch_merge_unchecked_sources, { unchecked = true; });
    if (unchecked)
        return std::nullopt;

    /// The counter of this epoch first, then the names: every part numbered at or below the counter is among them.
    table.prepareOperationCommits();
    const Int64 counter = table.getInodeDBCoordinator()->readCounter();
    const auto directory = listDirectory(table);

    if (directory.has_patch_parts)
        return refused(fmt::format("{} holds patch parts, which an exclusive writer applies", table.getStorageID().getNameForLogs()));

    std::vector<MergeTreePartInfo> infos;
    for (const auto & source : sources)
    {
        auto info = MergeTreePartInfo::tryParsePartName(source, table.format_version);
        if (!info)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "`{}` is no part name", source);
        if (info->getPartitionId() != partition_id)
            return refused(fmt::format("Part {} is outside partition {}", source, partition_id));
        if (!directory.names.contains(source))
            return claimLost(fmt::format("Part {} is gone", source));
        infos.push_back(*info);
    }

    /// The sources are a run of the partition's active parts in block order, with no active part between them:
    /// the merged part covers every block from the first source's to the last's.
    const auto first = std::ranges::find(directory.active, infos.front());
    if (first == directory.active.end())
        return claimLost(fmt::format("Part {} is covered by another part", sources.front()));
    for (size_t i = 0; i < infos.size(); ++i)
    {
        const auto at = first + static_cast<std::ptrdiff_t>(i);
        if (at != directory.active.end() && *at == infos[i])
            continue;
        if (std::ranges::find(directory.active, infos[i]) == directory.active.end())
            return claimLost(fmt::format("Part {} is covered by another part", sources[i]));
        return refused(fmt::format(
            "The sources are not adjacent in block order: after {} the partition holds {}, where the merge names {}",
            sources[i - 1], at == directory.active.end() ? "nothing" : at->getPartNameV1(), sources[i]));
    }

    for (size_t i = 0; i < infos.size(); ++i)
    {
        /// A part above the counter may have neighbors that have yet to commit.
        if (infos[i].max_block > counter)
            return refused(fmt::format("Part {} ends above block {}, the table's counter", sources[i], counter));
        /// An exclusive writer finishes mutations. Until it has, the parts a mutation still applies to stay apart.
        for (const Int64 mutation : directory.mutations)
            if (mutation > infos[i].getDataVersion())
                return refused(fmt::format("Mutation {} is pending for part {}, and an exclusive writer finishes mutations", mutation, sources[i]));
    }

    return std::nullopt;
}

}

void merge(const Engine & engine, const libch::v1::Merge & request, libch::v1::Response & response)
{
    const auto & name = request.table();
    if (name.database().empty() || name.table().empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "A merge names its table and the table's database");
    auto storage = DatabaseCatalog::instance().getTable(StorageID(name.database(), name.table()), engine.globalContext());
    auto * table = typeid_cast<StorageMergeTree *>(storage.get());
    if (!table)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} is a {} table, and a merge takes a `MergeTree` table", storage->getStorageID().getNameForLogs(), storage->getName());

    const Strings sources(request.sources().begin(), request.sources().end());
    if (auto refusal = checkSources(*table, request.partition_id(), sources))
    {
        response.set_status(refusal->status);
        response.set_message(std::move(refusal->reason));
        return;
    }

    auto merged = table->mergeNamedParts(sources);
    if (!merged)
    {
        response.set_status(libch::v1::CLAIM_LOST);
        response.set_message("Another writer holds a claim on a source part");
        return;
    }

    response.mutable_merged()->set_part(*merged);
    response.set_status(libch::v1::OK);
}

}

}
