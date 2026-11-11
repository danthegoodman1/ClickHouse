#include "Operations.h"

#include <Disks/DiskCommitTransactionOptions.h>
#include <Disks/DiskObjectStorage/MetadataStorages/InodeDB/InodeDBTableCoordinator.h>
#include <Disks/IDisk.h>
#include <Disks/IDiskTransaction.h>
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

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>

#include <chrono>
#include <filesystem>
#include <map>
#include <vector>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int BAD_ARGUMENTS;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsSeconds lock_acquire_timeout_for_background_operations;
    extern const MergeTreeSettingsUInt64 non_replicated_deduplication_window;
    extern const MergeTreeSettingsSeconds old_parts_lifetime;
    extern const MergeTreeSettingsSeconds temporary_directories_lifetime;
}

namespace FailPoints
{
    extern const char libch_housekeep_pause_between_batches[];
}

namespace LibchWorker
{

namespace
{

/// How many covered parts one batch removes.
constexpr size_t PARTS_PER_BATCH = 50;

Int64 nowSeconds()
{
    return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

UInt64 nowMilliseconds()
{
    return static_cast<UInt64>(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count());
}

/// Whether a claim file says another writer's merge holds the part now: `held {writer} {expiry ms} {result}` with
/// its expiry ahead. A `done {writer} {result}` claim marks a part a merge has replaced.
bool isHeld(const std::string & claim, UInt64 now_ms)
{
    std::vector<std::string> words;
    boost::split(words, claim, boost::is_any_of(" "));
    return words.size() == 4 && words[0] == "held" && parse<UInt64>(words[2]) > now_ms;
}

std::string trimSlashes(std::string path)
{
    while (path.ends_with('/'))
        path.pop_back();
    while (path.starts_with('/'))
        path.erase(0, 1);
    return path;
}

}

void housekeep(const Engine & engine, const libch::v1::Housekeep & request, libch::v1::Response & response)
{
    const auto & name = request.table();
    if (name.database().empty() || name.table().empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Housekeeping names its table and the table's database");
    auto storage = DatabaseCatalog::instance().getTable(StorageID(name.database(), name.table()), engine.globalContext());
    auto * table = typeid_cast<StorageMergeTree *>(storage.get());
    if (!table)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} is a {} table, and housekeeping takes a `MergeTree` table", storage->getStorageID().getNameForLogs(), storage->getName());

    auto & coordinator = *table->getInodeDBCoordinator();
    auto & coordination = coordinator.coordination();
    const auto disk = table->getStoragePolicy()->getDisks().front();
    const std::string table_path = trimSlashes(table->getRelativeDataPath());
    const auto settings = table->getSettings();
    const Int64 grace = (*settings)[MergeTreeSetting::old_parts_lifetime].totalSeconds();

    /// One listing of the directory says which parts another part contains.
    Stopwatch watch;
    std::vector<std::pair<String, MergeTreePartInfo>> listed;
    ActiveDataPartSet set(table->format_version);
    disk->refresh(0);
    for (auto it = disk->iterateDirectory(table->getRelativeDataPath()); it->isValid(); it->next())
    {
        if (auto info = MergeTreePartInfo::tryParsePartName(it->name(), table->format_version))
        {
            listed.emplace_back(it->name(), *info);
            set.tryAddPart(*info);
        }
    }
    if (watch.elapsedSeconds() * 2 > static_cast<double>(grace))
        throw Exception(ErrorCodes::ABORTED, "Listing the parts of {} took {:.1f} s, more than half of `old_parts_lifetime`",
            table->getStorageID().getNameForLogs(), watch.elapsedSeconds());

    /// Covered parts by the part that contains them.
    std::map<String, Strings> covered_by;
    for (const auto & [part_name, info] : listed)
    {
        const String covering = set.getContainingPart(info);
        if (!covering.empty() && covering != part_name)
            covered_by[covering].push_back(part_name);
    }

    auto & housekept = *response.mutable_housekept();
    auto remains = [&](const String & part, std::string reason)
    {
        auto & remaining = *housekept.add_remaining();
        remaining.set_part(part);
        remaining.set_reason(std::move(reason));
    };

    const Int64 now = nowSeconds();
    for (const auto & [covering, covered] : covered_by)
    {
        /// A part's directory takes its modification time from the commit that gave it its name. Readers that
        /// started before then may still read the parts it covers, so those stay for the table's grace period.
        const Int64 age = now - disk->getLastModified(fs::path(table->getRelativeDataPath()) / covering).epochTime();
        if (age < grace)
        {
            for (const auto & part : covered)
                remains(part, fmt::format("{} covered it {} s ago, and covered parts stay for {} s", covering, age, grace));
            continue;
        }

        for (size_t begin = 0; begin < covered.size(); begin += PARTS_PER_BATCH)
        {
            const size_t end = std::min(covered.size(), begin + PARTS_PER_BATCH);
            std::vector<std::string> claim_paths;
            for (size_t i = begin; i < end; ++i)
                claim_paths.push_back(fmt::format("{}/claims/{}", IInodeDBCoordination::coordPath(table_path), covered[i]));
            const auto claims = coordination.readFiles(claim_paths);

            /// The batch removes the parts and their claims only while the part that covers them is in place, and
            /// while each claim is as it was read: a claim that changed means a writer is at work on the part.
            InodeDBCommitOptions options;
            options.when.push_back({.check = InodeDBCommitOptions::Check::IsDir,
                .path = IInodeDBCoordination::treePath(fmt::format("{}/{}", table_path, covering)),
                .failure = InodeDBCommitStatus::ClaimLost});
            auto transaction = disk->createTransaction();
            Strings removing;
            const UInt64 now_ms = nowMilliseconds();
            for (size_t i = begin; i < end; ++i)
            {
                const auto & claim = claims[i - begin];
                if (claim && isHeld(claim->value, now_ms))
                {
                    remains(covered[i], "another writer's merge holds a claim on it");
                    continue;
                }
                transaction->removeRecursive(fs::path(table->getRelativeDataPath()) / covered[i] / "");
                if (claim)
                    options.ops.push_back({.kind = InodeDBCommitOptions::Kind::Remove, .path = claim_paths[i - begin],
                        .failure = InodeDBCommitStatus::ClaimLost,
                        .cond = {{.check = InodeDBCommitOptions::Check::VersionIs, .path = claim_paths[i - begin], .version = claim->version,
                            .failure = InodeDBCommitStatus::ClaimLost}}});
                removing.push_back(covered[i]);
            }
            if (removing.empty())
                continue;

            const auto outcome = transaction->tryCommit(options);
            FailPointInjection::pauseFailPoint(FailPoints::libch_housekeep_pause_between_batches);
            if (isSuccessfulOutcome(outcome))
            {
                for (const auto & part : removing)
                    housekept.add_removed(part);
            }
            else
            {
                for (const auto & part : removing)
                    remains(part, fmt::format("{} is gone, or a claim on the batch changed hands; the next housekeeping looks again", covering));
            }
        }
    }

    /// Tokens past their age, and the claims dead writers left. The table loaded no tokens, so it tells the
    /// coordinator its window before the coordinator counts what lies beyond it.
    coordinator.setWindow((*settings)[MergeTreeSetting::non_replicated_deduplication_window]);
    coordinator.expireTokens();
    coordinator.sweepClaims(1000);

    /// What an insert or a merge leaves when its worker dies before its commit: a temporary directory, which
    /// goes once it is as old as a server's background cleanup lets one get.
    {
        auto table_lock = table->lockForShare(RWLockImpl::NO_QUERY, (*settings)[MergeTreeSetting::lock_acquire_timeout_for_background_operations]);
        const size_t lifetime = static_cast<size_t>((*settings)[MergeTreeSetting::temporary_directories_lifetime].totalSeconds());
        housekept.set_temporary_directories_removed(static_cast<UInt32>(table->clearOldTemporaryDirectories(lifetime)));
    }

    response.set_status(libch::v1::OK);
}

}

}
