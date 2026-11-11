#include <Disks/DiskObjectStorage/MetadataStorages/InodeDB/InodeDBTableCoordinator.h>

#include <Disks/IStoragePolicy.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>

#include <boost/algorithm/string/split.hpp>
#include <fmt/ranges.h>
#include <magic_enum.hpp>
#include <boost/algorithm/string/classification.hpp>

#include <algorithm>
#include <filesystem>

namespace fs = std::filesystem;

namespace ProfileEvents
{
    extern const Event InodeDBClaimsTaken;
    extern const Event InodeDBClaimsLost;
}

namespace DB
{

namespace FailPoints
{
    extern const char inodedb_shared_release_claims_at_commit[];
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

/// An InodeDB batch holds at most 10,000 ops; removals go in batches well under it.
constexpr size_t removals_per_batch = 1000;

std::string trimSlashes(std::string path)
{
    while (path.ends_with('/'))
        path.pop_back();
    while (path.starts_with('/'))
        path.erase(0, 1);
    return path;
}

InodeDBCommitOptions::Op putOp(std::string path, std::string value, InodeDBCommitOptions::Mode mode, InodeDBCommitStatus failure)
{
    return {.kind = InodeDBCommitOptions::Kind::Put, .path = std::move(path), .value = std::move(value), .mode = mode, .failure = failure};
}

InodeDBCommitOptions::Op removeOp(std::string path)
{
    return {.kind = InodeDBCommitOptions::Kind::Remove, .path = std::move(path)};
}

/// `op`, applied only while the file it changes is at `version`.
InodeDBCommitOptions::Op atVersion(InodeDBCommitOptions::Op op, UInt64 version, InodeDBCommitStatus failure)
{
    op.cond.push_back({.check = InodeDBCommitOptions::Check::VersionIs, .path = op.path, .version = version, .failure = failure});
    return op;
}

InodeDBCommitOptions::Predicate versionIs(std::string path, UInt64 version, InodeDBCommitStatus failure)
{
    return {.check = InodeDBCommitOptions::Check::VersionIs, .path = std::move(path), .version = version, .failure = failure};
}

/// A claim file: `held {writer} {expiry ms} {result}` or `done {writer} {result}`.
struct ClaimValue
{
    bool done = false;
    std::string writer;
    UInt64 expiry_ms = 0;
    std::string result;

    static ClaimValue fromText(const std::string & text)
    {
        std::vector<std::string> words;
        boost::split(words, text, boost::is_any_of(" "));
        ClaimValue value;
        if (words.size() == 4 && words[0] == "held")
        {
            value.writer = words[1];
            value.expiry_ms = parse<UInt64>(words[2]);
            value.result = words[3];
            return value;
        }
        if (words.size() == 3 && words[0] == "done")
        {
            value.done = true;
            value.writer = words[1];
            value.result = words[2];
            return value;
        }
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Malformed merge claim `{}`", text);
    }
};

std::string heldClaim(const std::string & writer, UInt64 expiry_ms, const std::string & result)
{
    return fmt::format("held {} {} {}", writer, expiry_ms, result);
}

std::string doneClaim(const std::string & writer, const std::string & result)
{
    return fmt::format("done {} {}", writer, result);
}

}

std::unique_ptr<InodeDBTableCoordinator> InodeDBTableCoordinator::create(const StoragePolicyPtr & policy, const std::string & relative_data_path, LoggerPtr log)
{
    /// A replica's read-only disk commits nothing, and a replica inserts nothing.
    auto disks = policy->getDisks();
    if (disks.size() != 1 || disks.front()->isReadOnly())
        return nullptr;
    auto coordination = getInodeDBCoordination(disks.front());
    if (!coordination)
        return nullptr;
    return std::make_unique<InodeDBTableCoordinator>(std::move(coordination), relative_data_path, std::move(log));
}

InodeDBTableCoordinator::InodeDBTableCoordinator(InodeDBCoordinationPtr disk_, std::string table_path_, LoggerPtr log_)
    : disk(std::move(disk_))
    , log(std::move(log_))
    , table_path(trimSlashes(std::move(table_path_)))
{
    if (table_path.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A table on an `inodedb` disk has no data path to keep its coordination files under");
}

std::string InodeDBTableCoordinator::tablePath() const
{
    std::lock_guard lock(path_mutex);
    return table_path;
}

void InodeDBTableCoordinator::removeFiles()
{
    InodeDBCommitOptions options;
    options.ops.push_back({.kind = InodeDBCommitOptions::Kind::Remove, .path = IInodeDBCoordination::coordPath(tablePath())});
    if (auto outcome = disk->commit(options); outcome.status != InodeDBCommitStatus::Committed)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Removing the coordination files of `{}` failed: {}", tablePath(),
            magic_enum::enum_name(outcome.status));
}

void InodeDBTableCoordinator::moveTo(const std::string & new_table_path)
{
    auto to = trimSlashes(new_table_path);
    const auto from_dir = IInodeDBCoordination::coordPath(tablePath());
    const auto to_dir = IInodeDBCoordination::coordPath(to);
    bool keeps_files = shared();
    {
        std::lock_guard lock(mutex);
        keeps_files = keeps_files || window != 0;
    }

    /// Files a dropped table left at the new path go first. A table without deduplication keeps nothing.
    InodeDBCommitOptions options;
    options.ops.push_back({.kind = InodeDBCommitOptions::Kind::Remove, .path = to_dir});
    if (keeps_files)
    {
        options.ops.push_back({.kind = InodeDBCommitOptions::Kind::Mkdir, .path = from_dir});
        options.ops.push_back({.kind = InodeDBCommitOptions::Kind::Mkdir, .path = std::filesystem::path(to_dir).parent_path().string()});
        options.ops.push_back({.kind = InodeDBCommitOptions::Kind::Rename, .path = from_dir, .to = to_dir});
    }
    else
        options.ops.push_back({.kind = InodeDBCommitOptions::Kind::Remove, .path = from_dir});
    if (auto outcome = disk->commit(options); outcome.status != InodeDBCommitStatus::Committed)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Moving the coordination files of `{}` to `{}` failed: {}", tablePath(), to,
            magic_enum::enum_name(outcome.status));

    LOG_DEBUG(log, "Moved the coordination files of `{}` to `{}`", tablePath(), to);
    std::lock_guard lock(path_mutex);
    table_path = std::move(to);
}

std::string InodeDBTableCoordinator::coordPath(const std::string & name) const
{
    return fmt::format("{}/{}", IInodeDBCoordination::coordPath(tablePath()), name);
}

std::string InodeDBTableCoordinator::tokenPath(const std::string & block_id) const
{
    return coordPath("tokens/" + escapeForFileName(block_id));
}

std::string InodeDBTableCoordinator::claimPath(const std::string & part_name) const
{
    return coordPath("claims/" + part_name);
}

std::string InodeDBTableCoordinator::partPath(const std::string & part_name) const
{
    return IInodeDBCoordination::treePath(fmt::format("{}/{}", tablePath(), part_name));
}

UInt64 InodeDBTableCoordinator::nowMs()
{
    return static_cast<UInt64>(std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch()).count());
}

void InodeDBTableCoordinator::ensureDirectories()
{
    InodeDBCommitOptions options;
    options.ops.push_back({.kind = InodeDBCommitOptions::Kind::Mkdir, .path = coordPath("tokens")});
    if (shared())
        options.ops.push_back({.kind = InodeDBCommitOptions::Kind::Mkdir, .path = coordPath("claims")});
    auto outcome = disk->commit(options);
    if (outcome.status != InodeDBCommitStatus::Committed)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Creating the coordination directories of `{}` failed", tablePath());
}

void InodeDBTableCoordinator::reconcileCounter(Int64 at_least)
{
    ensureDirectories();
    const std::string path = coordPath("block_number");
    while (true)
    {
        auto file = disk->readFile(path);
        const Int64 value = file ? parse<Int64>(file->value) : 0;
        if (file && value >= at_least)
        {
            std::lock_guard lock(mutex);
            counter = value;
            counter_version = file->version;
            LOG_DEBUG(log, "The block number counter of `{}` is {}", tablePath(), value);
            return;
        }

        InodeDBCommitOptions options;
        auto op = putOp(path, toString(at_least), file ? InodeDBCommitOptions::Mode::Replace : InodeDBCommitOptions::Mode::Create,
            InodeDBCommitStatus::CounterConflict);
        options.ops.push_back(file ? atVersion(std::move(op), file->version, InodeDBCommitStatus::CounterConflict) : std::move(op));
        auto outcome = disk->commit(options);
        if (outcome.status == InodeDBCommitStatus::Committed)
        {
            std::lock_guard lock(mutex);
            counter = at_least;
            counter_version = outcome.version;
            LOG_INFO(log, "Raised the block number counter of `{}` from {} to {}", tablePath(), value, at_least);
            return;
        }
        /// Another writer moved the counter meanwhile; read it again.
    }
}

Int64 InodeDBTableCoordinator::readCounter()
{
    auto file = disk->readFile(coordPath("block_number"));
    std::lock_guard lock(mutex);
    counter = file ? parse<Int64>(file->value) : 0;
    counter_version = file ? file->version : 0;
    return counter;
}

bool InodeDBTableCoordinator::readCounterOfThisEpoch()
{
    auto file = disk->readFile(coordPath("block_number"));
    std::lock_guard lock(mutex);
    counter = file ? parse<Int64>(file->value) : 0;
    counter_version = file ? file->version : 0;
    return file && file->version > disk->ownerVersion();
}

Int64 InodeDBTableCoordinator::nextBlockNumber() const
{
    std::lock_guard lock(mutex);
    return counter + 1;
}

void InodeDBTableCoordinator::addCounterUpdate(InodeDBCommitOptions & options, Int64 number) const
{
    std::lock_guard lock(mutex);
    const std::string path = coordPath("block_number");
    if (counter_version == 0)
    {
        options.ops.push_back(putOp(path, toString(number), InodeDBCommitOptions::Mode::Create, InodeDBCommitStatus::CounterConflict));
        return;
    }
    options.ops.push_back(atVersion(putOp(path, toString(number), InodeDBCommitOptions::Mode::Replace, InodeDBCommitStatus::CounterConflict),
        counter_version, InodeDBCommitStatus::CounterConflict));
}

void InodeDBTableCoordinator::counterCommitted(Int64 number, UInt64 version)
{
    std::lock_guard lock(mutex);
    counter = number;
    counter_version = version;
}

void InodeDBTableCoordinator::addPartRename(InodeDBCommitOptions & options, const std::string & from_dir, const std::string & to_dir) const
{
    options.ops.push_back({.kind = InodeDBCommitOptions::Kind::Rename, .path = partPath(from_dir), .to = partPath(to_dir),
        .failure = InodeDBCommitStatus::CounterConflict});
}

void InodeDBTableCoordinator::setDefinition(const std::string & sql_disk_path)
{
    auto version = disk->versionOf(sql_disk_path);
    std::lock_guard lock(mutex);
    if (!version)
    {
        definition_error = fmt::format("the definition of the table at `{}` is not on its disk at `{}`", tablePath(), sql_disk_path);
        return;
    }
    definition_path = IInodeDBCoordination::treePath(sql_disk_path);
    definition_version = *version;
    definition_error.clear();
}

void InodeDBTableCoordinator::setDefinitionError(std::string error)
{
    std::lock_guard lock(mutex);
    definition_error = std::move(error);
}

void InodeDBTableCoordinator::addDefinitionCheck(InodeDBCommitOptions & options) const
{
    std::lock_guard lock(mutex);
    if (!definition_error.empty())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "A shared writer commits only to tables whose definitions live on the same `inodedb` disk, and {}", definition_error);
    if (definition_path.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A shared writer commits to `{}` before it read the table's definition", tablePath());
    options.when.push_back(versionIs(definition_path, definition_version, InodeDBCommitStatus::SchemaChanged));
}

void InodeDBTableCoordinator::loadTokens(size_t window_)
{
    /// A table without deduplication on an exclusive writer keeps nothing in InodeDB.
    if (window_ == 0 && !shared())
    {
        std::lock_guard lock(mutex);
        window = 0;
        token_order.clear();
        token_parts.clear();
        return;
    }
    ensureDirectories();
    auto files = disk->listFiles(coordPath("tokens"));
    std::ranges::sort(files, {}, [](const auto & file) { return file.second.version; });

    std::vector<std::string> beyond;
    {
        std::lock_guard lock(mutex);
        window = window_;
        token_order.clear();
        token_parts.clear();
        const size_t keep = std::min(files.size(), window);
        for (size_t i = 0; i < files.size(); ++i)
        {
            auto block_id = unescapeForFileName(files[i].first);
            if (i < files.size() - keep)
            {
                /// A shared writer leaves other writers' tokens to their age.
                if (!shared())
                    beyond.push_back(std::move(block_id));
                continue;
            }
            token_parts[block_id] = files[i].second.value;
            token_order.emplace_back(std::move(block_id), files[i].second.value);
        }
    }
    LOG_DEBUG(log, "Loaded {} deduplication tokens of `{}`, {} beyond the window of {}", files.size(), tablePath(), beyond.size(), window_);
    removeTokens(beyond);
}

void InodeDBTableCoordinator::setWindow(size_t window_)
{
    if (window_ > 0)
        ensureDirectories();
    std::vector<std::string> beyond;
    {
        std::lock_guard lock(mutex);
        window = window_;
        /// A smaller window forgets the oldest tokens at once, as `MergeTreeDeduplicationLog::setDeduplicationWindowSize` does.
        while (token_order.size() > window)
        {
            token_parts.erase(token_order.front().first);
            beyond.push_back(std::move(token_order.front().first));
            token_order.pop_front();
        }
    }
    /// A shared writer leaves its tokens to their age.
    if (!shared())
        removeTokens(beyond);
}

void InodeDBTableCoordinator::addDirectories(InodeDBCommitOptions & options) const
{
    options.ops.push_back({.kind = InodeDBCommitOptions::Kind::Mkdir, .path = coordPath("tokens")});
    if (shared())
        options.ops.push_back({.kind = InodeDBCommitOptions::Kind::Mkdir, .path = coordPath("claims")});
}

std::vector<std::string> InodeDBTableCoordinator::knownTokens(const std::vector<std::string> & block_ids) const
{
    std::lock_guard lock(mutex);
    std::vector<std::string> known;
    for (const auto & block_id : block_ids)
        if (token_parts.contains(block_id))
            known.push_back(block_id);
    return known;
}

void InodeDBTableCoordinator::addTokens(InodeDBCommitOptions & options, const std::vector<std::string> & block_ids, const std::string & part_name) const
{
    std::lock_guard lock(mutex);
    for (const auto & block_id : block_ids)
        options.ops.push_back(putOp(tokenPath(block_id), part_name, InodeDBCommitOptions::Mode::Create, InodeDBCommitStatus::DuplicateToken));

    /// An exclusive writer's window holds the newest tokens; those it pushes out go in the same batch.
    if (shared())
        return;
    const size_t total = token_order.size() + block_ids.size();
    const size_t evicted = total > window ? std::min(token_order.size(), total - window) : 0;
    for (size_t i = 0; i < evicted; ++i)
        options.ops.push_back(removeOp(tokenPath(token_order[i].first)));
}

void InodeDBTableCoordinator::tokensCommitted(const std::vector<std::string> & block_ids, const std::string & part_name)
{
    std::lock_guard lock(mutex);
    for (const auto & block_id : block_ids)
    {
        token_parts[block_id] = part_name;
        token_order.emplace_back(block_id, part_name);
    }
    /// A shared writer's window only remembers its own recent tokens; other writers' it learns from commits.
    while (token_order.size() > window)
    {
        token_parts.erase(token_order.front().first);
        token_order.pop_front();
    }
}

std::vector<std::string> InodeDBTableCoordinator::duplicatesOf(const InodeDBCommitOutcome & outcome)
{
    const std::string prefix = coordPath("tokens/");
    std::vector<std::string> block_ids;
    std::lock_guard lock(mutex);
    for (const auto & path : outcome.failed_paths)
    {
        if (!path.starts_with(prefix))
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A duplicate token at `{}` lies outside `{}`", path, prefix);
        auto block_id = unescapeForFileName(path.substr(prefix.size()));
        /// Remembered with no part: another writer's, or one this writer's window lost.
        token_parts.emplace(block_id, "");
        block_ids.push_back(std::move(block_id));
    }
    return block_ids;
}

void InodeDBTableCoordinator::removeTokens(const std::vector<std::string> & block_ids)
{
    for (size_t begin = 0; begin < block_ids.size(); begin += removals_per_batch)
    {
        InodeDBCommitOptions options;
        for (size_t i = begin; i < std::min(block_ids.size(), begin + removals_per_batch); ++i)
            options.ops.push_back(removeOp(tokenPath(block_ids[i])));
        disk->commit(options);
    }
}

void InodeDBTableCoordinator::dropPartTokens(const std::function<bool(const std::string &)> & dropped)
{
    std::vector<std::string> block_ids;
    {
        std::lock_guard lock(mutex);
        if (window == 0)
            return;
        for (auto it = token_order.begin(); it != token_order.end();)
        {
            if (!it->second.empty() && dropped(it->second))
            {
                token_parts.erase(it->first);
                block_ids.push_back(it->first);
                it = token_order.erase(it);
            }
            else
                ++it;
        }
    }
    removeTokens(block_ids);
}

size_t InodeDBTableCoordinator::expireTokens()
{
    auto files = disk->listFiles(coordPath("tokens"));
    std::ranges::sort(files, {}, [](const auto & file) { return file.second.version; });

    size_t limit = 0;
    {
        std::lock_guard lock(mutex);
        limit = window;
    }
    const UInt64 oldest_kept_ms = nowMs() - static_cast<UInt64>(std::chrono::duration_cast<std::chrono::milliseconds>(disk->tokenTTL()).count());
    std::vector<std::string> expired;
    for (size_t i = 0; i < files.size(); ++i)
        if (files[i].second.mtime_ms < oldest_kept_ms || i + limit < files.size())
            expired.push_back(unescapeForFileName(files[i].first));
    removeTokens(expired);
    return expired.size();
}

std::vector<std::string> InodeDBTableCoordinator::claim(const std::vector<std::string> & sources, const std::string & result)
{
    std::vector<std::string> paths;
    paths.reserve(sources.size());
    for (const auto & source : sources)
        paths.push_back(claimPath(source));
    auto claims = disk->readFiles(paths);

    const UInt64 now = nowMs();
    const UInt64 expiry = now + static_cast<UInt64>(disk->claimTTL().count());
    const std::string value = heldClaim(writerId(), expiry, result);

    InodeDBCommitOptions options;
    std::vector<std::string> held_elsewhere;
    size_t taken_over = 0;
    for (size_t i = 0; i < sources.size(); ++i)
    {
        options.when.push_back({.check = InodeDBCommitOptions::Check::IsDir, .path = partPath(sources[i]), .failure = InodeDBCommitStatus::ClaimLost});
        if (!claims[i])
        {
            options.ops.push_back(putOp(paths[i], value, InodeDBCommitOptions::Mode::Create, InodeDBCommitStatus::ClaimLost));
            continue;
        }
        auto current = ClaimValue::fromText(claims[i]->value);
        /// A control plane may hold parts for this writer by naming it in their claims.
        const bool for_this_writer = current.writer == writerId();
        if (current.done || (current.expiry_ms > now && !for_this_writer))
        {
            held_elsewhere.push_back(sources[i]);
            continue;
        }
        taken_over += !for_this_writer;
        options.ops.push_back(atVersion(putOp(paths[i], value, InodeDBCommitOptions::Mode::Replace, InodeDBCommitStatus::ClaimLost),
            claims[i]->version, InodeDBCommitStatus::ClaimLost));
    }
    if (!held_elsewhere.empty())
        return held_elsewhere;

    auto outcome = disk->commit(options);
    if (outcome.status != InodeDBCommitStatus::Committed)
    {
        /// Another writer claimed a source first, or a source is gone.
        LOG_INFO(log, "Claiming the sources of {} in `{}` failed: {} at {}", result, tablePath(), magic_enum::enum_name(outcome.status),
            fmt::join(outcome.failed_paths, ", "));
        std::vector<std::string> lost;
        for (const auto & path : outcome.failed_paths)
            lost.push_back(fs::path(path).filename());
        return lost.empty() ? sources : lost;
    }

    ProfileEvents::increment(ProfileEvents::InodeDBClaimsTaken, sources.size());
    if (taken_over)
        LOG_INFO(log, "Took over {} expired merge claims of `{}` to merge {}", taken_over, tablePath(), result);

    std::lock_guard lock(mutex);
    merges[result] = Merge{.sources = sources, .versions = std::vector<UInt64>(sources.size(), outcome.version)};
    return {};
}

void InodeDBTableCoordinator::addMergeCommit(InodeDBCommitOptions & options, const std::string & result) const
{
    {
        std::lock_guard lock(mutex);
        auto it = merges.find(result);
        if (it == merges.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A shared writer commits merge {} of `{}` without claims on its sources", result, tablePath());
        const auto & merge = it->second;
        for (size_t i = 0; i < merge.sources.size(); ++i)
        {
            const auto path = claimPath(merge.sources[i]);
            options.when.push_back({.check = InodeDBCommitOptions::Check::IsDir, .path = partPath(merge.sources[i]), .failure = InodeDBCommitStatus::ClaimLost});
            bool release = false;
            fiu_do_on(FailPoints::inodedb_shared_release_claims_at_commit, { release = true; });
            auto op = release ? removeOp(path) : putOp(path, doneClaim(writerId(), result), InodeDBCommitOptions::Mode::Replace, InodeDBCommitStatus::ClaimLost);
            options.ops.push_back(atVersion(std::move(op), merge.versions[i], InodeDBCommitStatus::ClaimLost));
        }
    }
    addDefinitionCheck(options);
}

void InodeDBTableCoordinator::mergeCommitted(const std::string & result, UInt64 version)
{
    std::lock_guard lock(mutex);
    auto & merge = merges.at(result);
    merge.committed = true;
    std::ranges::fill(merge.versions, version);
    for (const auto & source : merge.sources)
        merged_sources.insert(source);
}

void InodeDBTableCoordinator::endMerge(const std::string & result)
{
    Merge merge;
    {
        std::lock_guard lock(mutex);
        auto it = merges.find(result);
        if (it == merges.end())
            return;
        merge = std::move(it->second);
        merges.erase(it);
    }
    if (merge.committed || merge.lost)
        return;

    /// Give the claims up, unless another writer took one over.
    InodeDBCommitOptions options;
    for (size_t i = 0; i < merge.sources.size(); ++i)
    {
        options.ops.push_back(atVersion(removeOp(claimPath(merge.sources[i])), merge.versions[i], InodeDBCommitStatus::ClaimLost));
    }
    try
    {
        auto outcome = disk->commit(options);
        if (outcome.status != InodeDBCommitStatus::Committed)
            LOG_INFO(log, "Another writer took over a claim of merge {} of `{}`", result, tablePath());
    }
    catch (...)
    {
        /// The claims expire.
        tryLogCurrentException(log, fmt::format("Giving up the claims of merge {} of `{}`", result, tablePath()));
    }
}

void InodeDBTableCoordinator::renewClaims()
{
    std::vector<std::pair<std::string, Merge>> held;
    {
        std::lock_guard lock(mutex);
        for (const auto & [result, merge] : merges)
            if (!merge.committed && !merge.lost)
                held.emplace_back(result, merge);
    }

    for (const auto & [result, merge] : held)
    {
        std::lock_guard commits_lock(claim_commits_mutex);
        {
            std::lock_guard lock(mutex);
            auto it = merges.find(result);
            if (it == merges.end() || it->second.committed)
                continue;
        }
        const UInt64 expiry = nowMs() + static_cast<UInt64>(disk->claimTTL().count());
        InodeDBCommitOptions options;
        for (size_t i = 0; i < merge.sources.size(); ++i)
        {
            options.ops.push_back(atVersion(
                putOp(claimPath(merge.sources[i]), heldClaim(writerId(), expiry, result), InodeDBCommitOptions::Mode::Replace, InodeDBCommitStatus::ClaimLost),
                merge.versions[i], InodeDBCommitStatus::ClaimLost));
        }
        auto outcome = disk->commit(options);

        std::lock_guard lock(mutex);
        auto it = merges.find(result);
        /// The merge committed or ended while the renewal ran: it no longer needs the claims.
        if (it == merges.end() || it->second.committed)
            continue;
        if (outcome.status == InodeDBCommitStatus::Committed)
        {
            std::ranges::fill(it->second.versions, outcome.version);
            continue;
        }
        it->second.lost = true;
        ProfileEvents::increment(ProfileEvents::InodeDBClaimsLost);
        LOG_WARNING(log, "Merge {} of `{}` lost its claims to another writer", result, tablePath());
    }
}

std::string InodeDBTableCoordinator::describeClaims(const std::string & result) const
{
    Merge merge;
    {
        std::lock_guard lock(mutex);
        auto it = merges.find(result);
        if (it == merges.end())
            return "no claims";
        merge = it->second;
    }
    std::vector<std::string> paths;
    for (const auto & source : merge.sources)
        paths.push_back(claimPath(source));
    auto files = disk->readFiles(paths);
    std::vector<std::string> parts;
    for (size_t i = 0; i < merge.sources.size(); ++i)
        parts.push_back(fmt::format("{}: expected version {}, found {}", merge.sources[i], merge.versions[i],
            files[i] ? fmt::format("`{}` at version {}", files[i]->value, files[i]->version) : "nothing"));
    return fmt::format("{}", fmt::join(parts, "; "));
}

bool InodeDBTableCoordinator::isLost(const std::string & result) const
{
    std::lock_guard lock(mutex);
    auto it = merges.find(result);
    return it != merges.end() && it->second.lost;
}

void InodeDBTableCoordinator::removeClaims(const std::vector<std::string> & parts)
{
    for (size_t begin = 0; begin < parts.size(); begin += removals_per_batch)
    {
        InodeDBCommitOptions options;
        for (size_t i = begin; i < std::min(parts.size(), begin + removals_per_batch); ++i)
            options.ops.push_back(removeOp(claimPath(parts[i])));
        disk->commit(options);
    }
    std::lock_guard lock(mutex);
    for (const auto & part : parts)
        merged_sources.erase(part);
}

size_t InodeDBTableCoordinator::sweepClaims(size_t limit)
{
    auto claims = disk->listFiles(coordPath("claims"));
    const UInt64 now = nowMs();
    const UInt64 ttl = static_cast<UInt64>(disk->claimTTL().count());

    InodeDBCommitOptions options;
    size_t checked = 0;
    for (const auto & [part_name, file] : claims)
    {
        if (checked == limit)
            break;
        auto claim_value = ClaimValue::fromText(file.value);
        /// A claim of this writer's own merges ends with the merge.
        if (claim_value.writer == writerId())
            continue;
        /// A held claim stays until well past its expiry, so a slow holder that renews late finds it.
        if (!claim_value.done && claim_value.expiry_ms + ttl > now)
            continue;
        ++checked;
        if (claim_value.done && disk->versionOf(fmt::format("{}/{}", tablePath(), part_name)))
            continue;
        options.ops.push_back(atVersion(removeOp(claimPath(part_name)), file.version, InodeDBCommitStatus::ClaimLost));
    }
    if (options.ops.empty())
        return 0;
    auto outcome = disk->commit(options);
    return outcome.status == InodeDBCommitStatus::Committed ? options.ops.size() : 0;
}

std::vector<std::string> InodeDBTableCoordinator::partsHeldByOthers() const
{
    const UInt64 now = nowMs();
    std::vector<std::string> held;
    for (const auto & [part_name, file] : disk->listFiles(coordPath("claims")))
    {
        const auto claim = ClaimValue::fromText(file.value);
        if (!claim.done && claim.expiry_ms > now && claim.writer != writerId())
            held.push_back(part_name);
    }
    return held;
}

bool InodeDBTableCoordinator::mergedByThisWriter(const std::string & part_name) const
{
    std::lock_guard lock(mutex);
    return merged_sources.contains(part_name);
}

}
