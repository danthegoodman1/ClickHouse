#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/InodeDB/InodeDBCoordination.h>

#include <Common/Logger.h>

#include <chrono>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace DB
{

class IStoragePolicy;
using StoragePolicyPtr = std::shared_ptr<const IStoragePolicy>;

/// BigHouse: what one `MergeTree` table on an `inodedb` disk keeps in InodeDB, so that its commits check it in
/// the part's batch: deduplication tokens in both writer modes, and for shared writers the block number counter,
/// the version of the table's definition and merge claims (`IInodeDBCoordination` has the layout).
///
/// Block numbers. A shared writer numbers a new part one above the counter it last read, and the part's batch
/// replaces the counter under that version. When another writer's part came first, the batch applies nothing and
/// the writer reads the counter again. A number exists only in its part's batch, so no merge range can cover a
/// number whose part has yet to commit.
///
/// Tokens. One file per block id, created with `Create` in the part's batch. An exclusive writer keeps the table's
/// `non_replicated_deduplication_window` newest tokens, as `MergeTreeDeduplicationLog` keeps its window, and removes
/// those that leave the window in the batch that adds new ones. Shared writers expire tokens by age.
///
/// Claims. A merge claims each source part before it starts: `held {writer} {expiry ms} {result}`. The merged part's
/// batch requires each claim's version and each source directory, and rewrites the claims to `done {writer} {result}`.
/// A `done` claim stays until its source directory is gone, so a writer with a stale view of the parts finds the
/// claim, or no source directory, and merges nothing over another writer's result. A `held` claim past its expiry
/// may be replaced, which fails its old holder's commit; clocks only decide when a writer tries.
class InodeDBTableCoordinator
{
public:
    /// Nullptr unless the policy is one writable `inodedb` disk.
    static std::unique_ptr<InodeDBTableCoordinator> create(const StoragePolicyPtr & policy, const std::string & relative_data_path, LoggerPtr log);

    InodeDBTableCoordinator(InodeDBCoordinationPtr disk_, std::string table_path_, LoggerPtr log_);

    bool shared() const { return disk->isSharedWriter(); }
    const std::string & writerId() const { return disk->writerId(); }
    IInodeDBCoordination & coordination() const { return *disk; }

    /// Removes the coordination files at the table's path, so that a new table starts with none.
    void removeFiles();
    /// Moves the coordination files to the table's new path, as `MergeTreeData::rename` moves its directory.
    void moveTo(const std::string & new_table_path);

    /// Block numbers, in shared mode.

    /// At startup: raises the counter to at least `at_least`, the highest block number the writer loaded.
    void reconcileCounter(Int64 at_least);
    /// Reads the counter from InodeDB and returns it.
    Int64 readCounter();
    /// One above the counter as last read: the number a new part tries.
    Int64 nextBlockNumber() const;
    /// Replaces the counter with `number` under the version last read.
    void addCounterUpdate(InodeDBCommitOptions & options, Int64 number) const;
    void counterCommitted(Int64 number, UInt64 version);
    /// Moves a part's directory to its name in the batch. Another writer's part under that name means the counter
    /// moved on.
    void addPartRename(InodeDBCommitOptions & options, const std::string & from_dir, const std::string & to_dir) const;

    /// The definition, in shared mode: commits require the version of the table's `.sql` file on the disk.
    void setDefinition(const std::string & sql_disk_path);
    /// Why commits cannot check the definition; they throw it.
    void setDefinitionError(std::string error);
    void addDefinitionCheck(InodeDBCommitOptions & options) const;

    /// Tokens.

    /// Loads the tokens and sets the window; zero turns deduplication off.
    void loadTokens(size_t window);
    void setWindow(size_t window);
    /// The block ids among `block_ids` the writer knows as taken.
    std::vector<std::string> knownTokens(const std::vector<std::string> & block_ids) const;
    /// Creates a token for each block id and, for an exclusive writer, removes those that leave the window.
    void addTokens(InodeDBCommitOptions & options, const std::vector<std::string> & block_ids, const std::string & part_name) const;
    void tokensCommitted(const std::vector<std::string> & block_ids, const std::string & part_name);
    /// The block ids whose tokens a commit found taken; the writer remembers them.
    std::vector<std::string> duplicatesOf(const InodeDBCommitOutcome & outcome);
    /// Removes the tokens of the parts `dropped` names, as `MergeTreeDeduplicationLog::dropPart` does.
    void dropPartTokens(const std::function<bool(const std::string & part_name)> & dropped);
    /// A shared writer's cleanup: removes tokens older than the disk's TTL, and the oldest beyond the window.
    size_t expireTokens();

    /// Merge claims, in shared mode.

    /// Claims every source of a merge, all or none. A claim another writer holds past its expiry is taken over.
    /// Returns the sources another writer holds, empty when this writer now holds them all.
    std::vector<std::string> claim(const std::vector<std::string> & sources, const std::string & result);
    /// The predicates and ops of the merged part's batch.
    void addMergeCommit(InodeDBCommitOptions & options, const std::string & result) const;
    void mergeCommitted(const std::string & result, UInt64 version);
    /// Ends a merge. One that did not commit gives its claims up.
    void endMerge(const std::string & result);
    /// Renews every held claim. A merge whose claim another writer took over is marked lost.
    void renewClaims();
    /// Held from reading a merge's claim versions to recording its commit, so a renewal changes none in between.
    std::unique_lock<std::mutex> lockClaimCommits() { return std::unique_lock(claim_commits_mutex); }
    bool isLost(const std::string & result) const;
    /// Each source claim of a merge: the version this writer expects, and the file's value and version now.
    std::string describeClaims(const std::string & result) const;
    /// Removes the claims on source parts whose directories this writer removed.
    void removeClaims(const std::vector<std::string> & parts);
    /// Removes other writers' claims on parts that no longer exist, and `held` ones well past their expiry,
    /// checking at most `limit` parts.
    size_t sweepClaims(size_t limit);
    /// Whether this writer merged the part away, and so removes its directory once it is old.
    bool mergedByThisWriter(const std::string & part_name) const;

private:
    std::string tablePath() const;
    std::string coordPath(const std::string & name) const;
    std::string tokenPath(const std::string & block_id) const;
    std::string claimPath(const std::string & part_name) const;
    std::string partPath(const std::string & part_name) const;
    static UInt64 nowMs();
    /// `coord/{table path}/tokens` and, for a shared writer, `claims`.
    void ensureDirectories();
    void removeTokens(const std::vector<std::string> & block_ids);

    const InodeDBCoordinationPtr disk;
    const LoggerPtr log;

    mutable std::mutex path_mutex;
    /// The table's directory on the disk, without slashes at either end.
    std::string table_path;

    mutable std::mutex mutex;
    std::mutex claim_commits_mutex;

    Int64 counter = 0;
    /// The counter file's version as last read; zero while the file is absent.
    UInt64 counter_version = 0;

    std::string definition_path;
    UInt64 definition_version = 0;
    /// Why commits cannot check the definition: it lives elsewhere than this disk.
    std::string definition_error;

    size_t window = 0;
    /// An exclusive writer's window, oldest first, with each token's part.
    std::deque<std::pair<std::string, std::string>> token_order;
    std::unordered_map<std::string, std::string> token_parts;

    struct Merge
    {
        std::vector<std::string> sources;
        /// Each source claim's version as this writer last wrote it.
        std::vector<UInt64> versions;
        bool committed = false;
        bool lost = false;
    };
    std::map<std::string, Merge> merges;
    std::unordered_set<std::string> merged_sources;
};

}
