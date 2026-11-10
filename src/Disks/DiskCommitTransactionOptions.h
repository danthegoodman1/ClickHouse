#pragma once

#include <variant>
#include <Common/Exception.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperWithFaultInjection.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

using NoCommitOptions = std::monostate;

template <typename ZooKeeperConnection>
struct MetaInKeeperCommitOptions
{
    bool need_rollback_blobs = true;
    bool may_retry = false;
    ZooKeeperConnection * zookeeper = nullptr;
    Coordination::Requests additional_requests;
};

/// BigHouse: why a transaction on an `inodedb` disk committed or not. Each op and predicate of
/// `InodeDBCommitOptions` names the status its failure means. A fenced writer's commit throws
/// `TABLE_IS_READ_ONLY` instead, as every commit of a fenced writer does, and a revoked key's throws the
/// authentication error InodeDB returns.
enum class InodeDBCommitStatus : uint8_t
{
    Committed,
    /// A table's block number counter changed since the writer read it.
    CounterConflict,
    /// A deduplication token exists.
    DuplicateToken,
    /// The table's definition changed since the writer loaded it.
    SchemaChanged,
    /// A merge claim changed hands, or a merge's source part is gone.
    ClaimLost,
};

/// BigHouse: ops and predicates that join the batch of a transaction on an `inodedb` disk. Paths are relative to
/// the root of the disk's InodeDB key, as `coord/{table path}/block_number`.
struct InodeDBCommitOptions
{
    enum class Mode : uint8_t
    {
        Create,
        Replace,
        Upsert,
    };

    enum class Check : uint8_t
    {
        Exists,
        Absent,
        IsDir,
        VersionIs,
    };

    /// A predicate on the state an op sees: the state the batch's earlier ops left. As a batch's `when`, it joins
    /// every op, so it must not name a file the batch changes; such a file's version goes in that op's `cond`.
    struct Predicate
    {
        Check check = Check::Exists;
        std::string path;
        UInt64 version = 0;
        InodeDBCommitStatus failure = InodeDBCommitStatus::Committed;
    };

    enum class Kind : uint8_t
    {
        /// Writes `value` in `mode`.
        Put,
        /// Removes a file or a directory with everything under it, and succeeds when nothing is there.
        Remove,
        /// Creates a directory and its parents, and succeeds when the directory exists.
        Mkdir,
        /// Moves `path` to `to`, which must not exist.
        Rename,
    };

    struct Op
    {
        Kind kind = Kind::Put;
        std::string path;
        std::string value{};
        std::string to{};
        Mode mode = Mode::Upsert;
        /// What a skip of this op means: `Exists` for `Create`, `NotFound` for `Replace`.
        InodeDBCommitStatus failure = InodeDBCommitStatus::Committed;
        /// Predicates of this op alone.
        std::vector<Predicate> cond{};
    };

    std::vector<Op> ops;
    std::vector<Predicate> when;
};

struct InodeDBCommitOutcome
{
    InodeDBCommitStatus status = InodeDBCommitStatus::Committed;
    /// The commit's version; every file the batch wrote now has it.
    UInt64 version = 0;
    /// The paths of the ops and predicates whose failure gave `status`.
    std::vector<std::string> failed_paths{};
};

using TransactionCommitOptionsVariant = std::variant<
    NoCommitOptions,
    MetaInKeeperCommitOptions<zkutil::ZooKeeper>,
    MetaInKeeperCommitOptions<ZooKeeperWithFaultInjection>,
    InodeDBCommitOptions>;

inline String getFirstFailedPath(Coordination::Error code, const Coordination::Responses & responses,
                                 size_t first_additional_request_idx, const Coordination::Requests & additional_ops)
{
    size_t failed_op_index = zkutil::getFailedOpIndex(code, responses);
    if (failed_op_index < first_additional_request_idx)
        return "<in disk transaction>";
    return additional_ops.at(failed_op_index - first_additional_request_idx)->getPath();
}

struct MetaInKeeperCommitOutcome
{
    Coordination::Error code{};

    /// Responses for the keeper multi-op performed by this transaction.
    /// The order is: disk's internal ops, then `additional_requests`.
    /// Populated only if isUserError(code).
    Coordination::Responses responses;
    size_t first_additional_request_idx = 0;

    size_t additionalOpIdx(size_t idx) const { return first_additional_request_idx + idx; }
};

using TransactionCommitOutcomeVariant = std::variant<
    bool,
    MetaInKeeperCommitOutcome,
    InodeDBCommitOutcome>;

inline bool isSuccessfulOutcome(const TransactionCommitOutcomeVariant & outcome)
{
    if (const auto * success = std::get_if<bool>(&outcome); success)
        return *success;
    else if (const auto * result = std::get_if<MetaInKeeperCommitOutcome>(&outcome); result)
        return result->code == Coordination::Error::ZOK;
    else if (const auto * inodedb = std::get_if<InodeDBCommitOutcome>(&outcome); inodedb)
        return inodedb->status == InodeDBCommitStatus::Committed;

    UNREACHABLE();
}

inline bool needRollbackBlobs(const TransactionCommitOptionsVariant & options)
{
    if (const auto * with_faults = std::get_if<MetaInKeeperCommitOptions<ZooKeeperWithFaultInjection>>(&options); with_faults)
        return with_faults->need_rollback_blobs;
    else if (const auto * just_keeper = std::get_if<MetaInKeeperCommitOptions<zkutil::ZooKeeper>>(&options); just_keeper)
        return just_keeper->need_rollback_blobs;
    /// BigHouse: the caller retries a failed `inodedb` commit with the same files, or undoes the transaction.
    else if (std::holds_alternative<InodeDBCommitOptions>(options))
        return false;

    return true;
}

inline bool canRollbackBlobs(const TransactionCommitOptionsVariant & options, const TransactionCommitOutcomeVariant & outcome)
{
    if (needRollbackBlobs(options))
    {
        if (const auto * result = std::get_if<MetaInKeeperCommitOutcome>(&outcome); result)
        {
            return !Coordination::isHardwareError(result->code);
        }
        return true;
    }
    return false;
}

inline bool mayRetryCommit(const TransactionCommitOptionsVariant & options, const TransactionCommitOutcomeVariant & outcome)
{
    if (isSuccessfulOutcome(outcome))
        return false;

    if (const auto * with_faults = std::get_if<MetaInKeeperCommitOptions<ZooKeeperWithFaultInjection>>(&options); with_faults)
    {
        return with_faults->may_retry;
    }
    else if (const auto * just_keeper = std::get_if<MetaInKeeperCommitOptions<zkutil::ZooKeeper>>(&options); just_keeper)
    {
        return just_keeper->may_retry;
    }

    return false;
}

inline MetaInKeeperCommitOutcome getKeeperOutcome(const TransactionCommitOutcomeVariant & outcome)
{
    if (const auto * result = std::get_if<MetaInKeeperCommitOutcome>(&outcome); result)
        return *result;

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Not keeper outcome");
}

}
