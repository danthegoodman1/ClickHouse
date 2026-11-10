#pragma once

#include <Disks/DiskCommitTransactionOptions.h>
#include <Disks/IDisk.h>

#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace DB
{

/// BigHouse: what an `inodedb` disk offers the MergeTree tables on it so that several writers can share it.
///
/// Coordination files live under `coord/`, beside the disk's `tree/`, at the paths of the tables they serve:
///   `coord/{table path}/block_number`       the table's highest committed block number, in decimal;
///   `coord/{table path}/tokens/{block id}`  a deduplication token, holding the name of the part that took it;
///   `coord/{table path}/claims/{part name}` a merge claim on a source part (`InodeDBMergeClaim`).
/// Removing a directory of the disk removes its mirror under `coord/` in the same batch, so dropping a table
/// removes its coordination files.
class IInodeDBCoordination
{
public:
    struct File
    {
        std::string value;
        /// The version of the file's last change, which `VersionIs` compares.
        UInt64 version = 0;
        /// Milliseconds since the epoch, by the InodeDB writer's clock when the file was last written.
        UInt64 mtime_ms = 0;
    };

    virtual ~IInodeDBCoordination() = default;

    /// Whether this server shares the disk with other writers (`inodedb_writer_mode` = `shared`).
    virtual bool isSharedWriter() const = 0;
    /// Random for each server start. It names the writer in claims and in its temporary directories.
    virtual const std::string & writerId() const = 0;
    /// How long a shared writer keeps a deduplication token.
    virtual std::chrono::seconds tokenTTL() const = 0;
    /// How long a merge claim lasts without renewal.
    virtual std::chrono::milliseconds claimTTL() const = 0;

    /// A file read from InodeDB itself, past the disk's cache, or nothing. `path` is relative to the key's root.
    virtual std::optional<File> readFile(const std::string & path) const = 0;
    /// The same for many paths, in their order.
    virtual std::vector<std::optional<File>> readFiles(const std::vector<std::string> & paths) const = 0;
    /// The files of a directory with their values, read from InodeDB itself; empty when the directory is absent.
    virtual std::vector<std::pair<std::string, File>> listFiles(const std::string & dir) const = 0;
    /// The version of the inode at a disk path, or nothing.
    virtual std::optional<UInt64> versionOf(const std::string & disk_path) const = 0;
    /// Commits the ops and predicates alone, fenced as every commit of this writer is.
    virtual InodeDBCommitOutcome commit(const InodeDBCommitOptions & options) = 0;

    /// `coord/{disk path}`.
    static std::string coordPath(const std::string & disk_path);
    /// `tree/{disk path}`.
    static std::string treePath(const std::string & disk_path);
};

using InodeDBCoordinationPtr = std::shared_ptr<IInodeDBCoordination>;

/// The coordination of `disk`, unwrapping a `cache` disk, or nullptr unless the disk keeps its metadata in InodeDB.
InodeDBCoordinationPtr getInodeDBCoordination(const DiskPtr & disk);

}
