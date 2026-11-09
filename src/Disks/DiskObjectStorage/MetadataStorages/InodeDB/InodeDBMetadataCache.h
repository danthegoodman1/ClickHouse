#pragma once

#include "config.h"

#if USE_GRPC

#include <Disks/DiskObjectStorage/MetadataStorages/InodeDB/InodeDBClient.h>

#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

namespace DB::InodeDB
{

/// The files and directory listings an `inodedb` disk has read, by InodeDB path, so that repeated reads cost no
/// round trip. Reading a file loads its whole directory, so a part's files arrive in one listing.
///
/// A writer's commits invalidate what they touch: every path a batch names, with its subtree and its parent's
/// listing, and every name of each inode the batch changed, which InodeDB reports per op. A read is kept only if it
/// reflects every commit the cache has seen, so a read that raced a commit can't bring back what the commit changed.
///
/// A replica forgets listings at each refresh and keeps file records: a part's files never change in place, and a
/// query still reading a part the writer removed finds its records here.
class MetadataCache
{
public:
    struct File
    {
        pb::Inode inode;
        std::string value;
    };

    /// What the cache knows of a path from its parent's listing.
    enum class Presence : uint8_t
    {
        Unknown,
        Absent,
        File,
        Directory,
    };

    explicit MetadataCache(size_t max_bytes_) : max_bytes(max_bytes_) {}

    std::optional<File> file(const std::string & path) const;
    Presence presence(const std::string & path) const;
    std::optional<Listing> listing(const std::string & dir) const;

    /// Keeps a listing read at `version` with `attrs` and `values`, and the files it holds.
    void putListing(const std::string & dir, const Listing & listing, UInt64 version);
    void putFile(const std::string & path, File file, UInt64 version);

    /// After a batch committed.
    void invalidate(const pb::CommitRequest & request, const pb::CommitResponse & response);
    /// After a batch that may or may not have committed.
    void clear();
    /// A replica's refresh: directories may hold new entries.
    void forgetListings();

private:
    void eraseFileUnlocked(const std::string & path);
    void eraseSubtreeUnlocked(const std::string & path);
    void putFileUnlocked(const std::string & path, File file);
    void fitUnlocked();

    const size_t max_bytes;

    mutable std::mutex mutex;
    /// The version of the newest commit seen. Reads older than it are not kept.
    UInt64 newest_commit = 0;
    size_t bytes = 0;
    std::map<std::string, File> files;
    std::unordered_map<UInt64, std::set<std::string>> paths_of_inode;
    /// Complete listings: names and kinds.
    std::map<std::string, Listing> listings;
};

}

#endif
