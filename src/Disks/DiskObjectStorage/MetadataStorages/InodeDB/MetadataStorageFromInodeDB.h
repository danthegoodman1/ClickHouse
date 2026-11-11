#pragma once

#include "config.h"

#if USE_GRPC

#include <Disks/DiskObjectStorage/MetadataStorages/DiskObjectStorageMetadata.h>
#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/InMemoryRemovalQueue.h>
#include <Disks/DiskObjectStorage/MetadataStorages/InodeDB/InodeDBClient.h>
#include <Disks/DiskObjectStorage/MetadataStorages/InodeDB/InodeDBCoordination.h>
#include <Disks/DiskObjectStorage/MetadataStorages/InodeDB/InodeDBMetadataCache.h>
#include <Disks/DiskObjectStorage/MetadataStorages/InodeDB/InodeDBValue.h>

#include <Common/Logger.h>
#include <Common/ObjectStorageKeyGenerator.h>
#include <Common/SharedMutex.h>

#include <atomic>
#include <chrono>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

namespace DB
{

struct InodeDBMetadataSettings
{
    /// A read replica: it changes nothing in InodeDB and deletes no blobs.
    bool read_only = false;
    /// A record whose text passes this many bytes moves to a blob, since an InodeDB value holds 64 KiB.
    size_t spill_threshold = 32 * 1024;
    /// Files up to this size keep their bytes in their record instead of a blob, so writing them costs no
    /// object-storage request and a directory listing reads them. Smaller than `spill_threshold`.
    size_t inline_file_max_bytes = 16 * 1024;
    /// Freed files claimed from InodeDB at a time.
    UInt32 claim_batch = 1000;
    /// A replica's lease holds what InodeDB froze at the versions it read until this long after it read
    /// newer ones, so a query running that long finds every blob it started with.
    std::chrono::milliseconds lease_lag{600'000};
    /// What the metadata cache holds at most; zero turns it off.
    size_t cache_bytes = 256 << 20;
    /// `inodedb_writer_mode` = `shared`: the writer joins other shared writers instead of fencing them.
    bool shared = false;
    /// How long a shared writer keeps a deduplication token.
    std::chrono::seconds token_ttl{3600};
    /// How long a merge claim lasts unless its holder renews it.
    std::chrono::milliseconds claim_ttl{60'000};
};

class InodeDBReaderLease;

/// Metadata of an object-storage disk kept in InodeDB, a transactional tree of inodes.
///
/// The key's root holds four entries:
///   `tree/`   the disk's files and directories;
///   `owner`   the fence: every commit requires the version a writer found or wrote at startup;
///   `trash/`  where tombstones are created and unlinked;
///   `coord/`  coordination files of MergeTree tables (`IInodeDBCoordination`).
///
/// An exclusive writer (`inodedb_writer_mode` = `exclusive`) writes `owner` at startup, which fences every other
/// writer. A shared writer joins the shared owner it finds, or replaces an exclusive one, so shared writers commit
/// side by side until an exclusive writer starts.
///
/// A file's value is an `InodeDB::Value`. Directories, renames and hardlinks are InodeDB's own, so a rename is
/// one entry change and a hardlink shares the inode. Each transaction commits as one all-or-nothing batch.
///
/// Blobs are deleted through InodeDB's free queue, which waits for every reader lease to pass:
///   - a file whose last link goes carries its final record, whose blobs go unless the record is `retained`;
///   - blobs a transaction drops from a live file (truncate, overwrite) go in a tombstone;
///   - a value that `Put` replaced deletes nothing, since its transaction wrote a tombstone for what it dropped.
class MetadataStorageFromInodeDB final : public IMetadataStorage, public IInodeDBCoordination
{
public:
    MetadataStorageFromInodeDB(
        std::shared_ptr<InodeDB::Client> client_,
        ObjectStoragePtr object_storage_,
        String compatible_key_prefix_,
        ObjectStorageKeyGeneratorPtr key_generator_,
        InodeDBMetadataSettings settings_);
    ~MetadataStorageFromInodeDB() override;

    MetadataTransactionPtr createTransaction() override;

    const std::string & getPath() const override { return description; }
    MetadataStorageType getType() const override { return MetadataStorageType::InodeDB; }

    bool supportsEmptyFilesWithoutBlobs() const override { return true; }
    bool supportsInlineData() const override { return true; }
    size_t getInlineFileMaxBytes() const override { return settings.inline_file_max_bytes; }
    bool areBlobPathsRandom() const override { return true; }
    bool supportWritingWithAppend() const override { return true; }
    ObjectStorageKeyGeneratorPtr getKeyGenerator() const override { return key_generator; }

    bool existsFile(const std::string & path) const override;
    bool existsDirectory(const std::string & path) const override;
    bool existsFileOrDirectory(const std::string & path) const override;

    uint64_t getFileSize(const std::string & path) const override;
    std::optional<uint64_t> getFileSizeIfExists(const std::string & path) const override;

    Poco::Timestamp getLastModified(const std::string & path) const override;
    std::optional<Poco::Timestamp> getLastModifiedIfExists(const std::string & path) const override;
    time_t getLastChanged(const std::string & path) const override;

    bool supportsChmod() const override { return true; }
    bool supportsStat() const override { return true; }
    struct stat stat(const String & path) const override;

    std::vector<std::string> listDirectory(const std::string & path) const override;
    DirectoryIteratorPtr iterateDirectory(const std::string & path) const override;
    bool isDirectoryEmpty(const std::string & path) const override;

    uint32_t getHardlinkCount(const std::string & path) const override;

    std::string readFileToString(const std::string & path) const override;
    std::string readInlineDataToString(const std::string & path) const override;
    std::unordered_map<std::string, std::string> getSerializedMetadata(const std::vector<String> & file_paths) const override;

    StoredObjects getStorageObjects(const std::string & path) const override;
    std::optional<StoredObjects> getStorageObjectsIfExist(const std::string & path) const override;

    bool isReadOnly() const override { return settings.read_only; }
    bool isRemote() const override { return true; }

    void startup() override;
    void shutdown() override;
    /// A replica notes the version it reads now, which its lease releases once `lease_lag` passes.
    void refresh(UInt64 not_sooner_than_milliseconds) override;

    BlobsToRemove getBlobsToRemove(const ClusterConfigurationPtr & cluster, int64_t max_count) override;
    int64_t recordAsRemoved(const StoredObjects & blobs) override;
    bool hasPendingRemovalBlobs(const StoredObjects & blobs) const override;
    int64_t getDeadBlobsQueueEstimate() override;

    /// The InodeDB path of a disk path: `tree/<path>`, without trailing slashes.
    static std::string treePath(const std::string & path) { return IInodeDBCoordination::treePath(path); }

    bool isSharedWriter() const override { return settings.shared; }
    UInt64 ownerVersion() const override { return owner_version.load(); }
    const std::string & writerId() const override { return writer_id; }
    std::chrono::seconds tokenTTL() const override { return settings.token_ttl; }
    std::chrono::milliseconds claimTTL() const override { return settings.claim_ttl; }
    std::optional<IInodeDBCoordination::File> readFile(const std::string & path) const override;
    std::vector<std::optional<IInodeDBCoordination::File>> readFiles(const std::vector<std::string> & paths) const override;
    std::vector<std::pair<std::string, IInodeDBCoordination::File>> listFiles(const std::string & dir) const override;
    std::optional<UInt64> versionOf(const std::string & disk_path) const override;
    InodeDBCommitOutcome commit(const InodeDBCommitOptions & options) override;

private:
    friend class MetadataStorageFromInodeDBTransaction;
    friend class InodeDBBatchBuilder;

    struct File
    {
        InodeDB::pb::Inode inode;
        InodeDB::Value value;
    };

    /// The file at the InodeDB path, or nothing when no file is there.
    std::optional<File> readFileAt(const std::string & inodedb_path) const;
    std::optional<File> readTreeFile(const std::string & path) const { return readFileAt(treePath(path)); }
    /// The inode at a disk path, or nothing when the path names nothing.
    std::optional<InodeDB::pb::Inode> statPath(const std::string & path) const;
    /// The same, read from InodeDB: a cached link count misses what removing another name's directory took.
    std::optional<InodeDB::pb::Inode> statPathUncached(const std::string & path) const;
    /// Whether a disk path names a file, a directory or nothing, from the cache when it knows.
    InodeDB::MetadataCache::Presence presenceOf(const std::string & path) const;
    /// The listing of a disk directory, from the cache when it holds it.
    InodeDB::Listing listingOf(const std::string & path) const;
    /// The listing of an InodeDB directory read one page per request, each page at its own version, past
    /// the cache. Hazard tests list this way (`inodedb_list_in_pages`) to pause between the first two pages.
    InodeDB::Listing listingInPages(const std::string & inodedb_path) const;

    /// The record a value holds, following a spill pointer. Throws `UNKNOWN_FORMAT` for other kinds.
    DiskObjectStorageMetadata recordOf(const InodeDB::Value & value, const std::string & path) const;
    /// The blobs a value owns, the spill blob included. None for a retained or non-record value.
    StoredObjects blobsOwnedBy(const InodeDB::Value & value, const std::string & path) const;

    std::string readSpilled(const std::string & key) const;
    /// Writes bytes to a new blob and returns its key: a spilled record, or the bytes of an inline file that
    /// an append moves out of its record.
    std::string writeBlob(const std::string & bytes) const;
    /// Deletes the blobs a batch wrote, once the batch is known to have applied nothing.
    void removeUnappliedBlobs(const std::vector<std::string> & keys) const noexcept;

    void assertWritable() const;
    /// Joins the shared owner, or replaces an exclusive one under its version.
    void joinSharedOwner();

    /// Claims released free-queue entries and queues their blobs. Returns the blobs it queued.
    StoredObjects pullFreedUnlocked() TSA_REQUIRES(removal_mutex);
    StoredObjects pullFreed();
    /// The blobs to delete for a freed entry. `last` gets blobs that must go after the rest: a spill blob.
    StoredObjects blobsToDelete(const InodeDB::pb::Freed & freed, StoredObjects & last) const;

    const std::shared_ptr<InodeDB::Client> client;
    const ObjectStoragePtr object_storage;
    const std::string compatible_key_prefix;
    const ObjectStorageKeyGeneratorPtr key_generator;
    const InodeDBMetadataSettings settings;
    const std::string description;
    const LoggerPtr log;
    const std::unique_ptr<InodeDB::MetadataCache> cache;
    const std::string writer_id;

    /// The version of `owner` this server wrote or joined; every commit requires it. Zero before `startup`.
    std::atomic<UInt64> owner_version = 0;

    /// The lease of a replica or a shared writer, from `startup` to `shutdown`. A shared writer reads parts that
    /// other writers may remove.
    std::unique_ptr<InodeDBReaderLease> reader_lease;

    /// Transactions that read before they write hold it exclusively; the rest share it. An exclusive writer is the
    /// only one under its key's root, so an exclusive holder reads exactly what its batch applies to. Shared
    /// writers change only their own parts' files and coordination files, which every batch checks in InodeDB.
    mutable SharedMutex commit_mutex;

    struct FreedEntry
    {
        UInt64 claim = 0;
        InodeDB::pb::Freed freed{};
        /// Blobs of this entry not yet recorded as removed.
        std::unordered_set<std::string> remaining{};
        /// Blobs queued once `remaining` empties.
        StoredObjects last{};
    };

    mutable std::mutex removal_mutex;
    InMemoryRemovalQueue blobs_to_remove TSA_GUARDED_BY(removal_mutex);
    std::unordered_map<std::string, FreedEntry> freed_entries TSA_GUARDED_BY(removal_mutex);
    std::unordered_map<std::string, std::vector<std::string>> entries_of_blob TSA_GUARDED_BY(removal_mutex);
};

class MetadataStorageFromInodeDBTransaction final : public IMetadataTransaction
{
public:
    enum class IntentKind : uint8_t
    {
        CreateDirectory,
        CreateDirectoryRecursive,
        RemoveDirectory,
        RemoveRecursive,
        CreateHardLink,
        MoveFile,
        MoveDirectory,
        ReplaceFile,
        UnlinkFile,
        CreateMetadataFile,
        AddBlob,
        TruncateFile,
        WriteString,
        WriteInline,
        SetReadOnly,
        SetLastModified,
        Chmod,
    };

    /// One call, recorded until the commit turns the transaction into a batch.
    struct Intent
    {
        IntentKind kind = IntentKind::CreateDirectory;
        std::string path{};
        std::string path_to{};
        StoredObjects objects{};
        std::string data{};
        /// A size, a modification time in milliseconds, or a mode.
        UInt64 number = 0;
        bool if_exists = false;
        bool remove_objects = true;
        ShouldRemoveObjectsPredicate predicate{};
        /// An `if_exists` removal whose path lies under a file: it removes nothing, so the batch leaves it out.
        bool skip = false;

        /// Whether building the batch needs the current state of the paths it names.
        bool readsState() const;
        std::string describe() const;
    };

    explicit MetadataStorageFromInodeDBTransaction(MetadataStorageFromInodeDB & storage_);

    void commit(const TransactionCommitOptionsVariant & options) override;
    /// With `InodeDBCommitOptions`, returns an `InodeDBCommitOutcome` and keeps the intents, so the caller can retry.
    TransactionCommitOutcomeVariant tryCommit(const TransactionCommitOptionsVariant & options) override;
    /// Commits the intents with `extra` joined to the batch.
    InodeDBCommitOutcome commitWith(const InodeDBCommitOptions * extra);

    void writeStringToFile(const std::string & path, const std::string & data) override;
    void writeInlineDataToFile(const std::string & path, const std::string & data) override;
    void setLastModified(const std::string & path, const Poco::Timestamp & timestamp) override;
    bool supportsChmod() const override { return true; }
    void chmod(const String & path, mode_t mode) override;
    void setReadOnly(const std::string & path) override;
    void unlinkFile(const std::string & path, bool if_exists, bool should_remove_objects) override;
    void createDirectory(const std::string & path) override;
    void createDirectoryRecursive(const std::string & path) override;
    void removeDirectory(const std::string & path) override;
    void removeRecursive(const std::string & path, const ShouldRemoveObjectsPredicate & should_remove_objects) override;
    void createHardLink(const std::string & path_from, const std::string & path_to) override;
    void moveFile(const std::string & path_from, const std::string & path_to) override;
    void moveDirectory(const std::string & path_from, const std::string & path_to) override;
    void replaceFile(const std::string & path_from, const std::string & path_to) override;

    ObjectStorageKey generateObjectKeyForPath(const std::string & path) override;
    StoredObjects getSubmittedForRemovalBlobs() override { return submitted_for_removal; }

    void createMetadataFile(const std::string & path, const StoredObjects & objects) override;
    void addBlobToMetadata(const std::string & path, const StoredObject & object) override;
    void truncateFile(const std::string & path, size_t size) override;

private:
    enum class AttemptResult : uint8_t
    {
        Committed,
        /// An op or predicate of `InodeDBCommitOptions` failed; the outcome says which.
        ExtraFailed,
        /// A file the batch created without reading already existed.
        CreatedExisting,
        /// An `if_exists` removal found a file where its path needs a directory; it is now marked `skip`.
        SkippedRemoval,
    };

    AttemptResult attempt(bool read_state, bool & frees, const InodeDBCommitOptions * extra, InodeDBCommitOutcome & outcome);
    /// Whether every intent creates a directory that exists already, so the transaction changes nothing.
    bool onlyCreatesExistingDirectories() const;
    /// Whether every intent creates a directory that exists or removes a path that is absent.
    bool changesNothing() const;

    MetadataStorageFromInodeDB & storage;
    std::vector<Intent> intents;
    StoredObjects submitted_for_removal;
};

}

#endif
