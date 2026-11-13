#include <Disks/DiskObjectStorage/MetadataStorages/InodeDB/MetadataStorageFromInodeDB.h>

#if USE_GRPC

#include <Disks/DiskObjectStorage/MetadataStorages/StaticDirectoryIterator.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/ThreadPool.h>
#include <Common/ProfileEvents.h>
#include <Common/getRandomASCIIString.h>
#include <Common/logger_useful.h>
#include <Common/thread_local_rng.h>

#include <magic_enum.hpp>

#include <base/defines.h>
#include <base/scope_guard.h>

#include <condition_variable>
#include <deque>
#include <filesystem>
#include <map>
#include <ranges>
#include <sys/stat.h>

namespace fs = std::filesystem;

namespace ProfileEvents
{
    extern const Event InodeDBCommitConflicts;
    extern const Event InodeDBFreedClaimed;
}

namespace DB
{

namespace FailPoints
{
    extern const char inodedb_list_in_pages[];
    extern const char inodedb_list_pause_between_pages[];
    extern const char inodedb_claim_freed_pause[];
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_RMDIR;
    extern const int CANNOT_UNLINK;
    extern const int DIRECTORY_ALREADY_EXISTS;
    extern const int DIRECTORY_DOESNT_EXIST;
    extern const int FILE_ALREADY_EXISTS;
    extern const int FILE_DOESNT_EXIST;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int SUPPORT_IS_DISABLED;
    extern const int TABLE_IS_READ_ONLY;
    extern const int UNKNOWN_FORMAT;
}

namespace pb = InodeDB::pb;
using InodeDB::Value;
using InodeDB::ValueKind;

namespace
{

constexpr std::string_view OWNER_PATH = "owner";
constexpr std::string_view TRASH_PATH = "trash";
constexpr std::string_view COORD_PATH = "coord";
/// `owner` holds the mode of the writers it fences in, then the id of the writer that wrote it.
constexpr std::string_view EXCLUSIVE_OWNER_PREFIX = "exclusive ";
constexpr std::string_view SHARED_OWNER_PREFIX = "shared ";

/// A disk path relative to the disk's root, without leading or trailing slashes. Dot-files keep their dots.
std::string normalize(const std::string & path)
{
    std::string normal = fs::path(path).lexically_normal().string();
    while (normal.ends_with('/'))
        normal.pop_back();
    while (normal.starts_with('/'))
        normal.erase(0, 1);
    if (normal == ".")
        normal.clear();
    for (const auto & part : fs::path(normal))
        if (part == "..")
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Path `{}` leaves the disk", path);
    return normal;
}

/// Whether normalized `path` is `prefix` or lies under it.
bool isUnder(const std::string & path, const std::string & prefix)
{
    if (prefix.empty())
        return true;
    return path.starts_with(prefix) && (path.size() == prefix.size() || path[prefix.size()] == '/');
}

/// `path`, which lies under `from`, moved under `to`.
std::string rebase(const std::string & path, const std::string & from, const std::string & to)
{
    std::string rest = path.substr(from.size());
    if (to.empty() && rest.starts_with('/'))
        rest.erase(0, 1);
    return to + rest;
}

std::string parentOf(const std::string & path)
{
    auto slash = path.rfind('/');
    return slash == std::string::npos ? std::string{} : path.substr(0, slash);
}

std::string randomName()
{
    return getRandomASCIIString(32);
}

/// Whether a disk path holds database or table definitions, which only an exclusive writer changes.
bool holdsDefinitions(const std::string & path)
{
    const std::string normal = normalize(path);
    return normal.ends_with(".sql") || normal.ends_with(".sql.tmp") || normal.ends_with(".sql.detached")
        || isUnder(normal, "metadata") || isUnder(normal, "metadata_dropped");
}

bool holdsRecord(const Value & value)
{
    return value.kind == ValueKind::Record || value.kind == ValueKind::Spilled;
}

Value recordValue(const DiskObjectStorageMetadata & record)
{
    Value value;
    value.kind = ValueKind::Record;
    value.body = record.serializeToString();
    return value;
}

pb::Op mkdirOp(const std::string & path, bool parents)
{
    pb::Op op;
    auto * mkdir = op.mutable_mkdir();
    mkdir->set_path(path);
    mkdir->set_parents(parents);
    mkdir->set_if_absent(true);
    return op;
}

pb::Op rmdirOp(const std::string & path)
{
    pb::Op op;
    op.mutable_rmdir()->set_path(path);
    return op;
}

pb::Op removeAllOp(const std::string & path)
{
    pb::Op op;
    auto * remove = op.mutable_remove_all();
    remove->set_path(path);
    remove->set_if_exists(true);
    return op;
}

pb::Op unlinkOp(const std::string & path, bool if_exists)
{
    pb::Op op;
    auto * unlink = op.mutable_unlink();
    unlink->set_path(path);
    unlink->set_if_exists(if_exists);
    return op;
}

pb::Op linkOp(const std::string & from, const std::string & to)
{
    pb::Op op;
    auto * link = op.mutable_link();
    link->set_from(from);
    link->set_to(to);
    return op;
}

pb::Op renameOp(const std::string & from, const std::string & to, pb::RenameFlag flag)
{
    pb::Op op;
    auto * rename = op.mutable_rename();
    rename->set_from(from);
    rename->set_to(to);
    rename->set_flag(flag);
    return op;
}

pb::Op putOp(const std::string & path, std::string value, pb::PutMode mode)
{
    pb::Op op;
    auto * put = op.mutable_put();
    put->set_path(path);
    put->set_value(std::move(value));
    put->set_mode(mode);
    return op;
}

const std::string & opPath(const pb::Op & op)
{
    switch (op.kind_case())
    {
        case pb::Op::kMkdir: return op.mkdir().path();
        case pb::Op::kPut: return op.put().path();
        case pb::Op::kLink: return op.link().to();
        case pb::Op::kUnlink: return op.unlink().path();
        case pb::Op::kRmdir: return op.rmdir().path();
        case pb::Op::kRemoveAll: return op.remove_all().path();
        case pb::Op::kRename: return op.rename().from();
        case pb::Op::kSetAttr: return op.set_attr().path();
        case pb::Op::KIND_NOT_SET: break;
    }
    static const std::string none;
    return none;
}

/// The error code a skipped op maps to, following what the local filesystem reports for the same call.
int errorCodeOf(const pb::Op & op, pb::Error error, bool moves_directory)
{
    switch (error)
    {
        case pb::NOT_FOUND:
            return (op.has_mkdir() || op.has_rmdir() || moves_directory) ? ErrorCodes::DIRECTORY_DOESNT_EXIST : ErrorCodes::FILE_DOESNT_EXIST;
        case pb::EXISTS:
            return ErrorCodes::FILE_ALREADY_EXISTS;
        case pb::NOT_DIR:
            return ErrorCodes::DIRECTORY_DOESNT_EXIST;
        case pb::IS_DIR:
            return op.has_unlink() ? ErrorCodes::CANNOT_UNLINK : ErrorCodes::FILE_ALREADY_EXISTS;
        case pb::NOT_EMPTY:
            return op.has_rmdir() ? ErrorCodes::CANNOT_RMDIR : ErrorCodes::DIRECTORY_ALREADY_EXISTS;
        case pb::INVALID:
            return ErrorCodes::BAD_ARGUMENTS;
        default:
            return ErrorCodes::LOGICAL_ERROR;
    }
}

}

/// Turns a transaction's intents into one InodeDB batch.
///
/// It simulates the transaction over the files it names, so an intent that reads a file sees what the intents
/// before it left: a value written earlier, a file moved here, another name of a hardlinked file. A file the
/// transaction has not touched is read from InodeDB at the path it had before the transaction's directory moves.
///
/// Without `read_state`, the builder reads nothing: a file it writes and has not seen is assumed absent and
/// created with `Put` mode `CREATE`, which fails when the file exists, and the transaction then retries with
/// `read_state`.
class InodeDBBatchBuilder
{
public:
    using Intent = MetadataStorageFromInodeDBTransaction::Intent;
    using IntentKind = MetadataStorageFromInodeDBTransaction::IntentKind;

    InodeDBBatchBuilder(const MetadataStorageFromInodeDB & storage_, bool read_state_, UInt64 ctime_)
        : storage(storage_), read_state(read_state_), ctime(ctime_)
    {
    }

    void apply(const Intent & intent, size_t intent_index);
    pb::CommitRequest finish();

    std::optional<size_t> intentOfOp(size_t op_index) const { return op_intents.at(op_index); }
    bool isBlindCreate(size_t op_index) const { return blind_creates.at(op_index); }
    bool freesAnything() const { return frees; }
    const std::vector<std::string> & blobsWritten() const { return written_blobs; }

private:
    struct FileNode
    {
        /// Where InodeDB holds the file, until this transaction reads or writes it.
        std::optional<std::string> store_path;
        bool known = false;
        /// Once known: the value, or nothing for a file that doesn't exist.
        std::optional<Value> value;
        /// Once known: the links InodeDB reported, before this transaction's links and unlinks.
        Int64 stored_links = 0;
        Int64 link_delta = 0;

        Int64 links() const { return stored_links + link_delta; }
    };
    using FileNodePtr = std::shared_ptr<FileNode>;

    /// A directory moved or removed by this transaction.
    struct TreeEvent
    {
        bool is_move = false;
        std::string from{};
        std::string to{};
    };

    /// The file at `path` now, or null when no file is there.
    FileNodePtr lookup(const std::string & path);
    void load(FileNode & node);
    /// The value at `path` before an intent writes it. Nothing for an absent file, or an unread one without
    /// `read_state`.
    std::optional<Value> base(const FileNodePtr & node);
    /// The record a base value holds, or an empty record.
    DiskObjectStorageMetadata baseRecord(const std::optional<Value> & value, const std::string & path) const;

    void emit(pb::Op op, std::optional<size_t> intent_index, bool blind_create = false);
    void putValue(const std::string & path, FileNodePtr node, Value value, size_t intent_index);
    /// Drops the blobs the old value owns that the new objects don't.
    void dropReplaced(const std::optional<Value> & old_value, const StoredObjects & new_objects, const std::string & path);
    /// Before an unlink that keeps the blobs, marks the value retained when the unlink frees it.
    void retainIfLast(const std::string & path, const FileNodePtr & node, size_t intent_index);
    /// Before a recursive removal, marks retained the values the removal frees and the predicate keeps.
    void retainKept(const std::string & path, const MetadataStorageFromInodeDBTransaction::ShouldRemoveObjectsPredicate & predicate, size_t intent_index);
    void moveSubtree(const std::string & from, const std::string & to);
    void removeSubtree(const std::string & path);

    const MetadataStorageFromInodeDB & storage;
    const bool read_state;
    const UInt64 ctime;

    /// Files by their path at this point of the transaction. Null: no file there.
    std::map<std::string, FileNodePtr> files;
    std::vector<TreeEvent> events;

    pb::CommitRequest request;
    std::vector<std::optional<size_t>> op_intents;
    std::vector<bool> blind_creates;
    StoredObjects dropped;
    /// Blobs this batch wrote: spilled records and inline bytes moved out by an append.
    std::vector<std::string> written_blobs;
    bool frees = false;
};

InodeDBBatchBuilder::FileNodePtr InodeDBBatchBuilder::lookup(const std::string & path)
{
    if (auto it = files.find(path); it != files.end())
        return it->second;

    /// Undo this transaction's directory moves and removals, newest first, to find where the file was.
    std::string store_path = path;
    for (const auto & event : std::views::reverse(events))
    {
        if (event.is_move && isUnder(store_path, event.to))
        {
            store_path = rebase(store_path, event.to, event.from);
        }
        else if (isUnder(store_path, event.from))
        {
            files[path] = nullptr;
            return nullptr;
        }
    }

    auto node = std::make_shared<FileNode>();
    node->store_path = std::move(store_path);
    files[path] = node;
    return node;
}

void InodeDBBatchBuilder::load(FileNode & node)
{
    if (node.known)
        return;
    chassert(read_state && node.store_path);

    if (auto file = storage.readTreeFile(*node.store_path))
    {
        node.value = std::move(file->value);
        node.stored_links = file->inode.nlink();
    }
    node.known = true;
}

std::optional<Value> InodeDBBatchBuilder::base(const FileNodePtr & node)
{
    if (!node)
        return std::nullopt;
    if (read_state)
        load(*node);
    return node->known ? node->value : std::nullopt;
}

DiskObjectStorageMetadata InodeDBBatchBuilder::baseRecord(const std::optional<Value> & value, const std::string & path) const
{
    /// Like the local filesystem, a file that holds no record reads as an empty one.
    if (value && holdsRecord(*value))
        return storage.recordOf(*value, path);
    return DiskObjectStorageMetadata(storage.compatible_key_prefix, path);
}

void InodeDBBatchBuilder::emit(pb::Op op, std::optional<size_t> intent_index, bool blind_create)
{
    *request.add_ops() = std::move(op);
    op_intents.push_back(intent_index);
    blind_creates.push_back(blind_create);
}

void InodeDBBatchBuilder::putValue(const std::string & path, FileNodePtr node, Value value, size_t intent_index)
{
    value.ctime = ctime;
    if (value.kind == ValueKind::Record && value.body.size() > storage.settings.spill_threshold)
    {
        std::string key = storage.writeBlob(value.body);
        written_blobs.push_back(key);
        value.kind = ValueKind::Spilled;
        value.body = std::move(key);
    }

    const bool unknown = node && !node->known;
    const bool absent = !node || (node->known && !node->value);
    const bool create = absent || unknown;
    emit(putOp(MetadataStorageFromInodeDB::treePath(path), value.encode(), create ? pb::CREATE : pb::UPSERT), intent_index, unknown);

    if (!node || absent || unknown)
    {
        node = std::make_shared<FileNode>();
        node->stored_links = 1;
        files[path] = node;
    }
    node->known = true;
    node->value = std::move(value);
}

void InodeDBBatchBuilder::dropReplaced(const std::optional<Value> & old_value, const StoredObjects & new_objects, const std::string & path)
{
    if (!old_value)
        return;

    std::unordered_set<std::string> kept;
    for (const auto & object : new_objects)
        kept.insert(object.remote_path);

    for (auto & object : storage.blobsOwnedBy(*old_value, path))
        if (!kept.contains(object.remote_path))
            dropped.push_back(std::move(object));
}

void InodeDBBatchBuilder::retainIfLast(const std::string & path, const FileNodePtr & node, size_t intent_index)
{
    if (!node)
        return;
    load(*node);
    if (!node->value || node->value->retained || !holdsRecord(*node->value))
        return;

    /// A recursive removal earlier in this transaction may have dropped links this count still holds, so after
    /// one the value is retained whatever the count says: at worst a blob outlives its last file.
    const bool removed_subtree = std::ranges::any_of(events, [](const TreeEvent & event) { return !event.is_move; });
    if (node->links() > 1 && !removed_subtree)
        return;

    Value retained = *node->value;
    retained.retained = true;
    emit(putOp(MetadataStorageFromInodeDB::treePath(path), retained.encode(), pb::UPSERT), intent_index);
    node->value = std::move(retained);
}

void InodeDBBatchBuilder::retainKept(
    const std::string & path, const MetadataStorageFromInodeDBTransaction::ShouldRemoveObjectsPredicate & predicate, size_t intent_index)
{
    /// The links counted below come from InodeDB, so the subtree must be as InodeDB holds it.
    for (const auto & [file_path, node] : files)
        if (isUnder(file_path, path))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Recursive removal of `{}` with a predicate after changes under it in the same transaction", path);
    for (const auto & event : events)
        if (isUnder(event.from, path) || isUnder(path, event.from) || (event.is_move && (isUnder(event.to, path) || isUnder(path, event.to))))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Recursive removal of `{}` with a predicate after moving or removing a directory around it", path);

    struct Name
    {
        std::string relative;
        std::string inodedb_path;
    };
    struct Inode
    {
        UInt32 links = 0;
        std::string value;
        std::vector<Name> names;
    };
    std::map<UInt64, Inode> inodes;

    if (auto file = storage.readTreeFile(path))
    {
        auto & inode = inodes[file->inode.handle().ino()];
        inode.links = file->inode.nlink();
        inode.value = file->value.encode();
        inode.names.push_back({".", MetadataStorageFromInodeDB::treePath(path)});
    }
    else
    {
        std::function<void(const std::string &, const std::string &)> walk = [&](const std::string & dir, const std::string & relative)
        {
            pb::ListRequest list;
            list.set_path(MetadataStorageFromInodeDB::treePath(dir));
            list.set_attrs(true);
            list.set_values(true);
            auto listing = storage.client->list(std::move(list));
            if (listing.error)
                return;
            for (const auto & entry : listing.entries)
            {
                std::string child = dir.empty() ? entry.name() : dir + "/" + entry.name();
                if (entry.kind() == pb::DIR)
                {
                    walk(child, relative + entry.name() + "/");
                    continue;
                }
                auto & inode = inodes[entry.handle().ino()];
                inode.links = entry.inode().nlink();
                inode.value = entry.value();
                inode.names.push_back({relative + entry.name(), MetadataStorageFromInodeDB::treePath(child)});
            }
        };
        walk(path, "");
    }

    for (const auto & [ino, inode] : inodes)
    {
        /// Only a file with every link inside the subtree is freed by the removal.
        if (inode.names.size() < inode.links)
            continue;
        const bool keep = std::ranges::any_of(inode.names, [&](const Name & name) { return !predicate(name.relative); });
        if (!keep)
            continue;

        Value value = Value::decode(inode.value, inode.names.front().inodedb_path);
        if (value.retained || !holdsRecord(value))
            continue;
        value.retained = true;
        emit(putOp(inode.names.front().inodedb_path, value.encode(), pb::UPSERT), intent_index);
    }
}

void InodeDBBatchBuilder::moveSubtree(const std::string & from, const std::string & to)
{
    std::vector<std::pair<std::string, FileNodePtr>> moved;
    for (auto it = files.begin(); it != files.end();)
    {
        if (isUnder(it->first, to))
        {
            it = files.erase(it);
        }
        else if (isUnder(it->first, from))
        {
            moved.emplace_back(rebase(it->first, from, to), it->second);
            it = files.erase(it);
        }
        else
        {
            ++it;
        }
    }
    for (auto & [path, node] : moved)
        files[path] = std::move(node);
    events.push_back({.is_move = true, .from = from, .to = to});
}

void InodeDBBatchBuilder::removeSubtree(const std::string & path)
{
    for (auto & [file_path, node] : files)
    {
        if (isUnder(file_path, path) && node)
        {
            --node->link_delta;
            node = nullptr;
        }
    }
    events.push_back({.is_move = false, .from = path, .to = {}});
}

void InodeDBBatchBuilder::apply(const Intent & intent, size_t intent_index)
{
    if (intent.skip)
        return;
    const std::string path = normalize(intent.path);
    const std::string tree_path = MetadataStorageFromInodeDB::treePath(path);

    switch (intent.kind)
    {
        case IntentKind::CreateDirectory:
        {
            emit(mkdirOp(tree_path, /*parents=*/ false), intent_index);
            break;
        }
        case IntentKind::CreateDirectoryRecursive:
        {
            if (!path.empty())
                emit(mkdirOp(tree_path, /*parents=*/ true), intent_index);
            break;
        }
        case IntentKind::RemoveDirectory:
        {
            emit(rmdirOp(tree_path), intent_index);
            /// A directory's coordination files go with it, as a table's do when it is dropped.
            emit(removeAllOp(IInodeDBCoordination::coordPath(path)), std::nullopt);
            break;
        }
        case IntentKind::RemoveRecursive:
        {
            if (intent.predicate)
                retainKept(path, intent.predicate, intent_index);
            emit(removeAllOp(tree_path), intent_index);
            emit(removeAllOp(IInodeDBCoordination::coordPath(path)), std::nullopt);
            removeSubtree(path);
            frees = true;
            break;
        }
        case IntentKind::CreateHardLink:
        {
            const std::string to = normalize(intent.path_to);
            auto node = lookup(path);
            emit(linkOp(tree_path, MetadataStorageFromInodeDB::treePath(to)), intent_index);
            if (node)
                ++node->link_delta;
            files[to] = node;
            break;
        }
        case IntentKind::MoveFile:
        {
            const std::string to = normalize(intent.path_to);
            emit(renameOp(tree_path, MetadataStorageFromInodeDB::treePath(to), pb::NO_REPLACE), intent_index);
            auto node = lookup(path);
            files[to] = node;
            files[path] = nullptr;
            break;
        }
        case IntentKind::ReplaceFile:
        {
            const std::string to = normalize(intent.path_to);
            /// Like the local filesystem, replacing creates the target's directories.
            if (std::string parent = parentOf(to); !parent.empty())
                emit(mkdirOp(MetadataStorageFromInodeDB::treePath(parent), /*parents=*/ true), intent_index);
            emit(renameOp(tree_path, MetadataStorageFromInodeDB::treePath(to), pb::REPLACE_TARGET), intent_index);
            if (auto replaced = lookup(to))
                --replaced->link_delta;
            auto node = lookup(path);
            files[to] = node;
            files[path] = nullptr;
            frees = true;
            break;
        }
        case IntentKind::MoveDirectory:
        {
            const std::string to = normalize(intent.path_to);
            /// Like `rename(2)`: a directory replaces an empty one.
            emit(renameOp(tree_path, MetadataStorageFromInodeDB::treePath(to), pb::REPLACE_TARGET), intent_index);
            moveSubtree(path, to);
            break;
        }
        case IntentKind::UnlinkFile:
        {
            auto node = lookup(path);
            if (!intent.remove_objects)
                retainIfLast(path, node, intent_index);
            emit(unlinkOp(tree_path, intent.if_exists), intent_index);
            if (node)
                --node->link_delta;
            files[path] = nullptr;
            frees = true;
            break;
        }
        case IntentKind::CreateMetadataFile:
        case IntentKind::WriteInline:
        {
            /// Both rewrite the file: blobs, or inline bytes, as `MetadataStorageFromMemory` writes them.
            auto node = lookup(path);
            auto old_value = base(node);
            DiskObjectStorageMetadata record(storage.compatible_key_prefix, path);
            record.read_only = baseRecord(old_value, path).read_only;
            record.objects = intent.objects;
            record.inline_data = intent.data;
            dropReplaced(old_value, record.objects, path);
            frees |= old_value.has_value();
            putValue(path, node, recordValue(record), intent_index);
            break;
        }
        case IntentKind::WriteString:
        {
            auto node = lookup(path);
            auto old_value = base(node);
            dropReplaced(old_value, {}, path);
            frees |= old_value.has_value();
            Value value;
            value.kind = ValueKind::String;
            value.body = intent.data;
            putValue(path, node, std::move(value), intent_index);
            break;
        }
        case IntentKind::AddBlob:
        {
            auto node = lookup(path);
            auto old_value = base(node);
            auto record = baseRecord(old_value, path);
            /// Inline bytes and blobs never share a record, so an append moves inline bytes to a blob first.
            if (!record.inline_data.empty())
            {
                std::string key = storage.writeBlob(record.inline_data);
                written_blobs.push_back(key);
                record.objects.emplace_back(key, path, record.inline_data.size());
                record.inline_data.clear();
            }
            record.objects.push_back(intent.objects.at(0));
            dropReplaced(old_value, record.objects, path);
            putValue(path, node, recordValue(record), intent_index);
            break;
        }
        case IntentKind::TruncateFile:
        {
            auto node = lookup(path);
            auto old_value = base(node);
            if (!old_value || !holdsRecord(*old_value))
            {
                if (intent.number > 0)
                    throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File `{}` doesn't exist, can't truncate", intent.path);
                break;
            }
            auto record = baseRecord(old_value, path);
            size_t size;
            if (!record.inline_data.empty())
            {
                record.inline_data.resize(std::min<size_t>(record.inline_data.size(), intent.number));
                size = record.inline_data.size();
            }
            else
            {
                /// Blobs go whole, so the size must fall on a blob boundary.
                size = getTotalSize(record.objects);
                while (size > intent.number)
                {
                    size -= record.objects.back().bytes_size;
                    record.objects.pop_back();
                }
            }
            if (size != intent.number)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "File `{}` can't be truncated to size {}", intent.path, intent.number);
            dropReplaced(old_value, record.objects, path);
            frees = true;
            putValue(path, node, recordValue(record), intent_index);
            break;
        }
        case IntentKind::SetReadOnly:
        {
            auto node = lookup(path);
            auto old_value = base(node);
            if (!old_value || !holdsRecord(*old_value))
                throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Can't update readonly flag for file: {}", intent.path);
            auto record = baseRecord(old_value, path);
            record.read_only = true;
            dropReplaced(old_value, record.objects, path);
            putValue(path, node, recordValue(record), intent_index);
            break;
        }
        case IntentKind::SetLastModified:
        {
            pb::Op op;
            op.mutable_set_attr()->set_path(tree_path);
            op.mutable_set_attr()->set_mtime(intent.number);
            emit(std::move(op), intent_index);
            break;
        }
        case IntentKind::Chmod:
        {
            pb::Op op;
            op.mutable_set_attr()->set_path(tree_path);
            op.mutable_set_attr()->set_mode(static_cast<UInt32>(intent.number));
            emit(std::move(op), intent_index);
            break;
        }
    }
}

pb::CommitRequest InodeDBBatchBuilder::finish()
{
    /// One tombstone per chunk of dropped blobs, each small enough for an InodeDB value.
    std::unordered_set<std::string> seen;
    StoredObjects unique;
    for (auto & blob : dropped)
        if (seen.insert(blob.remote_path).second)
            unique.push_back(std::move(blob));

    for (size_t i = 0; i < unique.size();)
    {
        DiskObjectStorageMetadata tombstone(storage.compatible_key_prefix, std::string(TRASH_PATH));
        size_t bytes = 0;
        while (i < unique.size() && (tombstone.objects.empty() || bytes + unique[i].remote_path.size() + 32 <= storage.settings.spill_threshold))
        {
            bytes += unique[i].remote_path.size() + 32;
            tombstone.objects.push_back(unique[i]);
            ++i;
        }

        Value value;
        value.kind = ValueKind::Tombstone;
        value.ctime = ctime;
        value.body = tombstone.serializeToString();

        const std::string name = fmt::format("{}/{}", TRASH_PATH, randomName());
        emit(putOp(name, value.encode(), pb::CREATE), std::nullopt);
        emit(unlinkOp(name, /*if_exists=*/ false), std::nullopt);
        frees = true;
    }

    request.set_all_or_nothing(true);
    if (UInt64 owner = storage.owner_version.load())
    {
        auto * version_is = request.add_when()->mutable_version_is();
        version_is->set_path(std::string(OWNER_PATH));
        version_is->set_version(owner);
    }
    return std::move(request);
}


/// Holds a replica's InodeDB lease from a thread of its own, renewing it every 10 seconds, and releases it on
/// destruction, so the writer can delete what it held right away.
///
/// The lease opens at the newest version. Its oldest version moves to a version the replica observed only once
/// `lag` has passed since, so the blobs of every part a query started with outlive the query, as long as it
/// takes less than `lag`. A lease that lapses, after a long network failure, opens again at the newest version.
class InodeDBReaderLease
{
public:
    InodeDBReaderLease(std::shared_ptr<InodeDB::Client> client_, std::chrono::milliseconds lag_, LoggerPtr log_)
        : client(std::move(client_)), lag(lag_), log(std::move(log_))
    {
        thread = ThreadFromGlobalPool([this] { run(); });
    }

    ~InodeDBReaderLease()
    {
        std::unique_lock lock(mutex);
        stopped = true;
        stopped_cv.notify_all();
        /// The thread releases the lease on its way out, in one round trip. A beat or release still waiting after
        /// an attempt's deadline is cancelled, and InodeDB lets the lease run out its TTL.
        if (!stopped_cv.wait_for(lock, client->attemptTimeout(), [this] { return finished; }) && stream)
            stream->cancel();
        lock.unlock();
        thread.join();
    }

    void observe(UInt64 version)
    {
        std::lock_guard lock(mutex);
        observed.emplace_back(std::chrono::steady_clock::now(), version);
    }

private:
    static constexpr auto HEARTBEAT_INTERVAL = std::chrono::seconds(10);

    /// Waits up to `duration` or until stopped. Returns whether it was stopped.
    bool waitStopped(std::chrono::milliseconds duration)
    {
        std::unique_lock lock(mutex);
        return stopped_cv.wait_for(lock, duration, [this] { return stopped; });
    }

    void run()
    {
        SCOPE_EXIT({
            std::lock_guard lock(mutex);
            stream.reset();
            finished = true;
            stopped_cv.notify_all();
        });

        while (true)
        {
            try
            {
                {
                    std::lock_guard lock(mutex);
                    if (stopped)
                        return;
                    stream = client->openLease();
                }

                pb::LeaseBeat open;
                open.set_current(true);
                auto state = stream->beat(open);
                const UInt64 lease = state.lease();
                UInt64 oldest = state.oldest();
                LOG_INFO(log, "Holding InodeDB lease {} from version {}", lease, oldest);

                while (!waitStopped(HEARTBEAT_INTERVAL))
                {
                    {
                        std::lock_guard lock(mutex);
                        const auto horizon = std::chrono::steady_clock::now() - lag;
                        while (!observed.empty() && observed.front().first <= horizon)
                        {
                            oldest = std::max(oldest, observed.front().second);
                            observed.pop_front();
                        }
                    }

                    pb::LeaseBeat beat;
                    beat.set_lease(lease);
                    beat.set_oldest(oldest);
                    beat.set_position(oldest);
                    state = stream->beat(beat);
                    if (state.expired())
                    {
                        LOG_ERROR(log, "InodeDB lease {} lapsed: blobs freed since version {} may be gone", lease, oldest);
                        break;
                    }
                }

                bool stop = false;
                {
                    std::lock_guard lock(mutex);
                    stop = stopped;
                    if (!stop)
                        stream.reset();
                }
                /// Outside the lock, so the destructor can cancel a release that hangs.
                if (stop)
                {
                    stream->release(lease);
                    return;
                }
            }
            catch (...)
            {
                {
                    std::lock_guard lock(mutex);
                    stream.reset();
                    if (stopped)
                        return;
                }
                tryLogCurrentException(log, "Holding an InodeDB lease");
                if (waitStopped(std::chrono::milliseconds(1000)))
                    return;
            }
        }
    }

    const std::shared_ptr<InodeDB::Client> client;
    const std::chrono::milliseconds lag;
    const LoggerPtr log;

    std::mutex mutex;
    /// Signals both `stopped` and `finished`.
    std::condition_variable stopped_cv;
    bool stopped = false;
    bool finished = false;
    std::unique_ptr<InodeDB::LeaseStream> stream;
    std::deque<std::pair<std::chrono::steady_clock::time_point, UInt64>> observed;
    ThreadFromGlobalPool thread;
};


MetadataStorageFromInodeDB::MetadataStorageFromInodeDB(
    std::shared_ptr<InodeDB::Client> client_,
    ObjectStoragePtr object_storage_,
    String compatible_key_prefix_,
    ObjectStorageKeyGeneratorPtr key_generator_,
    InodeDBMetadataSettings settings_)
    : client(std::move(client_))
    , object_storage(std::move(object_storage_))
    , compatible_key_prefix(std::move(compatible_key_prefix_))
    , key_generator(std::move(key_generator_))
    , settings(settings_)
    , description("inodedb://" + client->endpoint())
    , log(getLogger("MetadataStorageFromInodeDB"))
    , cache(std::make_unique<InodeDB::MetadataCache>(settings.cache_bytes))
    , writer_id(fmt::format("{:08x}", static_cast<UInt32>(thread_local_rng())))
{
}

MetadataStorageFromInodeDB::~MetadataStorageFromInodeDB() = default;

MetadataTransactionPtr MetadataStorageFromInodeDB::createTransaction()
{
    return std::make_shared<MetadataStorageFromInodeDBTransaction>(*this);
}

void MetadataStorageFromInodeDB::startup()
{
    if (settings.read_only)
    {
        if (!statPath(""))
            throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "{} holds no disk tree; a writer creates it at startup", description);
        reader_lease = std::make_unique<InodeDBReaderLease>(client, settings.lease_lag, log);
        LOG_INFO(log, "Reading {} as a replica", description);
        return;
    }

    if (settings.shared)
    {
        joinSharedOwner();
        reader_lease = std::make_unique<InodeDBReaderLease>(client, settings.lease_lag, log);
        return;
    }

    /// Writing `owner` fences off every other writer: their batches require the version they found or wrote.
    pb::CommitRequest request;
    *request.add_ops() = mkdirOp("tree", /*parents=*/ false);
    *request.add_ops() = mkdirOp(std::string(TRASH_PATH), /*parents=*/ false);
    *request.add_ops() = mkdirOp(std::string(COORD_PATH), /*parents=*/ false);
    *request.add_ops() = putOp(std::string(OWNER_PATH), fmt::format("{}{}", EXCLUSIVE_OWNER_PREFIX, writer_id), pb::UPSERT);
    request.set_all_or_nothing(true);

    auto response = client->commit(request);
    for (const auto & outcome : response.outcomes())
        if (outcome.has_skipped())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "{} could not take ownership: {}", description, outcome.skipped().ShortDebugString());
    cache->invalidate(request, response);

    /// The commit's version is the owner file's version: reading it back could see a later writer's.
    owner_version = response.version();
    LOG_INFO(log, "Took ownership of {} at version {} as writer {}", description, response.version(), writer_id);
}

void MetadataStorageFromInodeDB::joinSharedOwner()
{
    while (true)
    {
        auto owner = readFile(std::string(OWNER_PATH));
        if (owner && owner->value.starts_with(SHARED_OWNER_PREFIX))
        {
            /// The writer that wrote a shared owner created the tree in the same batch.
            owner_version = owner->version;
            LOG_INFO(log, "Joined the shared writers of {} at owner version {} as writer {}", description, owner->version, writer_id);
            return;
        }

        /// The exclusive writer may be at work, and only its operator knows when it is done.
        if (owner && !settings.replace_exclusive_owner)
            throw Exception(ErrorCodes::TABLE_IS_READ_ONLY,
                "{} belongs to an exclusive writer (`owner` is `{}` at version {}), and this writer leaves an exclusive owner in place "
                "(`inodedb_replace_exclusive_owner` = 0)",
                description, owner->value, owner->version);

        /// Replacing an exclusive owner, or none, under the version read fences the exclusive writer, and of
        /// shared writers starting at once only the first replaces it; the others join on their next read.
        pb::CommitRequest request;
        *request.add_ops() = mkdirOp("tree", /*parents=*/ false);
        *request.add_ops() = mkdirOp(std::string(TRASH_PATH), /*parents=*/ false);
        *request.add_ops() = mkdirOp(std::string(COORD_PATH), /*parents=*/ false);
        *request.add_ops() = putOp(std::string(OWNER_PATH), fmt::format("{}{}", SHARED_OWNER_PREFIX, writer_id), pb::UPSERT);
        auto * when = request.add_when();
        if (owner)
        {
            when->mutable_version_is()->set_path(std::string(OWNER_PATH));
            when->mutable_version_is()->set_version(owner->version);
        }
        else
        {
            when->set_absent(std::string(OWNER_PATH));
        }
        request.set_all_or_nothing(true);

        auto response = client->commit(request);
        bool owner_changed = false;
        for (const auto & outcome : response.outcomes())
        {
            if (!outcome.has_skipped() || outcome.skipped().reason_case() == pb::Skipped::kAborted)
                continue;
            if (outcome.skipped().reason_case() != pb::Skipped::kWhen)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "{} could not begin a shared epoch: {}", description, outcome.skipped().ShortDebugString());
            owner_changed = true;
        }
        if (owner_changed)
            continue;

        cache->invalidate(request, response);
        owner_version = response.version();
        LOG_INFO(log, "Began a shared epoch of {} at version {} as writer {}", description, response.version(), writer_id);
        return;
    }
}

std::optional<IInodeDBCoordination::File> MetadataStorageFromInodeDB::readFile(const std::string & path) const
{
    pb::ReadRequest request;
    request.add_targets()->mutable_target()->set_path(path);
    auto response = client->read(request);
    if (response.results_size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "InodeDB answered a read of one file with {} results", response.results_size());

    const auto & result = response.results(0);
    if (result.has_error())
    {
        switch (result.error())
        {
            case pb::NOT_FOUND:
            case pb::NOT_DIR:
                return std::nullopt;
            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Reading `{}` from InodeDB failed with {}", path, pb::Error_Name(result.error()));
        }
    }
    return IInodeDBCoordination::File{result.value().value(), result.value().inode().version(), result.value().inode().mtime()};
}

std::vector<std::optional<IInodeDBCoordination::File>> MetadataStorageFromInodeDB::readFiles(const std::vector<std::string> & paths) const
{
    /// InodeDB reads at most 60 targets a call.
    constexpr size_t max_targets = 60;
    std::vector<std::optional<IInodeDBCoordination::File>> files;
    files.reserve(paths.size());
    for (size_t begin = 0; begin < paths.size(); begin += max_targets)
    {
        const size_t end = std::min(paths.size(), begin + max_targets);
        pb::ReadRequest request;
        for (size_t i = begin; i < end; ++i)
            request.add_targets()->mutable_target()->set_path(paths[i]);
        auto response = client->read(request);
        if (static_cast<size_t>(response.results_size()) != end - begin)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "InodeDB answered a read of {} files with {} results", end - begin, response.results_size());
        for (size_t i = begin; i < end; ++i)
        {
            const auto & result = response.results(static_cast<int>(i - begin));
            if (!result.has_error())
            {
                files.emplace_back(IInodeDBCoordination::File{result.value().value(), result.value().inode().version(), result.value().inode().mtime()});
                continue;
            }
            if (result.error() != pb::NOT_FOUND && result.error() != pb::NOT_DIR)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Reading `{}` from InodeDB failed with {}", paths[i], pb::Error_Name(result.error()));
            files.emplace_back(std::nullopt);
        }
    }
    return files;
}

std::vector<std::pair<std::string, IInodeDBCoordination::File>> MetadataStorageFromInodeDB::listFiles(const std::string & dir) const
{
    pb::ListRequest request;
    request.set_path(dir);
    request.set_attrs(true);
    request.set_values(true);
    auto listing = client->list(std::move(request));
    if (listing.error)
    {
        if (*listing.error == pb::NOT_FOUND)
            return {};
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Listing `{}` in InodeDB failed with {}", dir, pb::Error_Name(*listing.error));
    }

    std::vector<std::pair<std::string, IInodeDBCoordination::File>> files;
    files.reserve(listing.entries.size());
    for (auto & entry : listing.entries)
        if (entry.kind() == pb::FILE)
            files.emplace_back(entry.name(), IInodeDBCoordination::File{std::move(*entry.mutable_value()), entry.inode().version(), entry.inode().mtime()});
    return files;
}

std::optional<UInt64> MetadataStorageFromInodeDB::versionOf(const std::string & disk_path) const
{
    if (auto inode = statPathUncached(disk_path))
        return inode->version();
    return std::nullopt;
}

InodeDBCommitOutcome MetadataStorageFromInodeDB::commit(const InodeDBCommitOptions & options)
{
    MetadataStorageFromInodeDBTransaction transaction(*this);
    return transaction.commitWith(&options);
}

void MetadataStorageFromInodeDB::shutdown()
{
    reader_lease.reset();
}

void MetadataStorageFromInodeDB::refresh(UInt64 /* not_sooner_than_milliseconds */)
{
    if (!reader_lease)
        return;
    cache->forgetListings();
    pb::StatRequest request;
    request.add_targets()->set_path("tree");
    reader_lease->observe(client->stat(request).version());
}

void MetadataStorageFromInodeDB::assertWritable() const
{
    if (settings.read_only)
        throw Exception(ErrorCodes::TABLE_IS_READ_ONLY, "{} is read-only", description);
    if (owner_version.load() == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "{} commits before it took ownership at startup", description);
}

std::optional<MetadataStorageFromInodeDB::File> MetadataStorageFromInodeDB::readFileAt(const std::string & inodedb_path) const
{
    if (auto cached = cache->file(inodedb_path))
        return File{std::move(cached->inode), Value::decode(cached->value, inodedb_path)};

    switch (cache->presence(inodedb_path))
    {
        case InodeDB::MetadataCache::Presence::Absent:
        case InodeDB::MetadataCache::Presence::Directory:
            return std::nullopt;
        case InodeDB::MetadataCache::Presence::File:
        case InodeDB::MetadataCache::Presence::Unknown:
            break;
    }

    /// Load the whole directory: a part's files are read together.
    if (auto slash = inodedb_path.rfind('/'); slash != std::string::npos)
    {
        const std::string dir = inodedb_path.substr(0, slash);
        const std::string name = inodedb_path.substr(slash + 1);
        pb::ListRequest list;
        list.set_path(dir);
        list.set_attrs(true);
        list.set_values(true);
        auto listing = client->list(std::move(list));
        if (listing.error)
            return std::nullopt;
        cache->putListing(dir, listing, listing.version);
        for (auto & entry : listing.entries)
        {
            if (entry.name() != name)
                continue;
            if (entry.kind() != pb::FILE)
                return std::nullopt;
            pb::Inode inode = std::move(*entry.mutable_inode());
            *inode.mutable_handle() = entry.handle();
            return File{std::move(inode), Value::decode(entry.value(), inodedb_path)};
        }
        return std::nullopt;
    }

    pb::ReadRequest request;
    request.add_targets()->mutable_target()->set_path(inodedb_path);
    auto response = client->read(request);
    if (response.results_size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "InodeDB answered a read of one file with {} results", response.results_size());

    const auto & result = response.results(0);
    if (result.has_error())
    {
        switch (result.error())
        {
            case pb::NOT_FOUND:
            case pb::NOT_DIR:
            case pb::IS_DIR:
                return std::nullopt;
            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Reading `{}` from InodeDB failed with {}", inodedb_path, pb::Error_Name(result.error()));
        }
    }

    const auto & value = result.value();
    if (value.inode().kind() != pb::FILE)
        return std::nullopt;
    cache->putFile(inodedb_path, {value.inode(), value.value()}, response.version());
    return File{value.inode(), Value::decode(value.value(), inodedb_path)};
}

std::optional<pb::Inode> MetadataStorageFromInodeDB::statPath(const std::string & path) const
{
    const std::string inodedb_path = treePath(path);
    if (auto cached = cache->file(inodedb_path))
        return std::move(cached->inode);
    if (cache->presence(inodedb_path) == InodeDB::MetadataCache::Presence::Absent)
        return std::nullopt;
    return statPathUncached(path);
}

std::optional<pb::Inode> MetadataStorageFromInodeDB::statPathUncached(const std::string & path) const
{
    pb::StatRequest request;
    request.add_targets()->set_path(treePath(path));
    auto response = client->stat(request);
    if (response.results_size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "InodeDB answered a stat of one path with {} results", response.results_size());

    const auto & result = response.results(0);
    if (result.has_error())
    {
        if (result.error() == pb::NOT_FOUND || result.error() == pb::NOT_DIR)
            return std::nullopt;
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Stat of `{}` in InodeDB failed with {}", path, pb::Error_Name(result.error()));
    }
    return result.inode();
}

DiskObjectStorageMetadata MetadataStorageFromInodeDB::recordOf(const Value & value, const std::string & path) const
{
    DiskObjectStorageMetadata record(compatible_key_prefix, path);
    switch (value.kind)
    {
        case ValueKind::Record:
        case ValueKind::Tombstone:
            record.deserializeFromString(value.body);
            return record;
        case ValueKind::Spilled:
            record.deserializeFromString(readSpilled(value.body));
            return record;
        case ValueKind::String:
            throw Exception(ErrorCodes::UNKNOWN_FORMAT, "File `{}` holds bytes written by `writeStringToFile`, not a metadata record", path);
    }
    UNREACHABLE();
}

StoredObjects MetadataStorageFromInodeDB::blobsOwnedBy(const Value & value, const std::string & path) const
{
    StoredObjects blobs;
    if (!holdsRecord(value))
        return blobs;
    if (!value.retained)
        blobs = recordOf(value, path).objects;
    if (value.kind == ValueKind::Spilled)
        blobs.emplace_back(value.body, path);
    return blobs;
}

std::string MetadataStorageFromInodeDB::readSpilled(const std::string & key) const
{
    auto buf = object_storage->readObject(StoredObject(key), ReadSettings{});
    std::string text;
    readStringUntilEOF(text, *buf);
    return text;
}

std::string MetadataStorageFromInodeDB::writeBlob(const std::string & bytes) const
{
    std::string key = key_generator->generate().serialize();
    auto buf = object_storage->writeObject(StoredObject(key), WriteMode::Rewrite);
    writeString(bytes, *buf);
    buf->finalize();
    return key;
}

void MetadataStorageFromInodeDB::removeUnappliedBlobs(const std::vector<std::string> & keys) const noexcept
{
    if (keys.empty())
        return;
    try
    {
        StoredObjects objects;
        for (const auto & key : keys)
            objects.emplace_back(key);
        object_storage->removeObjectsIfExist(objects);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Removing the blobs of a batch that did not apply");
    }
}

InodeDB::MetadataCache::Presence MetadataStorageFromInodeDB::presenceOf(const std::string & path) const
{
    using Presence = InodeDB::MetadataCache::Presence;
    if (auto presence = cache->presence(treePath(path)); presence != Presence::Unknown)
        return presence;
    auto inode = statPath(path);
    if (!inode)
        return Presence::Absent;
    return inode->kind() == pb::DIR ? Presence::Directory : Presence::File;
}

InodeDB::Listing MetadataStorageFromInodeDB::listingOf(const std::string & path) const
{
    const std::string inodedb_path = treePath(path);
    bool in_pages = false;
    fiu_do_on(FailPoints::inodedb_list_in_pages, { in_pages = true; });
    if (in_pages)
        return listingInPages(inodedb_path);

    if (auto cached = cache->listing(inodedb_path))
        return std::move(*cached);

    pb::ListRequest request;
    request.set_path(inodedb_path);
    request.set_attrs(true);
    request.set_values(true);
    auto listing = client->list(std::move(request));
    cache->putListing(inodedb_path, listing, listing.version);
    return listing;
}

InodeDB::Listing MetadataStorageFromInodeDB::listingInPages(const std::string & inodedb_path) const
{
    constexpr UInt64 page_size = 1024;
    InodeDB::Listing listing;
    while (true)
    {
        pb::ListRequest request;
        request.set_path(inodedb_path);
        request.set_attrs(true);
        request.set_values(true);
        request.set_limit(page_size);
        if (!listing.entries.empty())
            request.set_after(listing.entries.back().name());
        auto page = client->list(std::move(request));
        if (page.error)
            return page;
        if (listing.version == 0 || page.version < listing.version)
            listing.version = page.version;
        const bool full = page.entries.size() == page_size;
        std::ranges::move(page.entries, std::back_inserter(listing.entries));
        if (!full)
            return listing;
        if (listing.entries.size() == page_size)
            FailPointInjection::pauseFailPoint(FailPoints::inodedb_list_pause_between_pages);
    }
}

bool MetadataStorageFromInodeDB::existsFile(const std::string & path) const
{
    return presenceOf(path) == InodeDB::MetadataCache::Presence::File;
}

bool MetadataStorageFromInodeDB::existsDirectory(const std::string & path) const
{
    return presenceOf(path) == InodeDB::MetadataCache::Presence::Directory;
}

bool MetadataStorageFromInodeDB::existsFileOrDirectory(const std::string & path) const
{
    return presenceOf(path) != InodeDB::MetadataCache::Presence::Absent;
}

namespace
{

uint64_t sizeOfRecord(const DiskObjectStorageMetadata & record)
{
    return record.objects.empty() ? record.inline_data.size() : getTotalSize(record.objects);
}

}

std::optional<uint64_t> MetadataStorageFromInodeDB::getFileSizeIfExists(const std::string & path) const
{
    auto file = readTreeFile(path);
    if (!file)
        return std::nullopt;
    if (file->value.kind == ValueKind::String)
        return file->value.body.size();
    return sizeOfRecord(recordOf(file->value, path));
}

uint64_t MetadataStorageFromInodeDB::getFileSize(const std::string & path) const
{
    if (auto size = getFileSizeIfExists(path))
        return *size;
    throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File `{}` doesn't exist", path);
}

std::optional<Poco::Timestamp> MetadataStorageFromInodeDB::getLastModifiedIfExists(const std::string & path) const
{
    auto inode = statPath(path);
    if (!inode)
        return std::nullopt;
    return Poco::Timestamp(static_cast<Poco::Timestamp::TimeVal>(inode->mtime()) * 1000);
}

Poco::Timestamp MetadataStorageFromInodeDB::getLastModified(const std::string & path) const
{
    if (auto timestamp = getLastModifiedIfExists(path))
        return *timestamp;
    throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File or directory `{}` doesn't exist", path);
}

time_t MetadataStorageFromInodeDB::getLastChanged(const std::string & path) const
{
    if (auto file = readTreeFile(path))
        return static_cast<time_t>(std::max(file->value.ctime, file->inode.mtime() / 1000));
    if (auto inode = statPath(path))
        return static_cast<time_t>(inode->mtime() / 1000);
    throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File or directory `{}` doesn't exist", path);
}

struct stat MetadataStorageFromInodeDB::stat(const String & path) const
{
    struct stat result{};
    auto set_times = [&](UInt64 mtime_ms, UInt64 ctime_s)
    {
        result.st_mtim.tv_sec = static_cast<time_t>(mtime_ms / 1000);
        result.st_mtim.tv_nsec = static_cast<long>((mtime_ms % 1000) * 1'000'000);
        result.st_ctim.tv_sec = static_cast<time_t>(std::max(ctime_s, mtime_ms / 1000));
        result.st_atim = result.st_mtim;
    };

    if (auto file = readTreeFile(path))
    {
        result.st_ino = file->inode.handle().ino();
        result.st_mode = S_IFREG | (file->inode.mode() ? (file->inode.mode() & 07777) : 0644);
        result.st_nlink = statPathUncached(path).value_or(file->inode).nlink();
        result.st_size = file->value.kind == ValueKind::String ? file->value.body.size() : sizeOfRecord(recordOf(file->value, path));
        set_times(file->inode.mtime(), file->value.ctime);
        return result;
    }
    if (auto inode = statPath(path))
    {
        result.st_ino = inode->handle().ino();
        result.st_mode = S_IFDIR | (inode->mode() ? (inode->mode() & 07777) : 0755);
        result.st_nlink = std::max<UInt32>(inode->nlink(), 1);
        set_times(inode->mtime(), 0);
        return result;
    }
    throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File or directory `{}` doesn't exist", path);
}

std::vector<std::string> MetadataStorageFromInodeDB::listDirectory(const std::string & path) const
{
    auto listing = listingOf(path);
    if (listing.error)
        throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Directory `{}` doesn't exist", path);

    std::vector<std::string> names;
    names.reserve(listing.entries.size());
    for (const auto & entry : listing.entries)
        names.push_back(entry.name());
    return names;
}

DirectoryIteratorPtr MetadataStorageFromInodeDB::iterateDirectory(const std::string & path) const
{
    auto listing = listingOf(path);

    /// Like the local filesystem: a missing directory iterates over nothing, and a directory's path ends in `/`.
    std::vector<fs::path> paths;
    if (!listing.error)
    {
        paths.reserve(listing.entries.size());
        for (const auto & entry : listing.entries)
        {
            fs::path child = fs::path(path) / entry.name();
            paths.push_back(entry.kind() == pb::DIR ? child / "" : child);
        }
    }
    return std::make_unique<StaticDirectoryIterator>(std::move(paths));
}

bool MetadataStorageFromInodeDB::isDirectoryEmpty(const std::string & path) const
{
    if (auto cached = cache->listing(treePath(path)))
        return cached->entries.empty();

    pb::ListRequest request;
    request.set_path(treePath(path));
    request.set_limit(1);
    auto listing = client->list(std::move(request));
    return listing.error || listing.entries.empty();
}

uint32_t MetadataStorageFromInodeDB::getHardlinkCount(const std::string & path) const
{
    auto inode = statPathUncached(path);
    if (!inode || inode->kind() != pb::FILE)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File `{}` doesn't exist", path);
    /// Like the local record's `ref_count`: links besides this one.
    return inode->nlink() - 1;
}

std::string MetadataStorageFromInodeDB::readFileToString(const std::string & path) const
{
    auto file = readTreeFile(path);
    if (!file)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File `{}` doesn't exist", path);
    if (file->value.kind == ValueKind::Spilled)
        return readSpilled(file->value.body);
    return file->value.body;
}

std::string MetadataStorageFromInodeDB::readInlineDataToString(const std::string & path) const
{
    auto file = readTreeFile(path);
    if (!file)
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File `{}` doesn't exist", path);
    return recordOf(file->value, path).inline_data;
}

std::unordered_map<std::string, std::string> MetadataStorageFromInodeDB::getSerializedMetadata(const std::vector<String> & file_paths) const
{
    std::unordered_map<std::string, std::string> result;
    for (const auto & path : file_paths)
    {
        auto file = readTreeFile(path);
        if (!file)
            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File `{}` doesn't exist", path);
        auto record = recordOf(file->value, path);
        record.ref_count = 0;
        result[path] = record.serializeToString();
    }
    return result;
}

StoredObjects MetadataStorageFromInodeDB::getStorageObjects(const std::string & path) const
{
    if (auto objects = getStorageObjectsIfExist(path))
        return std::move(*objects);
    throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File `{}` doesn't exist", path);
}

std::optional<StoredObjects> MetadataStorageFromInodeDB::getStorageObjectsIfExist(const std::string & path) const
{
    auto file = readTreeFile(path);
    if (!file)
        return std::nullopt;
    return recordOf(file->value, path).objects;
}

StoredObjects MetadataStorageFromInodeDB::blobsToDelete(const pb::Freed & freed, StoredObjects & last) const
{
    /// A value that `Put` replaced: the transaction that replaced it wrote a tombstone for the blobs it dropped.
    if (freed.has_value_version())
        return {};
    if (freed.inode().kind() != pb::FILE || !freed.has_value())
        return {};
    /// Coordination files under `coord/` hold plain text, which never starts with the format byte, and name no blob.
    if (freed.value().empty() || static_cast<UInt8>(freed.value()[0]) != Value::FORMAT_VERSION)
        return {};

    const std::string path = fmt::format("freed inode {}", freed.handle().ino());
    Value value;
    try
    {
        value = Value::decode(freed.value(), path);
    }
    catch (const Exception &)
    {
        /// Not a value this disk writes, so it names no blob this disk knows how to delete.
        tryLogCurrentException(log, "Skipping a freed file that holds no InodeDB metadata value");
        return {};
    }

    switch (value.kind)
    {
        case ValueKind::String:
            return {};
        case ValueKind::Tombstone:
            return recordOf(value, path).objects;
        case ValueKind::Record:
            return value.retained ? StoredObjects{} : recordOf(value, path).objects;
        case ValueKind::Spilled:
        {
            /// The spill blob goes last, so a crash midway can still find the blobs it names. A spill blob that
            /// is gone means an earlier attempt deleted those blobs already.
            StoredObject spill(value.body, path);
            if (!object_storage->exists(spill))
                return {};
            last.push_back(spill);
            return value.retained ? StoredObjects{} : recordOf(value, path).objects;
        }
    }
    UNREACHABLE();
}

StoredObjects MetadataStorageFromInodeDB::pullFreed(size_t & claimed)
{
    StoredObjects queued;
    FailPointInjection::pauseFailPoint(FailPoints::inodedb_claim_freed_pause);
    auto claim = client->claimFreed(settings.claim_batch);
    claimed = claim.freed().size();
    if (claim.freed().empty())
        return queued;
    ProfileEvents::increment(ProfileEvents::InodeDBFreedClaimed, claim.freed().size());

    pb::AckRequest ack_now;
    ack_now.set_claim(claim.claim());
    {
        std::lock_guard lock(removal_mutex);
        for (const auto & freed : claim.freed())
        {
            std::string id = fmt::format("{}/{}/{}", freed.version(), freed.handle().ino(), freed.has_value_version() ? freed.value_version() : 0);
            if (auto it = freed_entries.find(id); it != freed_entries.end())
            {
                /// The claim ran out before the blobs went, and InodeDB handed the entry out again.
                it->second.claim = claim.claim();
                it->second.freed = freed;
                continue;
            }

            StoredObjects last;
            StoredObjects blobs = blobsToDelete(freed, last);
            if (blobs.empty())
                std::swap(blobs, last);
            if (blobs.empty())
            {
                *ack_now.add_freed() = freed;
                continue;
            }

            FreedEntry entry{.claim = claim.claim(), .freed = freed, .remaining = {}, .last = std::move(last)};
            for (const auto & blob : blobs)
            {
                entry.remaining.insert(blob.remote_path);
                entries_of_blob[blob.remote_path].push_back(id);
            }
            blobs_to_remove.submitForRemoval(blobs);
            queued.insert(queued.end(), blobs.begin(), blobs.end());
            freed_entries.emplace(std::move(id), std::move(entry));
        }
    }

    if (!ack_now.freed().empty())
        client->ackFreed(ack_now);
    return queued;
}

IMetadataStorage::BlobsToRemove MetadataStorageFromInodeDB::getBlobsToRemove(const ClusterConfigurationPtr & cluster, int64_t max_count)
{
    if (settings.read_only)
        return {};

    /// A claim holds at most `claim_batch` entries, so claim until the queue holds `max_count` blobs or a claim
    /// comes back empty.
    size_t claimed = 1;
    while (claimed > 0 && (max_count <= 0 || getDeadBlobsQueueEstimate() < max_count))
        pullFreed(claimed);

    std::lock_guard lock(removal_mutex);
    BlobsToRemove result;
    for (const auto & blob : blobs_to_remove.takeFirst(max_count))
        result[blob] = {cluster->getLocalLocation()};
    return result;
}

int64_t MetadataStorageFromInodeDB::recordAsRemoved(const StoredObjects & blobs)
{
    std::map<UInt64, pb::AckRequest> acks;
    int64_t count = 0;
    {
        std::lock_guard lock(removal_mutex);
        count = blobs_to_remove.markAsRemoved(blobs);

        StoredObjects next;
        for (const auto & blob : blobs)
        {
            auto it = entries_of_blob.find(blob.remote_path);
            if (it == entries_of_blob.end())
                continue;
            auto ids = std::move(it->second);
            entries_of_blob.erase(it);

            for (const auto & id : ids)
            {
                auto entry_it = freed_entries.find(id);
                if (entry_it == freed_entries.end())
                    continue;
                auto & entry = entry_it->second;
                entry.remaining.erase(blob.remote_path);
                if (!entry.remaining.empty())
                    continue;

                if (!entry.last.empty())
                {
                    for (const auto & last : entry.last)
                    {
                        entry.remaining.insert(last.remote_path);
                        entries_of_blob[last.remote_path].push_back(id);
                    }
                    next.insert(next.end(), entry.last.begin(), entry.last.end());
                    entry.last.clear();
                    continue;
                }

                auto & ack = acks[entry.claim];
                ack.set_claim(entry.claim);
                *ack.add_freed() = std::move(entry.freed);
                freed_entries.erase(entry_it);
            }
        }
        blobs_to_remove.submitForRemoval(next);
    }

    for (const auto & [claim, ack] : acks)
    {
        try
        {
            client->ackFreed(ack);
        }
        catch (...)
        {
            /// The claim runs out and InodeDB hands the entries out again; deleting their blobs twice is harmless.
            tryLogCurrentException(log, "Acknowledging freed files in InodeDB");
        }
    }
    return count;
}

bool MetadataStorageFromInodeDB::hasPendingRemovalBlobs(const StoredObjects & blobs) const
{
    std::lock_guard lock(removal_mutex);
    return blobs_to_remove.containsAny(blobs);
}

int64_t MetadataStorageFromInodeDB::getDeadBlobsQueueEstimate()
{
    std::lock_guard lock(removal_mutex);
    return std::ssize(blobs_to_remove);
}


bool MetadataStorageFromInodeDBTransaction::Intent::readsState() const
{
    switch (kind)
    {
        case IntentKind::AddBlob:
        case IntentKind::TruncateFile:
        case IntentKind::SetReadOnly:
            return true;
        case IntentKind::UnlinkFile:
            return !remove_objects;
        case IntentKind::RemoveRecursive:
            return static_cast<bool>(predicate);
        default:
            return false;
    }
}

std::string MetadataStorageFromInodeDBTransaction::Intent::describe() const
{
    if (path_to.empty())
        return fmt::format("{}(`{}`)", magic_enum::enum_name(kind), path);
    return fmt::format("{}(`{}`, `{}`)", magic_enum::enum_name(kind), path, path_to);
}

MetadataStorageFromInodeDBTransaction::MetadataStorageFromInodeDBTransaction(MetadataStorageFromInodeDB & storage_)
    : storage(storage_)
{
}

namespace
{

void addPredicate(pb::Pred & pred, const InodeDBCommitOptions::Predicate & extra);

pb::Op extraOp(const InodeDBCommitOptions::Op & extra)
{
    pb::Op op;
    if (extra.kind == InodeDBCommitOptions::Kind::Remove)
        op = removeAllOp(extra.path);
    else if (extra.kind == InodeDBCommitOptions::Kind::Mkdir)
        op = mkdirOp(extra.path, /*parents=*/ true);
    else if (extra.kind == InodeDBCommitOptions::Kind::Rename)
        op = renameOp(extra.path, extra.to, pb::NO_REPLACE);
    else
    {
        pb::PutMode mode = pb::UPSERT;
        switch (extra.mode)
        {
            case InodeDBCommitOptions::Mode::Create: mode = pb::CREATE; break;
            case InodeDBCommitOptions::Mode::Replace: mode = pb::REPLACE; break;
            case InodeDBCommitOptions::Mode::Upsert: mode = pb::UPSERT; break;
        }
        op = putOp(extra.path, extra.value, mode);
    }
    for (const auto & pred : extra.cond)
        addPredicate(*op.add_cond(), pred);
    return op;
}

void addPredicate(pb::Pred & pred, const InodeDBCommitOptions::Predicate & extra)
{
    switch (extra.check)
    {
        case InodeDBCommitOptions::Check::Exists: pred.set_exists(extra.path); break;
        case InodeDBCommitOptions::Check::Absent: pred.set_absent(extra.path); break;
        case InodeDBCommitOptions::Check::IsDir: pred.set_is_dir(extra.path); break;
        case InodeDBCommitOptions::Check::VersionIs:
            pred.mutable_version_is()->set_path(extra.path);
            pred.mutable_version_is()->set_version(extra.version);
            break;
    }
}

void noteFailure(InodeDBCommitOutcome & outcome, InodeDBCommitStatus status, const std::string & path)
{
    if (status == InodeDBCommitStatus::Committed)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "An op or predicate on `{}` that may fail names no failure status", path);
    /// The first failure decides the status; later ones of the same status add their paths.
    if (outcome.status == InodeDBCommitStatus::Committed)
        outcome.status = status;
    if (outcome.status == status && std::ranges::find(outcome.failed_paths, path) == outcome.failed_paths.end())
        outcome.failed_paths.push_back(path);
}

}

MetadataStorageFromInodeDBTransaction::AttemptResult MetadataStorageFromInodeDBTransaction::attempt(
    bool read_state, bool & frees, const InodeDBCommitOptions * extra, InodeDBCommitOutcome & outcome)
{
    std::unique_lock exclusive(storage.commit_mutex, std::defer_lock);
    std::shared_lock shared(storage.commit_mutex, std::defer_lock);
    if (read_state)
        exclusive.lock();
    else
        shared.lock();

    InodeDBBatchBuilder builder(storage, read_state, static_cast<UInt64>(time(nullptr)));
    pb::CommitRequest request;
    try
    {
        for (size_t i = 0; i < intents.size(); ++i)
            builder.apply(intents[i], i);
        request = builder.finish();
    }
    catch (...)
    {
        storage.removeUnappliedBlobs(builder.blobsWritten());
        throw;
    }

    /// The extras follow the batch's own ops and its owner fence.
    const int extra_ops_from = request.ops_size();
    const int extra_when_from = request.when_size();
    if (extra)
    {
        for (const auto & op : extra->ops)
            *request.add_ops() = extraOp(op);
        for (const auto & pred : extra->when)
            addPredicate(*request.add_when(), pred);
    }
    outcome = {};

    /// If the call itself fails, the batch may have applied, so the blobs it wrote stay, and nothing cached
    /// can be trusted.
    pb::CommitResponse response;
    try
    {
        response = storage.client->commit(request);
    }
    catch (...)
    {
        storage.cache->clear();
        throw;
    }
    if (response.outcomes_size() != request.ops_size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "InodeDB answered a batch of {} ops with {} outcomes", request.ops_size(), response.outcomes_size());

    for (int i = 0; i < response.outcomes_size(); ++i)
    {
        const auto & op_outcome = response.outcomes(i);
        if (!op_outcome.has_skipped() || op_outcome.skipped().reason_case() == pb::Skipped::kAborted)
            continue;

        const auto & skipped = op_outcome.skipped();
        if (skipped.reason_case() == pb::Skipped::kWhen && static_cast<int>(skipped.when()) >= extra_when_from)
        {
            const auto & pred = extra->when.at(skipped.when() - extra_when_from);
            noteFailure(outcome, pred.failure, pred.path);
            continue;
        }
        if (i >= extra_ops_from && skipped.reason_case() == pb::Skipped::kError)
        {
            const auto & extra_op = extra->ops.at(i - extra_ops_from);
            noteFailure(outcome, extra_op.failure, extra_op.path);
            continue;
        }
        if (i >= extra_ops_from && skipped.reason_case() == pb::Skipped::kCond)
        {
            const auto & pred = extra->ops.at(i - extra_ops_from).cond.at(skipped.cond());
            noteFailure(outcome, pred.failure, pred.path);
            continue;
        }

        /// The batch is all-or-nothing, so nothing applied.
        storage.removeUnappliedBlobs(builder.blobsWritten());

        if (skipped.reason_case() == pb::Skipped::kWhen)
            throw Exception(ErrorCodes::TABLE_IS_READ_ONLY,
                "{} lost ownership: another server took over the disk, so this one can no longer write", storage.description);

        const auto & op = request.ops(i);
        if (skipped.reason_case() != pb::Skipped::kError)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "InodeDB skipped op {} (`{}`) for {}", i, opPath(op), skipped.ShortDebugString());

        if (builder.isBlindCreate(i) && skipped.error() == pb::EXISTS)
            return AttemptResult::CreatedExisting;

        auto intent_index = builder.intentOfOp(i);

        /// Like `rm -f`, an `if_exists` removal of a path under a file removes nothing.
        const bool removes_if_exists = (op.has_remove_all() && op.remove_all().if_exists()) || (op.has_unlink() && op.unlink().if_exists());
        if (intent_index && removes_if_exists && skipped.error() == pb::NOT_DIR)
        {
            intents[*intent_index].skip = true;
            return AttemptResult::SkippedRemoval;
        }

        if (!intent_index)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "InodeDB refused a tombstone at `{}` with {}", opPath(op), pb::Error_Name(skipped.error()));

        const auto & intent = intents[*intent_index];
        throw Exception(errorCodeOf(op, skipped.error(), intent.kind == IntentKind::MoveDirectory),
            "{} failed: InodeDB reports {} at `{}`", intent.describe(), pb::Error_Name(skipped.error()), opPath(op));
    }

    if (outcome.status != InodeDBCommitStatus::Committed)
    {
        storage.removeUnappliedBlobs(builder.blobsWritten());
        ProfileEvents::increment(ProfileEvents::InodeDBCommitConflicts);
        return AttemptResult::ExtraFailed;
    }

    storage.cache->invalidate(request, response);
    frees = builder.freesAnything();
    outcome.version = response.version();
    return AttemptResult::Committed;
}

void MetadataStorageFromInodeDBTransaction::commit(const TransactionCommitOptionsVariant & options)
{
    if (!std::holds_alternative<NoCommitOptions>(options))
        throwNotImplemented();
    commitWith(nullptr);
}

InodeDBCommitOutcome MetadataStorageFromInodeDBTransaction::commitWith(const InodeDBCommitOptions * extra)
{
    if (intents.empty() && (!extra || (extra->ops.empty() && extra->when.empty())))
        return {};
    /// `DiskObjectStorage::createDirectories` commits even for directories that exist, as when a replica attaches a
    /// database from this disk. Such a transaction changes nothing, so a read-only disk commits it, as `local` does.
    if (!extra && storage.isReadOnly() && onlyCreatesExistingDirectories())
        return {};
    /// A writer that shares the disk attaches databases an exclusive writer created: it creates directories that
    /// exist, and removes a temporary database's directory that is absent. Neither changes anything, so the writer
    /// sends no batch, and attaching costs it reads alone.
    if (!extra && storage.settings.shared && changesNothing())
        return {};
    storage.assertWritable();

    /// A shared writer's server changes no definitions: an exclusive writer runs DDL. Every server at startup creates
    /// the directories definitions live in and removes leftover temporary files, which changes none.
    if (storage.settings.shared)
    {
        for (const auto & intent : intents)
        {
            if (intent.kind == IntentKind::CreateDirectory || intent.kind == IntentKind::CreateDirectoryRecursive
                || (intent.kind == IntentKind::UnlinkFile && intent.if_exists))
                continue;
            if (holdsDefinitions(intent.path) || (!intent.path_to.empty() && holdsDefinitions(intent.path_to)))
                throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                    "Changing definitions needs an exclusive writer: {} shares its disk with other writers (`inodedb_writer_mode` = 'shared'), "
                    "and {} changes `{}`", storage.description, magic_enum::enum_name(intent.kind), intent.path);
        }
    }

    bool read_state = std::ranges::any_of(intents, [](const Intent & intent) { return intent.readsState(); });
    bool frees = false;
    InodeDBCommitOutcome outcome;
    while (true)
    {
        auto result = attempt(read_state, frees, extra, outcome);
        if (result == AttemptResult::ExtraFailed)
            return outcome;
        if (result == AttemptResult::Committed)
            break;
        if (result == AttemptResult::CreatedExisting)
        {
            if (read_state)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "A transaction that read the files it writes still found one it assumed absent");
            read_state = true;
        }
        /// `SkippedRemoval` marked one intent, so the loop ends within as many attempts as there are intents.
    }

    if (!frees)
        return outcome;

    /// Queue the blobs this commit freed now, so the disk can wait for their removal as with local metadata.
    /// The commit stands either way: a failure here leaves them to the next pass.
    try
    {
        size_t claimed = 0;
        submitted_for_removal = storage.pullFreed(claimed);
    }
    catch (...)
    {
        tryLogCurrentException(storage.log, "Claiming freed files after a commit");
    }
    return outcome;
}

bool MetadataStorageFromInodeDBTransaction::onlyCreatesExistingDirectories() const
{
    return std::ranges::all_of(intents, [this](const Intent & intent)
    {
        return (intent.kind == IntentKind::CreateDirectory || intent.kind == IntentKind::CreateDirectoryRecursive)
            && storage.existsDirectory(intent.path);
    });
}

bool MetadataStorageFromInodeDBTransaction::changesNothing() const
{
    return std::ranges::all_of(intents, [this](const Intent & intent)
    {
        switch (intent.kind)
        {
            case IntentKind::CreateDirectory:
            case IntentKind::CreateDirectoryRecursive:
                return storage.existsDirectory(intent.path);
            case IntentKind::RemoveRecursive:
                return !storage.existsFileOrDirectory(intent.path);
            default:
                return false;
        }
    });
}

TransactionCommitOutcomeVariant MetadataStorageFromInodeDBTransaction::tryCommit(const TransactionCommitOptionsVariant & options)
{
    if (const auto * extra = std::get_if<InodeDBCommitOptions>(&options))
        return commitWith(extra);
    commit(options);
    return true;
}

void MetadataStorageFromInodeDBTransaction::writeStringToFile(const std::string & path, const std::string & data)
{
    intents.push_back({.kind = IntentKind::WriteString, .path = path, .data = data});
}

void MetadataStorageFromInodeDBTransaction::writeInlineDataToFile(const std::string & path, const std::string & data)
{
    intents.push_back({.kind = IntentKind::WriteInline, .path = path, .data = data});
}

void MetadataStorageFromInodeDBTransaction::setLastModified(const std::string & path, const Poco::Timestamp & timestamp)
{
    intents.push_back({.kind = IntentKind::SetLastModified, .path = path, .number = static_cast<UInt64>(timestamp.epochMicroseconds() / 1000)});
}

void MetadataStorageFromInodeDBTransaction::chmod(const String & path, mode_t mode)
{
    intents.push_back({.kind = IntentKind::Chmod, .path = path, .number = mode});
}

void MetadataStorageFromInodeDBTransaction::setReadOnly(const std::string & path)
{
    intents.push_back({.kind = IntentKind::SetReadOnly, .path = path});
}

void MetadataStorageFromInodeDBTransaction::unlinkFile(const std::string & path, bool if_exists, bool should_remove_objects)
{
    intents.push_back({.kind = IntentKind::UnlinkFile, .path = path, .if_exists = if_exists, .remove_objects = should_remove_objects});
}

void MetadataStorageFromInodeDBTransaction::createDirectory(const std::string & path)
{
    intents.push_back({.kind = IntentKind::CreateDirectory, .path = path});
}

void MetadataStorageFromInodeDBTransaction::createDirectoryRecursive(const std::string & path)
{
    intents.push_back({.kind = IntentKind::CreateDirectoryRecursive, .path = path});
}

void MetadataStorageFromInodeDBTransaction::removeDirectory(const std::string & path)
{
    intents.push_back({.kind = IntentKind::RemoveDirectory, .path = path});
}

void MetadataStorageFromInodeDBTransaction::removeRecursive(const std::string & path, const ShouldRemoveObjectsPredicate & should_remove_objects)
{
    intents.push_back({.kind = IntentKind::RemoveRecursive, .path = path, .predicate = should_remove_objects});
}

void MetadataStorageFromInodeDBTransaction::createHardLink(const std::string & path_from, const std::string & path_to)
{
    intents.push_back({.kind = IntentKind::CreateHardLink, .path = path_from, .path_to = path_to});
}

void MetadataStorageFromInodeDBTransaction::moveFile(const std::string & path_from, const std::string & path_to)
{
    intents.push_back({.kind = IntentKind::MoveFile, .path = path_from, .path_to = path_to});
}

void MetadataStorageFromInodeDBTransaction::moveDirectory(const std::string & path_from, const std::string & path_to)
{
    intents.push_back({.kind = IntentKind::MoveDirectory, .path = path_from, .path_to = path_to});
}

void MetadataStorageFromInodeDBTransaction::replaceFile(const std::string & path_from, const std::string & path_to)
{
    intents.push_back({.kind = IntentKind::ReplaceFile, .path = path_from, .path_to = path_to});
}

ObjectStorageKey MetadataStorageFromInodeDBTransaction::generateObjectKeyForPath(const std::string & /*path*/)
{
    return storage.key_generator->generate();
}

void MetadataStorageFromInodeDBTransaction::createMetadataFile(const std::string & path, const StoredObjects & objects)
{
    intents.push_back({.kind = IntentKind::CreateMetadataFile, .path = path, .objects = objects});
}

void MetadataStorageFromInodeDBTransaction::addBlobToMetadata(const std::string & path, const StoredObject & object)
{
    intents.push_back({.kind = IntentKind::AddBlob, .path = path, .objects = {object}});
}

void MetadataStorageFromInodeDBTransaction::truncateFile(const std::string & path, size_t size)
{
    intents.push_back({.kind = IntentKind::TruncateFile, .path = path, .number = size});
}

}

#endif
