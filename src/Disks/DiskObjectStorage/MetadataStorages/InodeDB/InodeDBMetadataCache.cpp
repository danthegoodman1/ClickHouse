#include <Disks/DiskObjectStorage/MetadataStorages/InodeDB/InodeDBMetadataCache.h>

#if USE_GRPC

namespace DB::InodeDB
{

namespace
{

std::string parentOf(const std::string & path)
{
    auto slash = path.rfind('/');
    return slash == std::string::npos ? std::string{} : path.substr(0, slash);
}

std::string nameOf(const std::string & path)
{
    auto slash = path.rfind('/');
    return slash == std::string::npos ? path : path.substr(slash + 1);
}

bool isUnder(const std::string & path, const std::string & prefix)
{
    return path.starts_with(prefix) && (path.size() == prefix.size() || path[prefix.size()] == '/');
}

/// The paths an op names, whose entries or listings it may change.
std::vector<const std::string *> pathsOf(const pb::Op & op)
{
    switch (op.kind_case())
    {
        case pb::Op::kMkdir: return {&op.mkdir().path()};
        case pb::Op::kPut: return {&op.put().path()};
        case pb::Op::kLink: return {&op.link().from(), &op.link().to()};
        case pb::Op::kUnlink: return {&op.unlink().path()};
        case pb::Op::kRmdir: return {&op.rmdir().path()};
        case pb::Op::kRemoveAll: return {&op.remove_all().path()};
        case pb::Op::kRename: return {&op.rename().from(), &op.rename().to()};
        case pb::Op::kSetAttr: return {&op.set_attr().path()};
        case pb::Op::KIND_NOT_SET: return {};
    }
    return {};
}

size_t sizeOf(const std::string & path, const MetadataCache::File & file)
{
    return path.size() + file.value.size() + 128;
}

}

std::optional<MetadataCache::File> MetadataCache::file(const std::string & path) const
{
    std::lock_guard lock(mutex);
    if (auto it = files.find(path); it != files.end())
        return it->second;
    return std::nullopt;
}

MetadataCache::Presence MetadataCache::presence(const std::string & path) const
{
    std::lock_guard lock(mutex);
    if (files.contains(path))
        return Presence::File;
    auto it = listings.find(parentOf(path));
    if (it == listings.end())
        return Presence::Unknown;
    const std::string name = nameOf(path);
    for (const auto & entry : it->second.entries)
        if (entry.name() == name)
            return entry.kind() == pb::DIR ? Presence::Directory : Presence::File;
    return Presence::Absent;
}

std::optional<Listing> MetadataCache::listing(const std::string & dir) const
{
    std::lock_guard lock(mutex);
    if (auto it = listings.find(dir); it != listings.end())
        return it->second;
    return std::nullopt;
}

void MetadataCache::putListing(const std::string & dir, const Listing & listing, UInt64 version)
{
    if (max_bytes == 0 || listing.error)
        return;
    std::lock_guard lock(mutex);
    if (version < newest_commit)
        return;

    Listing names;
    names.entries.reserve(listing.entries.size());
    for (const auto & entry : listing.entries)
    {
        auto & name = names.entries.emplace_back();
        name.set_name(entry.name());
        name.set_kind(entry.kind());
        if (entry.kind() == pb::FILE && entry.has_inode() && entry.has_value())
        {
            pb::Inode inode = entry.inode();
            *inode.mutable_handle() = entry.handle();
            putFileUnlocked(dir.empty() ? entry.name() : dir + "/" + entry.name(), File{std::move(inode), entry.value()});
        }
    }
    bytes += dir.size() + 64 * names.entries.size();
    listings[dir] = std::move(names);
    fitUnlocked();
}

void MetadataCache::putFile(const std::string & path, File file, UInt64 version)
{
    if (max_bytes == 0)
        return;
    std::lock_guard lock(mutex);
    if (version < newest_commit)
        return;
    putFileUnlocked(path, std::move(file));
    fitUnlocked();
}

void MetadataCache::putFileUnlocked(const std::string & path, File file)
{
    eraseFileUnlocked(path);
    bytes += sizeOf(path, file);
    paths_of_inode[file.inode.handle().ino()].insert(path);
    files.emplace(path, std::move(file));
}

void MetadataCache::eraseFileUnlocked(const std::string & path)
{
    auto it = files.find(path);
    if (it == files.end())
        return;
    bytes -= sizeOf(path, it->second);
    if (auto names = paths_of_inode.find(it->second.inode.handle().ino()); names != paths_of_inode.end())
    {
        names->second.erase(path);
        if (names->second.empty())
            paths_of_inode.erase(names);
    }
    files.erase(it);
}

void MetadataCache::eraseSubtreeUnlocked(const std::string & path)
{
    for (auto it = files.lower_bound(path); it != files.end() && isUnder(it->first, path);)
    {
        auto next = std::next(it);
        eraseFileUnlocked(it->first);
        it = next;
    }
    for (auto it = listings.lower_bound(path); it != listings.end() && isUnder(it->first, path);)
        it = listings.erase(it);
    listings.erase(parentOf(path));
}

void MetadataCache::invalidate(const pb::CommitRequest & request, const pb::CommitResponse & response)
{
    std::lock_guard lock(mutex);
    newest_commit = std::max(newest_commit, response.version());
    for (const auto & op : request.ops())
        for (const auto * path : pathsOf(op))
            eraseSubtreeUnlocked(*path);

    /// Another name of a changed inode holds its old value or attributes.
    for (const auto & outcome : response.outcomes())
    {
        if (!outcome.has_applied() || !outcome.applied().has_handle())
            continue;
        auto names = paths_of_inode.find(outcome.applied().handle().ino());
        if (names == paths_of_inode.end())
            continue;
        for (const auto & path : std::set<std::string>(names->second))
            eraseFileUnlocked(path);
    }
}

void MetadataCache::clear()
{
    std::lock_guard lock(mutex);
    files.clear();
    paths_of_inode.clear();
    listings.clear();
    bytes = 0;
}

void MetadataCache::forgetListings()
{
    std::lock_guard lock(mutex);
    for (const auto & [dir, listing] : listings)
        bytes -= dir.size() + 64 * listing.entries.size();
    listings.clear();
}

void MetadataCache::fitUnlocked()
{
    if (bytes <= max_bytes)
        return;
    files.clear();
    paths_of_inode.clear();
    listings.clear();
    bytes = 0;
}

}

#endif
