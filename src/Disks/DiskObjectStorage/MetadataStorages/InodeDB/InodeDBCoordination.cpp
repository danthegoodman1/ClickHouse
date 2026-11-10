#include <Disks/DiskObjectStorage/MetadataStorages/InodeDB/InodeDBCoordination.h>

#include "config.h"

#if USE_GRPC
#include <Disks/DiskObjectStorage/MetadataStorages/Cache/MetadataStorageFromCacheObjectStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/InodeDB/MetadataStorageFromInodeDB.h>
#endif

#include <Common/Exception.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// A disk path without leading or trailing slashes, and empty for the disk's root.
std::string normalizeDiskPath(const std::string & path)
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

std::string under(std::string_view root, const std::string & disk_path)
{
    std::string normal = normalizeDiskPath(disk_path);
    return normal.empty() ? std::string(root) : fmt::format("{}/{}", root, normal);
}

}

std::string IInodeDBCoordination::coordPath(const std::string & disk_path)
{
    return under("coord", disk_path);
}

std::string IInodeDBCoordination::treePath(const std::string & disk_path)
{
    return under("tree", disk_path);
}

InodeDBCoordinationPtr getInodeDBCoordination(const DiskPtr & disk)
{
#if USE_GRPC
    if (!disk || disk->getDataSourceDescription().metadata_type != MetadataStorageType::InodeDB)
        return nullptr;
    auto storage = disk->getMetadataStorage();
    if (auto cached = std::dynamic_pointer_cast<MetadataStorageFromCacheObjectStorage>(storage))
        storage = cached->getUnderlying();
    return std::dynamic_pointer_cast<MetadataStorageFromInodeDB>(storage);
#else
    return nullptr;
#endif
}

}
