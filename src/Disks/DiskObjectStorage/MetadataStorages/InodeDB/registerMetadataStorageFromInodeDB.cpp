#include <Disks/DiskObjectStorage/MetadataStorages/InodeDB/registerMetadataStorageFromInodeDB.h>

#include "config.h"

#if USE_GRPC

#include <Disks/DiskObjectStorage/MetadataStorages/InodeDB/MetadataStorageFromInodeDB.h>
#include <Disks/DiskObjectStorage/MetadataStorages/MetadataStorageFactory.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <Common/Exception.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <boost/algorithm/string/trim.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int NO_ELEMENTS_IN_CONFIG;
}

namespace
{

std::string readKey(const Poco::Util::AbstractConfiguration & config, const std::string & config_prefix)
{
    if (config.has(config_prefix + ".inodedb_key"))
        return config.getString(config_prefix + ".inodedb_key");

    if (!config.has(config_prefix + ".inodedb_key_file"))
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Metadata type `inodedb` needs `inodedb_key` or `inodedb_key_file`");

    ReadBufferFromFile file(config.getString(config_prefix + ".inodedb_key_file"));
    std::string key;
    readStringUntilEOF(key, file);
    boost::algorithm::trim(key);
    return key;
}

}

void registerMetadataStorageFromInodeDB(MetadataStorageFactory & factory)
{
    factory.registerMetadataStorageType("inodedb", [](
        const std::string & /* name */,
        const Poco::Util::AbstractConfiguration & config,
        const std::string & config_prefix,
        const ClusterConfigurationPtr & cluster,
        const ObjectStorageRouterPtr & object_storages,
        bool /* run_local_paths_check */) -> MetadataStoragePtr
    {
        if (cluster->getConfiguration().size() > 1)
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Metadata type `inodedb` supports only single location clusters");

        if (!config.has(config_prefix + ".inodedb_endpoint"))
            throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Metadata type `inodedb` needs `inodedb_endpoint`");

        InodeDB::ClientSettings client_settings;
        client_settings.endpoint = config.getString(config_prefix + ".inodedb_endpoint");
        client_settings.key = readKey(config, config_prefix);
        client_settings.attempt_timeout = std::chrono::milliseconds(config.getUInt64(config_prefix + ".inodedb_attempt_timeout_ms", 10'000));
        client_settings.retry_timeout = std::chrono::milliseconds(config.getUInt64(config_prefix + ".inodedb_retry_timeout_ms", 60'000));

        auto object_storage = object_storages->takePointingTo(cluster->getLocalLocation());

        InodeDBMetadataSettings settings;
        settings.read_only = config.getBool(config_prefix + ".inodedb_read_only", false) || object_storage->isReadOnly();
        settings.spill_threshold = config.getUInt64(config_prefix + ".inodedb_spill_threshold", settings.spill_threshold);
        settings.inline_file_max_bytes = config.getUInt64(config_prefix + ".inodedb_inline_file_max_bytes", settings.inline_file_max_bytes);
        settings.commit_max_bytes = config.getUInt64(config_prefix + ".inodedb_commit_max_bytes", settings.commit_max_bytes);
        settings.lease_lag = std::chrono::milliseconds(config.getUInt64(config_prefix + ".inodedb_lease_lag_ms", settings.lease_lag.count()));
        settings.cache_bytes = config.getUInt64(config_prefix + ".inodedb_cache_bytes", settings.cache_bytes);
        const auto mode = config.getString(config_prefix + ".inodedb_writer_mode", "exclusive");
        if (mode != "exclusive" && mode != "shared")
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "`inodedb_writer_mode` is `exclusive` or `shared`, not `{}`", mode);
        settings.shared = mode == "shared";
        if (settings.shared && settings.read_only)
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "A read-only `inodedb` disk takes no `inodedb_writer_mode`");
        settings.token_ttl = std::chrono::seconds(config.getUInt64(config_prefix + ".inodedb_deduplication_token_ttl_seconds", settings.token_ttl.count()));
        settings.claim_ttl = std::chrono::milliseconds(config.getUInt64(config_prefix + ".inodedb_claim_ttl_ms", settings.claim_ttl.count()));
        if (settings.spill_threshold == 0 || settings.spill_threshold > 48 * 1024)
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "`inodedb_spill_threshold` must be between 1 and 49152 bytes, since an InodeDB value holds 64 KiB");
        if (settings.inline_file_max_bytes >= settings.spill_threshold)
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "`inodedb_inline_file_max_bytes` must be smaller than `inodedb_spill_threshold`");
        if (settings.commit_max_bytes >= 8 * 1024 * 1024)
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "`inodedb_commit_max_bytes` must be smaller than 8 MiB, the most one InodeDB commit holds");

        auto key_compatibility_prefix = config.getString(config_prefix + ".key_compatibility_prefix", object_storage->getCommonKeyPrefix());

        return std::make_shared<MetadataStorageFromInodeDB>(
            std::make_shared<InodeDB::Client>(std::move(client_settings)),
            object_storage,
            std::move(key_compatibility_prefix),
            object_storage->createKeyGenerator(),
            settings);
    });
}

}

#else

namespace DB
{

void registerMetadataStorageFromInodeDB(MetadataStorageFactory &)
{
}

}

#endif
