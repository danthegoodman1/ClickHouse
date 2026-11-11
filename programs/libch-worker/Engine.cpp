#include "Engine.h"

#include <Access/AccessControl.h>
#include <Access/MemoryAccessStorage.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Databases/DatabaseFactory.h>
#include <Disks/IDisk.h>
#include <Disks/registerDisks.h>
#include <Formats/registerFormats.h>
#include <Functions/registerFunctions.h>
#include <IO/SharedThreadPools.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/SecretArgumentsRegistry.h>
#include <Interpreters/loadMetadata.h>
#include <Interpreters/registerInterpreters.h>
#include <Parsers/SecretArguments.h>
#include <Parsers/registerParsers.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMergeTree.h>
#include <Common/AsyncLoader.h>
#include <Common/Exception.h>
#include <Common/ThreadPool.h>
#include <Common/logger_useful.h>

#include <Poco/AutoPtr.h>
#include <Poco/SAX/InputSource.h>
#include <Poco/Util/XMLConfiguration.h>

#include <filesystem>
#include <sstream>

#include <unistd.h>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

void registerDatabaseAtomic(DatabaseFactory & factory);

void registerStorageMergeTree(StorageFactory & factory);
void registerStorageNull(StorageFactory & factory);
void registerStorageView(StorageFactory & factory);
void registerStorageMaterializedView(StorageFactory & factory);

namespace LibchWorker
{

namespace
{

/// What a tenant's tables and views are made of. Table functions, dictionaries, the other database engines and
/// every table engine that reaches outside the tenant's disk stay unregistered, so a definition that names one
/// fails to attach.
void registerComponents()
{
    registerInterpreters();
    registerParsers();
    /// Error messages format the queries they quote, which hides what the engines mark as secret.
    setSecretArgumentsFinder(&SecretArgumentsRegistry::instance());
    registerFunctions();
    registerAggregateFunctions();

    registerDatabaseAtomic(DatabaseFactory::instance());

    auto & storages = StorageFactory::instance();
    registerStorageMergeTree(storages);
    registerStorageNull(storages);
    registerStorageView(storages);
    registerStorageMaterializedView(storages);

    registerDisks(/* global_skip_access_check= */ true);
    registerFormats();
}

/// The server configuration of a worker: the request's settings document, then what the worker itself decides.
Poco::AutoPtr<Poco::Util::XMLConfiguration> makeConfiguration(const libch::v1::Tenant & tenant, const std::string & settings_xml)
{
    std::stringstream document; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    document << "<clickhouse>" << settings_xml << "</clickhouse>";
    Poco::XML::InputSource source{document};
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config{new Poco::Util::XMLConfiguration{&source}};

    if (tenant.inodedb_endpoint().empty() || tenant.inodedb_key().empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "A request names the tenant's InodeDB store and key");
    const auto & s3 = tenant.s3();
    if (s3.endpoint().empty() || s3.bucket().empty() || s3.prefix().empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "A request names the tenant's S3 endpoint, bucket and prefix");

    /// The tenant's disk, as a stock shared writer of the disk configures it.
    const std::string disk = fmt::format("storage_configuration.disks.{}", TENANT_DISK);
    std::string endpoint = s3.endpoint();
    while (endpoint.ends_with('/'))
        endpoint.pop_back();
    config->setString(disk + ".type", "object_storage");
    config->setString(disk + ".object_storage_type", "s3");
    config->setString(disk + ".metadata_type", "inodedb");
    config->setString(disk + ".endpoint", fmt::format("{}/{}/{}/", endpoint, s3.bucket(), s3.prefix()));
    config->setString(disk + ".access_key_id", s3.access_key());
    config->setString(disk + ".secret_access_key", s3.secret_key());
    if (!s3.region().empty())
        config->setString(disk + ".region", s3.region());
    config->setString(disk + ".inodedb_endpoint", tenant.inodedb_endpoint());
    config->setString(disk + ".inodedb_key", tenant.inodedb_key());
    config->setString(disk + ".inodedb_read_only", "0");
    config->setString(disk + ".inodedb_writer_mode", "shared");
    config->setString(fmt::format("storage_configuration.policies.{}.volumes.main.disk", TENANT_DISK), TENANT_DISK);
    config->setString("merge_tree.storage_policy", TENANT_DISK);
    config->setString("database_disk.disk", TENANT_DISK);

    /// An operation runs in the thread that serves it. The engine's pools exist because its code names them, so
    /// each is as small as it goes.
    for (const auto * pool : {"background_pool_size", "background_move_pool_size", "background_fetches_pool_size", "background_common_pool_size",
             "background_schedule_pool_initial_size", "tables_loader_foreground_pool_size", "tables_loader_background_pool_size"})
        config->setString(pool, "1");

    /// Queries run as a default user with a default profile, which the settings document may fill.
    if (!config->has("profiles.default"))
        config->setString("profiles.default", "");
    config->setString("users.default.password", "");
    config->setString("users.default.networks.ip", "::/0");
    config->setString("users.default.profile", "default");
    config->setString("users.default.quota", "default");
    if (!config->has("quotas.default"))
        config->setString("quotas.default", "");

    return config;
}

}

Engine::Engine(std::string path_)
    : path(std::move(path_))
{
    registerComponents();

    GlobalThreadPool::initialize();
#define INITIALIZE_STATIC_THREAD_POOL(SUFFIX, NAME, METRIC) get##SUFFIX##ThreadPool().initializeWithDefaultSettingsIfNotInitialized();
    APPLY_FOR_STATIC_THREAD_POOLS(INITIALIZE_STATIC_THREAD_POOL)
#undef INITIALIZE_STATIC_THREAD_POOL

    shared_context.emplace(Context::createShared());
    global_context = Context::createGlobal(shared_context->get());
    global_context->makeGlobalContext();

    /// From here on a `MergeTree` table loads no parts and starts no background work.
    StorageMergeTree::setOperationMode(true);
}

Engine::~Engine()
{
    if (global_context)
    {
        global_context->shutdown();
        global_context.reset();
    }
    shared_context.reset();
    if (owns_path)
    {
        std::error_code ignored;
        fs::remove_all(path, ignored);
    }
}

void Engine::attachTenant(const libch::v1::Tenant & tenant, const std::string & settings_xml)
{
    if (tenant_attached)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A worker serves one tenant");

    /// The context reads its configuration from here on, so the configuration comes first.
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = makeConfiguration(tenant, settings_xml);
    global_context->setConfig(config);
    global_context->setApplicationType(Context::ApplicationType::LOCAL);

    if (path.empty())
    {
        path = fs::temp_directory_path() / fmt::format("libch-worker-{}", getpid());
        owns_path = true;
    }
    fs::create_directories(path);
    global_context->setPath(fs::path(path) / "");
    global_context->setTemporaryStoragePath(fs::path(path) / "tmp" / "", 0);
    global_context->setFlagsPath(fs::path(path) / "flags" / "");

    global_context->getProcessList().setMaxSize(0);

    auto & access_control = global_context->getAccessControl();
    access_control.setNoPasswordAllowed(true);
    access_control.setPlaintextPasswordAllowed(true);
    global_context->setUsersConfig(config);
    access_control.addMemoryStorage(MemoryAccessStorage::STORAGE_TYPE, /* allow_backup= */ false);
    global_context->setDefaultProfiles(*config);

    /// Projections are built in the temporary database.
    DatabaseCatalog::instance().initializeAndLoadTemporaryDatabase();

    /// Creating the disk joins its shared writers, which fences an exclusive writer, and takes the reader lease.
    tenant_attached = true;
    global_context->getDisk(TENANT_DISK);

    /// The databases live on the tenant's disk, and loading them attaches every table from its definition.
    waitLoad(loadMetadata(global_context));
}

void Engine::detachTenant()
{
    if (!tenant_attached)
        return;
    tenant_attached = false;
    global_context->getDisk(TENANT_DISK)->shutdown();
}

ContextMutablePtr Engine::makeQueryContext() const
{
    auto context = Context::createCopy(global_context);
    context->makeQueryContext();
    context->setCurrentQueryId("");
    return context;
}

}

}
