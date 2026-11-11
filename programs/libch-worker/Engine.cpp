#include "Engine.h"

#include <Access/AccessControl.h>
#include <Access/MemoryAccessStorage.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Compression/CompressionFactory.h>
#include <DataTypes/DataTypeFactory.h>
#include <Databases/DatabaseFactory.h>
#include <Disks/DiskObjectStorage/DiskObjectStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
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
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageMergeTree.h>
#include <Common/AsyncLoader.h>
#include <Common/PoolId.h>
#include <Common/Config/ConfigHelper.h>
#include <Common/DateLUT.h>
#include <Common/ErrnoException.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/MemoryTracker.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadPool.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

#include <Poco/AutoPtr.h>
#include <Poco/SAX/InputSource.h>
#include <Poco/Util/XMLConfiguration.h>

#include <filesystem>
#include <sstream>

#include <fcntl.h>
#include <unistd.h>

#if defined(OS_LINUX)
#    include <sys/inotify.h>
#endif

namespace fs = std::filesystem;

namespace ProfileEvents
{
    extern const Event InodeDBFreedClaimed;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_OPEN_FILE;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
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
Poco::AutoPtr<Poco::Util::XMLConfiguration> makeConfiguration(const libch::v1::Tenant & tenant, const std::string & settings_xml, Engine::Needs needs)
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
    /// A disk removes freed blobs in rounds, one of which a transaction waits for and one of which runs at
    /// shutdown. Only an operation that asks for the removal pays for them.
    if (!needs.blob_removal)
    {
        config->setString(disk + ".data_background_cleanup.enabled", "false");
        config->setString(disk + ".wait_for_blob_removal", "false");
    }
    config->setString(fmt::format("storage_configuration.policies.{}.volumes.main.disk", TENANT_DISK), TENANT_DISK);
    config->setString("merge_tree.storage_policy", TENANT_DISK);
    config->setString("database_disk.disk", TENANT_DISK);

    /// An operation runs in the thread that serves it. The engine's pools exist because its code names them, so
    /// each is as small as it goes.
    for (const auto * pool : {"background_move_pool_size", "background_fetches_pool_size", "background_common_pool_size",
             "background_schedule_pool_initial_size", "tables_loader_foreground_pool_size", "tables_loader_background_pool_size"})
        config->setString(pool, "1");
    /// The merge selector lowers the size of the merges it proposes as the merge pool fills, so a plan keeps the
    /// pool a server has, idle.
    if (!needs.merge_selection)
        config->setString("background_pool_size", "1");

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

/// Lets a test resume a worker it holds at a pausing failpoint. For each pausing failpoint that is on, a thread
/// waits until the worker reaches it, creates `{name}.paused` in `directory`, waits for `{name}.resume` to appear
/// there, and switches the failpoint off, which lets the worker go on. The settings name the directory with
/// `fail_point_signals`, relative to the worker's own.
void signalPauses(const std::string & directory)
{
#if defined(OS_LINUX)
    for (const auto & fail_point : FailPointInjection::getFailPoints())
    {
        const bool pauses = fail_point.type == FailPointInjection::FailPointType::PauseableOnce
            || fail_point.type == FailPointInjection::FailPointType::Pauseable;
        if (!pauses || !fail_point.enabled)
            continue;

        ThreadFromGlobalPool([directory, name = fail_point.name]
        {
            FailPointInjection::waitForPause(name);

            /// The watch starts before the worker says it paused, so the answer cannot slip in between.
            const int watch = inotify_init1(IN_CLOEXEC);
            if (watch < 0 || inotify_add_watch(watch, directory.c_str(), IN_CREATE | IN_MOVED_TO) < 0)
                throw ErrnoException(ErrorCodes::CANNOT_OPEN_FILE, "Cannot watch `{}` for the resume of failpoint `{}`", directory, name);
            const int paused = ::open((fs::path(directory) / (name + ".paused")).c_str(), O_CREAT | O_WRONLY | O_CLOEXEC, 0644);
            if (paused < 0)
                throw ErrnoException(ErrorCodes::CANNOT_OPEN_FILE, "Cannot create the pause file of failpoint `{}` in `{}`", name, directory);
            ::close(paused);

            const fs::path resume = fs::path(directory) / (name + ".resume");
            char events[4096];
            while (!fs::exists(resume))
                if (::read(watch, events, sizeof(events)) < 0 && errno != EINTR)
                    throw ErrnoException(ErrorCodes::CANNOT_OPEN_FILE, "Cannot read the watch of `{}`", directory);
            ::close(watch);

            FailPointInjection::disableFailPoint(name);
        }).detach();
    }
#else
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "`fail_point_signals` needs inotify, and `{}` names a directory on another system", directory);
#endif
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

    /// What every tenant's tables read and no tenant owns is built here, so that a waiting worker holds it before
    /// its request arrives and a forked child inherits it: the lookup tables of the server's time zone and of UTC,
    /// which take more than a millisecond each to fill, and the tables of types, codecs and `MergeTree` settings.
    /// A table that names another time zone fills that zone's table in the operation that loads it.
    UNUSED(DateLUT::serverTimezoneInstance());
    UNUSED(DateLUT::instance("UTC"));
    UNUSED(DataTypeFactory::instance());
    UNUSED(CompressionCodecFactory::instance());
    UNUSED(MergeTreeSettings{});
}

Engine::~Engine()
{
    /// A task cancels the jobs it still holds, which need the context.
    tenant_tables.clear();
    if (global_context)
    {
        global_context->shutdown();
        global_context.reset();
    }
    shared_context.reset();
}

void Engine::attachTenant(const libch::v1::Tenant & tenant, const std::string & settings_xml, Needs needs)
{
    if (tenant_attached)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "A worker serves one tenant");

    /// The context reads its configuration from here on, so the configuration comes first.
    Poco::AutoPtr<Poco::Util::XMLConfiguration> config = makeConfiguration(tenant, settings_xml, needs);
    global_context->setConfig(config);
    global_context->setApplicationType(Context::ApplicationType::LOCAL);

    /// The operation's memory cap, as a server's: an allocation that would pass it throws, so an operation over
    /// its cap fails before its commit.
    if (const UInt64 memory_limit = config->getUInt64("max_server_memory_usage", 0))
    {
        total_memory_tracker.setDescription("(total)");
        total_memory_tracker.setHardLimit(static_cast<Int64>(memory_limit));
    }

    /// As a server does: the failpoints the configuration names are on from the start. Tests use them to hold a
    /// worker at a point until they kill it or let it go, and to switch a protection off.
    Poco::Util::AbstractConfiguration::Keys fail_points;
    config->keys("fail_points_active", fail_points);
    for (const auto & fail_point : fail_points)
        if (ConfigHelper::getBool(*config, "fail_points_active." + fail_point))
            FailPointInjection::enableFailPoint(fail_point);

    global_context->setPath(fs::path(path) / "");
    /// The directory a test watches for a held worker, under the worker's own, where a sandboxed worker may write.
    if (const std::string signals = config->getString("fail_point_signals", ""); !signals.empty())
    {
        const auto directory = fs::path(path) / signals;
        fs::create_directories(directory);
        signalPauses(directory);
    }
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

    /// The databases live on the tenant's disk. Loading them reads every table's definition and makes the jobs
    /// that build the tables, which run for the tables the operation reaches.
    tenant_tables = loadMetadata(global_context, /* default_database_name= */ {}, /* async_load_databases= */ true, /* load_on_demand= */ true);
}

void Engine::loadEveryTable()
{
    waitLoad(TablesLoaderForegroundPoolId, tenant_tables);
}

bool Engine::removeFreedBlobs(size_t max_rounds)
{
    auto disk = global_context->getDisk(TENANT_DISK);
    auto * object_disk = typeid_cast<DiskObjectStorage *>(disk.get());
    if (!object_disk)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The tenant's disk is no object storage disk");
    disk->getMetadataStorage()->shutdown();

    for (size_t round = 0; round < max_rounds; ++round)
    {
        const auto claimed_before = ProfileEvents::global_counters[ProfileEvents::InodeDBFreedClaimed];
        object_disk->waitBlobsCleanup();
        if (ProfileEvents::global_counters[ProfileEvents::InodeDBFreedClaimed] == claimed_before)
            return false;
    }
    return true;
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
