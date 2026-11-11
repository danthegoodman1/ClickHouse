#pragma once

#include <Interpreters/Context.h>

#include <libch.pb.h>

#include <memory>
#include <optional>
#include <string>

namespace DB::LibchWorker
{

/// The name the worker gives the tenant's disk and its storage policy, as every BigHouse server does.
constexpr const char * TENANT_DISK = "bighouse";

/// The ClickHouse engine of a worker process, and the one tenant it serves.
///
/// A worker starts the engine before it reads its request, so a worker that waits for a request has done
/// everything that depends on no tenant. The request then names the tenant, and the worker attaches that tenant's
/// catalog from its `inodedb` disk as a shared writer, with every `MergeTree` table in operation mode: definitions
/// alone, no parts and no background work.
class Engine
{
public:
    /// Registers what a tenant's tables and views use and creates the global context. `path` is a local directory
    /// for the engine's temporary files; an empty one makes the engine create its own and remove it when it stops.
    explicit Engine(std::string path);
    ~Engine();

    /// What an operation needs of the engine beyond the tenant's tables.
    struct Needs
    {
        /// The merge pool at a server's size, which the merge selector reads to bound the merges it proposes.
        bool merge_selection = false;
        /// The disk's removal of freed blobs from S3. An operation without it starts and stops without waiting
        /// for a round of removals, and what it frees waits for one that has it.
        bool blob_removal = false;
    };

    /// Configures the engine with the tenant's disk and the request's settings, joins the disk's shared writers
    /// and attaches the tenant's databases.
    void attachTenant(const libch::v1::Tenant & tenant, const std::string & settings_xml, Needs needs);

    /// Releases what the worker holds in the tenant's InodeDB store, so nothing waits for it to expire.
    void detachTenant();

    ContextMutablePtr globalContext() const { return global_context; }

    /// A context for one query of the operation.
    ContextMutablePtr makeQueryContext() const;

private:
    std::string path;
    bool owns_path = false;
    bool tenant_attached = false;

    std::optional<SharedContextHolder> shared_context;
    ContextMutablePtr global_context;
};

}
