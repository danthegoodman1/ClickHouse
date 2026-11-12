#pragma once

#include <Interpreters/Context.h>
#include <Common/AsyncLoader.h>

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
    /// Registers what a tenant's tables and views use and creates the global context. `path` is the worker's own
    /// directory, where the engine keeps its temporary files.
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
        /// The disk for shared writers whoever owns it now: the worker replaces an exclusive writer's `owner`,
        /// which fences that writer. An operation without it leaves an exclusive owner in place and fails.
        bool shared_owner = false;
    };

    /// Configures the engine with the tenant's disk and the request's settings, joins the disk's shared writers
    /// and attaches the tenant's databases. Throws on a disk an exclusive writer owns, unless `needs` says to
    /// replace its owner.
    void attachTenant(const libch::v1::Tenant & tenant, const std::string & settings_xml, Needs needs);

    /// The version of the disk's `owner` the worker joined or wrote.
    UInt64 ownerVersion() const;

    /// Loads every table of the tenant. Without it a table is loaded when the operation first reaches it, with
    /// the tables it depends on and the materialized views that read from it.
    void loadEveryTable();

    /// Removes from S3 the blobs of the files the tenant's disk has freed. The disk claims the tenant's freed files
    /// from InodeDB a batch at a time, deletes their blobs and acknowledges them, and this runs such rounds until
    /// one finds nothing to claim or `max_rounds` have run. Returns whether it stopped with rounds still finding
    /// files. The reader lease goes first: while it lasts, InodeDB holds back what the operation itself freed.
    bool removeFreedBlobs(size_t max_rounds);

    /// Releases what the worker holds in the tenant's InodeDB store, so nothing waits for it to expire.
    void detachTenant();

    /// Moves the worker's own directory, before a tenant is attached: a forked child takes one of its own.
    void setPath(std::string path_) { path = std::move(path_); }

    ContextMutablePtr globalContext() const { return global_context; }

    /// A context for one query of the operation.
    ContextMutablePtr makeQueryContext() const;

private:
    std::string path;
    bool tenant_attached = false;

    std::optional<SharedContextHolder> shared_context;
    ContextMutablePtr global_context;
    /// The jobs that load and start the tenant's tables, each run when something waits for its table.
    LoadTaskPtrs tenant_tables;
};

}
