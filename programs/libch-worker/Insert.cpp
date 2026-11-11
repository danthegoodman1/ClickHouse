#include "Frames.h"
#include "Operations.h"

#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <IO/ConcatReadBuffer.h>
#include <IO/NullWriteBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/IAST.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageMergeTree.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/QueryScope.h>
#include <Common/quoteString.h>

#include <optional>
#include <unordered_set>
#include <utility>
#include <vector>

namespace ProfileEvents
{
    extern const Event DuplicatedInsertedBlocks;
    extern const Event InodeDBCommitRetries;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int UNKNOWN_FORMAT;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsUInt64 non_replicated_deduplication_window;
}

namespace LibchWorker
{

namespace
{

/// What an insert reaches: the table it writes, the materialized views that read from it, and through them every
/// table the views write, each once.
struct Reach
{
    std::vector<StoragePtr> tables;
    std::vector<StoragePtr> views;
};

Reach reachOf(const StorageID & table_id, ContextPtr context)
{
    const auto & catalog = DatabaseCatalog::instance();
    Reach reach;
    std::unordered_set<UUID> seen;
    std::vector<StorageID> pending{table_id};
    while (!pending.empty())
    {
        auto id = pending.back();
        pending.pop_back();
        auto table = catalog.getTable(id, context);
        const auto resolved = table->getStorageID();
        if (!seen.insert(resolved.uuid).second)
            continue;
        reach.tables.push_back(table);
        for (const auto & view_id : catalog.getDependentViews(resolved))
        {
            auto view = catalog.tryGetTable(view_id, context);
            if (const auto * materialized = view ? typeid_cast<const StorageMaterializedView *>(view.get()) : nullptr)
            {
                reach.views.push_back(view);
                pending.push_back(materialized->getTargetTableId());
            }
        }
    }
    return reach;
}

void collectTableIdentifiers(const ASTPtr & ast, std::vector<StorageID> & identifiers)
{
    if (!ast)
        return;
    if (const auto * identifier = ast->as<ASTTableIdentifier>())
        identifiers.push_back(identifier->getTableId());
    for (const auto & child : ast->children)
        collectTableIdentifiers(child, identifiers);
}

/// A view is fed the rows of the insert in place of its source table. Every other table its query names, and its
/// source named a second time, it reads from the disk. Returns the first such table of the first such view.
std::optional<std::pair<StorageID, StorageID>> viewThatReadsStoredRows(const std::vector<StoragePtr> & views, ContextPtr context)
{
    const auto & catalog = DatabaseCatalog::instance();
    for (const auto & view : views)
    {
        const auto view_id = view->getStorageID();
        const auto metadata = view->getInMemoryMetadataPtr(context, /* bypass_metadata_cache= */ false);
        const auto & select = metadata->getSelectQuery();
        std::vector<StorageID> identifiers;
        collectTableIdentifiers(select.inner_query, identifiers);

        bool fed = false;
        for (auto & identifier : identifiers)
        {
            if (identifier.database_name.empty())
                identifier.database_name = view_id.database_name;
            /// A name that is no table of the tenant is one the query defines itself.
            auto table = catalog.tryGetTable(identifier, context);
            if (!table)
                continue;
            const auto read_id = table->getStorageID();
            if (!fed && read_id.uuid == catalog.getTable(select.select_table_id, context)->getStorageID().uuid)
            {
                fed = true;
                continue;
            }
            return std::pair(view_id, read_id);
        }
    }
    return std::nullopt;
}

/// The first table function a query names, if it names one.
std::optional<String> tableFunctionIn(const ASTPtr & ast)
{
    if (!ast)
        return std::nullopt;
    if (const auto * expression = ast->as<ASTTableExpression>(); expression && expression->table_function)
    {
        const auto * function = expression->table_function->as<ASTFunction>();
        return function ? function->name : String("(unnamed)");
    }
    for (const auto & child : ast->children)
        if (auto found = tableFunctionIn(child))
            return found;
    return std::nullopt;
}

/// A worker registers no table function, so a view that names one fails. It fails as the view runs, after the
/// insert's own part is committed, so the insert is refused before. Returns the first such view and function.
std::optional<std::pair<StorageID, String>> viewThatNamesATableFunction(const std::vector<StoragePtr> & views, ContextPtr context)
{
    for (const auto & view : views)
    {
        const auto metadata = view->getInMemoryMetadataPtr(context, /* bypass_metadata_cache= */ false);
        if (auto function = tableFunctionIn(metadata->getSelectQuery().inner_query))
            return std::pair(view->getStorageID(), *function);
    }
    return std::nullopt;
}

/// The first table among those an insert writes that keeps no deduplication tokens, if any does.
StoragePtr tableWithoutTokens(const std::vector<StoragePtr> & tables)
{
    for (const auto & table : tables)
        if (const auto * merge_tree = typeid_cast<const StorageMergeTree *>(table.get()))
            if ((*merge_tree->getSettings())[MergeTreeSetting::non_replicated_deduplication_window] == 0)
                return table;
    return nullptr;
}

}

void insert(const Engine & engine, const libch::v1::Insert & request, ReadBuffer & in, libch::v1::Response & response)
{
    const auto & name = request.table();
    if (name.database().empty() || name.table().empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "An insert names its table and the table's database");
    if (request.token().empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "An insert names its batch with a token");
    if (!FormatFactory::instance().isInputFormat(request.format()))
        throw Exception(ErrorCodes::UNKNOWN_FORMAT, "`{}` is no input format", request.format());

    auto context = engine.makeQueryContext();
    const StorageID table_id(name.database(), name.table());

    const auto reach = reachOf(table_id, context);

    /// A table in operation mode knows none of its parts, so a view cannot read one.
    if (auto reading = viewThatReadsStoredRows(reach.views, context))
    {
        response.set_status(libch::v1::REFUSED);
        response.set_message(fmt::format(
            "The view {} reads {} from the disk, besides the rows it is fed, and a worker loads no table's parts",
            reading->first.getNameForLogs(), reading->second.getNameForLogs()));
        return;
    }

    if (auto naming = viewThatNamesATableFunction(reach.views, context))
    {
        response.set_status(libch::v1::REFUSED);
        response.set_message(fmt::format(
            "The view {} names the table function `{}`, and a worker provides no table function", naming->first.getNameForLogs(), naming->second));
        return;
    }

    /// A retried batch lands once only where every table it reaches keeps tokens.
    if (!request.at_least_once())
    {
        if (auto table = tableWithoutTokens(reach.tables))
        {
            response.set_status(libch::v1::REFUSED);
            response.set_message(fmt::format(
                "{} keeps no deduplication tokens (`non_replicated_deduplication_window` = 0), so a second insert of the batch would write it again",
                table->getStorageID().getNameForLogs()));
            return;
        }
    }

    context->setSetting("insert_deduplication_token", request.token());
    /// An operation commits before it answers.
    context->setSetting("async_insert", false);
    /// A batch a caller sends to one worker is small, and one thread writes it for the least CPU. A profile in the
    /// request's settings overrides either.
    for (const auto * threads : {"max_threads", "max_insert_threads"})
        if (!context->getSettingsRef().isChanged(threads))
            context->setSetting(threads, UInt64(1));

    /// The query's text and the batch reach the engine as one stream, as an INSERT over HTTP does.
    const std::string query = fmt::format("INSERT INTO {}.{} FORMAT {}\n", backQuote(name.database()), backQuote(name.table()), request.format());
    ReadBufferFromString query_text(query);
    ReadBufferFromDataFrames batch(in);
    ConcatReadBuffer stream(query_text, batch);
    NullWriteBuffer no_output;
    {
        auto scope = QueryScope::create(context);
        executeQuery(stream, no_output, context, {});
    }
    if (!batch.eof() || !batch.ended())
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "The insert ended before it read the whole batch");

    auto & inserted = *response.mutable_inserted();
    for (const auto & commit : StorageMergeTree::takeOperationCommits())
    {
        auto & part = *inserted.add_parts();
        part.mutable_table()->set_database(commit.table.database_name);
        part.mutable_table()->set_table(commit.table.table_name);
        part.set_part(commit.part_name);
        part.set_rows(commit.rows);
        part.set_bytes(commit.bytes);
    }
    inserted.set_counter_conflicts(static_cast<UInt32>(ProfileEvents::global_counters[ProfileEvents::InodeDBCommitRetries]));

    /// A batch whose every block was committed before writes nothing, and finds its tokens taken.
    const bool found_duplicates = ProfileEvents::global_counters[ProfileEvents::DuplicatedInsertedBlocks] > 0;
    response.set_status(inserted.parts().empty() && found_duplicates ? libch::v1::DUPLICATE : libch::v1::OK);
}

}

}
