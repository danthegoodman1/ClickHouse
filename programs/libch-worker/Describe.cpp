#include "Operations.h"

#include <Databases/IDatabase.h>
#include <Disks/IDisk.h>
#include <Disks/IStoragePolicy.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/MergeTree/ActiveDataPartSet.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/StorageMaterializedView.h>

#include <algorithm>
#include <unordered_set>
#include <vector>

namespace DB::LibchWorker
{

namespace
{

void setName(libch::v1::TableName & name, const StorageID & id)
{
    name.set_database(id.database_name);
    name.set_table(id.table_name);
}

libch::v1::Table::Kind kindOf(const IStorage & storage)
{
    if (storage.isDictionary())
        return libch::v1::Table::DICTIONARY;
    if (!storage.isView())
        return libch::v1::Table::TABLE;
    return typeid_cast<const StorageMaterializedView *>(&storage) ? libch::v1::Table::MATERIALIZED_VIEW : libch::v1::Table::VIEW;
}

/// Every part directory of a table, with its name as the engine parses it, and whether another part contains it.
void describeParts(const MergeTreeData & data, libch::v1::Table & table)
{
    std::vector<std::pair<String, MergeTreePartInfo>> parts;
    for (const auto & disk : data.getStoragePolicy()->getDisks())
        for (auto it = disk->iterateDirectory(data.getRelativeDataPath()); it->isValid(); it->next())
            if (auto info = MergeTreePartInfo::tryParsePartName(it->name(), data.format_version))
                parts.emplace_back(it->name(), *info);
    std::ranges::sort(parts, {}, [](const auto & part) { return part.second; });

    /// A part no other part contains is active. One that only intersects another stays active beside it, as both
    /// would be on a writer's disk.
    ActiveDataPartSet set(data.format_version);
    std::unordered_set<String> intersecting;
    for (const auto & [name, info] : parts)
        if (set.tryAddPart(info) == ActiveDataPartSet::AddPartOutcome::HasIntersectingPart)
            intersecting.insert(name);
    const auto remaining = set.getParts();
    const std::unordered_set<String> uncovered(remaining.begin(), remaining.end());

    for (const auto & [name, info] : parts)
    {
        auto & part = *table.add_parts();
        part.set_name(name);
        part.set_partition_id(info.getPartitionId());
        part.set_min_block(info.min_block);
        part.set_max_block(info.max_block);
        part.set_level(info.level);
        part.set_mutation(info.mutation);
        part.set_active(uncovered.contains(name) || intersecting.contains(name));
    }
}

}

libch::v1::Described describe(const Engine & engine, const libch::v1::Describe & request)
{
    auto context = engine.globalContext();
    const auto & catalog = DatabaseCatalog::instance();

    std::vector<StoragePtr> tables;
    for (const auto & [database_name, database] : catalog.getDatabases(GetDatabasesOptions{}))
    {
        if (database_name == DatabaseCatalog::TEMPORARY_DATABASE)
            continue;
        for (auto it = database->getTablesIterator(context); it->isValid(); it->next())
            tables.push_back(it->table());
    }
    std::ranges::sort(tables, {}, [](const StoragePtr & table)
    {
        auto id = table->getStorageID();
        return std::pair(id.database_name, id.table_name);
    });

    libch::v1::Described described;
    for (const auto & storage : tables)
    {
        const auto id = storage->getStorageID();
        auto & table = *described.add_tables();
        setName(*table.mutable_name(), id);
        table.set_uuid(toString(id.uuid));
        table.set_kind(kindOf(*storage));
        if (!storage->isView())
            table.set_engine(storage->getName());

        auto views = catalog.getDependentViews(id);
        std::ranges::sort(views, {}, [](const StorageID & view) { return std::pair(view.database_name, view.table_name); });
        for (const auto & view_id : views)
        {
            auto view = catalog.tryGetTable(view_id, context);
            const auto * materialized = view ? typeid_cast<const StorageMaterializedView *>(view.get()) : nullptr;
            if (!materialized)
                continue;
            auto & edge = *table.add_views();
            setName(*edge.mutable_view(), view_id);
            setName(*edge.mutable_target(), materialized->getTargetTableId());
        }

        if (request.parts())
            if (const auto * data = dynamic_cast<const MergeTreeData *>(storage.get()))
                describeParts(*data, table);
    }
    return described;
}

}
