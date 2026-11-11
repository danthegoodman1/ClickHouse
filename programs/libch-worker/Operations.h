#pragma once

#include "Engine.h"

#include <IO/ReadBuffer.h>

#include <libch.pb.h>

namespace DB::LibchWorker
{

/// The operations of `libch.proto`. Each runs against the tenant the engine has attached.

/// The tenant's tables and views as the engine attached them, with each table's parts when the request asks.
libch::v1::Described describe(const Engine & engine, const libch::v1::Describe & request);

/// Inserts the batch that follows the request on `in` into a table and, through its materialized views, into their
/// targets. Sets the response's status and, for an insert that ran, the parts it committed.
void insert(const Engine & engine, const libch::v1::Insert & request, ReadBuffer & in, libch::v1::Response & response);

/// Proposes merges for a table as a shared writer's selector would choose them, and runs none.
void plan(const Engine & engine, const libch::v1::Plan & request, libch::v1::Response & response);

/// Merges the named source parts of one partition into one part, after checking them against the table's directory.
/// Sets the response's status and, for a merge that committed, the merged part's name.
void merge(const Engine & engine, const libch::v1::Merge & request, libch::v1::Response & response);

/// Removes what operations left behind in a table: covered parts past the table's grace period with their claims,
/// tokens past their age, claims of writers that are gone, and temporary directories past theirs. Reports the parts
/// it removed and the covered parts that stay.
void housekeep(const Engine & engine, const libch::v1::Housekeep & request, libch::v1::Response & response);

/// What this process has spent so far: its calls to InodeDB and S3, its CPU time, its memory and its threads.
libch::v1::Usage usage();

}
