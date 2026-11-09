#pragma once

#include <base/types.h>

#include <string>
#include <string_view>

namespace DB::InodeDB
{

/// What an InodeDB file's value holds.
enum class ValueKind : char
{
    /// A `DiskObjectStorageMetadata` record in its text form: blobs, size, read-only flag, inline data.
    Record = 'R',
    /// The bytes of a file written with `writeStringToFile`.
    String = 'S',
    /// The full object key of a blob that holds a record too large for an InodeDB value.
    Spilled = 'P',
    /// The record text of blobs that a transaction dropped from a live file. A tombstone is created and
    /// unlinked in the same batch, so InodeDB frees it at once and its blobs go once no reader can reach them.
    Tombstone = 'T',
};

/// An InodeDB value: a fixed header, then the body.
///
///   byte 0      format version, `FORMAT_VERSION`
///   byte 1      kind
///   byte 2      flags: bit 0 is `retained`
///   bytes 3-10  change time, seconds since the epoch, little endian
///   bytes 11-   body
struct Value
{
    static constexpr UInt8 FORMAT_VERSION = 1;
    static constexpr size_t HEADER_SIZE = 11;

    ValueKind kind = ValueKind::Record;
    /// The blobs belong to another owner, so freeing this value deletes none of them.
    bool retained = false;
    UInt64 ctime = 0;
    std::string body;

    std::string encode() const;
    /// Throws `UNKNOWN_FORMAT` naming `path`.
    static Value decode(std::string_view bytes, const std::string & path);
};

}
