#include <Disks/DiskObjectStorage/MetadataStorages/InodeDB/InodeDBValue.h>

#include <Common/Exception.h>

#include <cstring>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_FORMAT;
}

namespace InodeDB
{

std::string Value::encode() const
{
    std::string bytes(HEADER_SIZE, '\0');
    bytes[0] = static_cast<char>(FORMAT_VERSION);
    bytes[1] = static_cast<char>(kind);
    bytes[2] = retained ? 1 : 0;
    for (size_t i = 0; i < sizeof(ctime); ++i)
        bytes[3 + i] = static_cast<char>((ctime >> (8 * i)) & 0xFF);
    bytes += body;
    return bytes;
}

Value Value::decode(std::string_view bytes, const std::string & path)
{
    if (bytes.size() < HEADER_SIZE || static_cast<UInt8>(bytes[0]) != FORMAT_VERSION)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT, "File `{}` holds no InodeDB metadata value of format {}", path, static_cast<unsigned>(FORMAT_VERSION));

    Value value;
    switch (bytes[1])
    {
        case static_cast<char>(ValueKind::Record):
        case static_cast<char>(ValueKind::String):
        case static_cast<char>(ValueKind::Spilled):
        case static_cast<char>(ValueKind::Tombstone):
            value.kind = static_cast<ValueKind>(bytes[1]);
            break;
        default:
            throw Exception(ErrorCodes::UNKNOWN_FORMAT, "File `{}` holds an InodeDB value of unknown kind {}", path, static_cast<int>(bytes[1]));
    }
    value.retained = (bytes[2] & 1) != 0;
    for (size_t i = 0; i < sizeof(value.ctime); ++i)
        value.ctime |= static_cast<UInt64>(static_cast<UInt8>(bytes[3 + i])) << (8 * i);
    value.body.assign(bytes.substr(HEADER_SIZE));
    return value;
}

}

}
