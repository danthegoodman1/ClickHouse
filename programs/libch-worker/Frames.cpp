#include "Frames.h"

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int TOO_LARGE_STRING_SIZE;
}

namespace LibchWorker
{

namespace
{

/// The length of the next frame, or nothing at the end of the stream.
std::optional<size_t> readFrameLength(ReadBuffer & in)
{
    if (in.eof())
        return std::nullopt;
    UInt32 length = 0;
    readBinaryLittleEndian(length, in);
    if (length > MAX_FRAME_SIZE)
        throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "A frame of {} bytes exceeds the limit of {}", length, MAX_FRAME_SIZE);
    return length;
}

}

std::optional<std::string> readFrame(ReadBuffer & in)
{
    auto length = readFrameLength(in);
    if (!length)
        return std::nullopt;
    std::string bytes(*length, '\0');
    in.readStrict(bytes.data(), bytes.size());
    return bytes;
}

void writeFrame(WriteBuffer & out, std::string_view bytes)
{
    if (bytes.size() > MAX_FRAME_SIZE)
        throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "A frame of {} bytes exceeds the limit of {}", bytes.size(), MAX_FRAME_SIZE);
    writeBinaryLittleEndian(static_cast<UInt32>(bytes.size()), out);
    out.write(bytes.data(), bytes.size());
}

ReadBufferFromDataFrames::ReadBufferFromDataFrames(ReadBuffer & in_)
    : in(in_)
{
}

bool ReadBufferFromDataFrames::nextImpl()
{
    if (batch_ended)
        return false;

    if (left_in_frame == 0)
    {
        auto length = readFrameLength(in);
        if (!length)
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "The batch ended without its frame of length zero");
        if (*length == 0)
        {
            batch_ended = true;
            return false;
        }
        left_in_frame = *length;
    }

    const size_t size = std::min(left_in_frame, internal_buffer.size());
    in.readStrict(internal_buffer.begin(), size);
    left_in_frame -= size;
    working_buffer = Buffer(internal_buffer.begin(), internal_buffer.begin() + size);
    return true;
}

}

}
