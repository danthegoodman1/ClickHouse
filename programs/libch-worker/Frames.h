#pragma once

#include <IO/BufferWithOwnMemory.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>

#include <optional>
#include <string>
#include <string_view>

namespace DB::LibchWorker
{

/// The frames of `libch.proto`: a 4-byte little-endian length, then that many bytes.

/// The longest frame either side sends.
constexpr size_t MAX_FRAME_SIZE = 64 << 20;

/// Reads one frame, or nothing at the end of the stream.
std::optional<std::string> readFrame(ReadBuffer & in);

void writeFrame(WriteBuffer & out, std::string_view bytes);

/// An insert's batch: the bytes of the data frames that follow a request, up to the frame of length zero.
class ReadBufferFromDataFrames : public BufferWithOwnMemory<ReadBuffer>
{
public:
    explicit ReadBufferFromDataFrames(ReadBuffer & in_);

    /// Whether the frame of length zero has been read.
    bool ended() const { return batch_ended; }

private:
    bool nextImpl() override;

    ReadBuffer & in;
    size_t left_in_frame = 0;
    bool batch_ended = false;
};

}
