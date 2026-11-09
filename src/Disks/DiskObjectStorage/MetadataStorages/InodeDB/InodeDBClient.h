#pragma once

#include "config.h"

#if USE_GRPC

#include <base/types.h>
#include <Common/ProfileEvents.h>

#include <inodedb.grpc.pb.h>

#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace DB::InodeDB
{

namespace pb = ::inodedb::v1;

struct ClientSettings
{
    /// `host:port`, with or without an `http://` prefix.
    std::string endpoint;
    /// `<id>.<secret>`. The key's root is the root of every path the client sends.
    std::string key;
    /// The deadline of one attempt.
    std::chrono::milliseconds attempt_timeout{10'000};
    /// How long transport failures are retried before a call gives up.
    std::chrono::milliseconds retry_timeout{60'000};
};

/// Every entry of one directory.
struct Listing
{
    std::vector<pb::Listed> entries;
    /// Why the directory could not be listed: `NOT_FOUND`, `NOT_DIR`.
    std::optional<pb::Error> error;
    /// The oldest version a page reflects.
    UInt64 version = 0;
};

/// A reader's lease. While it lasts, InodeDB keeps every file and value freed at or after the lease's oldest
/// version, so blobs the reader may still read stay in object storage.
class LeaseStream
{
public:
    /// Sends a beat and waits for the state that answers it. Throws when the stream breaks.
    pb::LeaseState beat(const pb::LeaseBeat & beat);
    /// Ends the lease, so InodeDB stops holding what it froze.
    void release(UInt64 lease) noexcept;
    /// Unblocks a `beat` waiting on another thread.
    void cancel();

private:
    friend class Client;
    /// Keeps the stub the stream runs on alive across a redirect to another server.
    std::shared_ptr<const void> channel;
    grpc::ClientContext context;
    std::unique_ptr<grpc::ClientReaderWriter<pb::LeaseBeat, pb::LeaseState>> stream;
};

/// A blocking client for one InodeDB server.
///
/// Transport failures retry until `retry_timeout`. A commit keeps its request id across retries, so the
/// server applies it at most once. A standby's `NotWriter` answer redirects every later call to the writer.
class Client
{
public:
    explicit Client(ClientSettings settings_);

    const std::string & endpoint() const { return settings.endpoint; }
    std::chrono::milliseconds attemptTimeout() const { return settings.attempt_timeout; }

    /// Fills in the request id.
    pb::CommitResponse commit(pb::CommitRequest request) const;
    pb::StatResponse stat(const pb::StatRequest & request) const;
    pb::ReadResponse read(const pb::ReadRequest & request) const;
    /// Pages through the directory, resuming after the last entry when a stream breaks.
    Listing list(pb::ListRequest request) const;
    pb::ClaimResponse claimFreed(uint32_t limit) const;
    void ackFreed(const pb::AckRequest & request) const;
    pb::MintKeyResponse mintKey(const pb::MintKeyRequest & request) const;
    /// Opens a stream for one lease. It has no deadline: it lasts until released or cancelled.
    std::unique_ptr<LeaseStream> openLease() const;

private:
    struct Channel
    {
        std::string target;
        std::unique_ptr<pb::InodeDB::Stub> stub;
    };
    using ChannelPtr = std::shared_ptr<const Channel>;

    static ChannelPtr connect(const std::string & target);
    ChannelPtr currentChannel() const;

    /// Runs `attempt` until it succeeds, fails for good, or `retry_timeout` passes, and counts the call in `event`.
    /// `attempt` takes the stub and a prepared context and returns the call's status.
    template <typename Attempt>
    void invoke(std::string_view method, ProfileEvents::Event event, Attempt && attempt) const;

    ClientSettings settings;
    std::string authorization;

    mutable std::mutex channel_mutex;
    mutable ChannelPtr channel;
};

}

#endif
