#include <Disks/DiskObjectStorage/MetadataStorages/InodeDB/InodeDBClient.h>

#if USE_GRPC

#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/thread_local_rng.h>
#include <base/scope_guard.h>
#include <base/sleep.h>

#include <cstring>

#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/support/channel_arguments.h>

namespace ProfileEvents
{
    extern const Event InodeDBCommit;
    extern const Event InodeDBStat;
    extern const Event InodeDBRead;
    extern const Event InodeDBList;
    extern const Event InodeDBClaimFreed;
    extern const Event InodeDBAckFreed;
    extern const Event InodeDBMintKey;
    extern const Event InodeDBRetries;
    extern const Event InodeDBMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
    extern const int AUTHENTICATION_FAILED;
    extern const int LOGICAL_ERROR;
    extern const int NETWORK_ERROR;
}

namespace InodeDB
{

namespace
{

std::string stripScheme(std::string address)
{
    for (std::string_view scheme : {"http://", "https://"})
        if (address.starts_with(scheme))
            return address.substr(scheme.size());
    return address;
}

std::string trailer(const grpc::ClientContext & context, std::string_view key)
{
    const auto & metadata = context.GetServerTrailingMetadata();
    auto it = metadata.find(grpc::string_ref(key.data(), key.size()));
    if (it == metadata.end())
        return {};
    return std::string(it->second.data(), it->second.size());
}

bool isRetryable(const grpc::Status & status)
{
    return status.error_code() == grpc::StatusCode::UNAVAILABLE || status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED;
}

[[noreturn]] void throwStatus(std::string_view method, const std::string & target, const grpc::Status & status, const std::string & inodedb_error)
{
    int code = ErrorCodes::NETWORK_ERROR;
    switch (status.error_code())
    {
        case grpc::StatusCode::UNAUTHENTICATED:
            code = ErrorCodes::AUTHENTICATION_FAILED;
            break;
        case grpc::StatusCode::PERMISSION_DENIED:
            code = ErrorCodes::ACCESS_DENIED;
            break;
        case grpc::StatusCode::INVALID_ARGUMENT:
            /// The disk built a request the server can't read.
            code = ErrorCodes::LOGICAL_ERROR;
            break;
        default:
            break;
    }
    throw Exception(code, "InodeDB `{}` at {} failed with gRPC status {}: {}{}",
        method, target, static_cast<int>(status.error_code()), status.error_message(),
        inodedb_error.empty() ? "" : fmt::format(" (inodedb-error: {})", inodedb_error));
}

std::string newRequestId()
{
    std::string id(16, '\0');
    for (size_t i = 0; i < id.size(); i += sizeof(UInt64))
    {
        UInt64 random = thread_local_rng();
        memcpy(id.data() + i, &random, sizeof(random));
    }
    return id;
}

}

Client::Client(ClientSettings settings_)
    : settings(std::move(settings_))
    , authorization("Bearer " + settings.key)
{
    settings.endpoint = stripScheme(settings.endpoint);
    channel = connect(settings.endpoint);
}

Client::ChannelPtr Client::connect(const std::string & target)
{
    grpc::ChannelArguments args;
    /// A listing page or a claim can hold many values.
    args.SetMaxReceiveMessageSize(256 << 20);
    args.SetMaxSendMessageSize(256 << 20);
    auto grpc_channel = grpc::CreateCustomChannel(target, grpc::InsecureChannelCredentials(), args);

    auto result = std::make_shared<Channel>();
    result->target = target;
    result->stub = pb::InodeDB::NewStub(grpc_channel);
    return result;
}

Client::ChannelPtr Client::currentChannel() const
{
    std::lock_guard lock(channel_mutex);
    return channel;
}

template <typename Attempt>
void Client::invoke(std::string_view method, ProfileEvents::Event event, Attempt && attempt) const
{
    ProfileEvents::increment(event);
    Stopwatch watch;
    SCOPE_EXIT({ ProfileEvents::increment(ProfileEvents::InodeDBMicroseconds, watch.elapsedMicroseconds()); });

    const auto give_up_at = std::chrono::steady_clock::now() + settings.retry_timeout;
    UInt64 backoff_ms = 5;

    while (true)
    {
        auto current = currentChannel();

        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + settings.attempt_timeout);
        context.AddMetadata("authorization", authorization);

        grpc::Status status = attempt(*current->stub, context);
        if (status.ok())
            return;

        const std::string inodedb_error = trailer(context, "inodedb-error");
        if (inodedb_error == "not-writer")
        {
            /// A standby names the writer. Without a name, retry here until a writer appears.
            if (std::string writer = stripScheme(trailer(context, "inodedb-writer")); !writer.empty() && writer != current->target)
            {
                std::lock_guard lock(channel_mutex);
                if (channel == current)
                    channel = connect(writer);
                continue;
            }
        }

        if (!isRetryable(status) || std::chrono::steady_clock::now() + std::chrono::milliseconds(backoff_ms) >= give_up_at)
        {
            /// After a failure, go back to the configured address, which finds the writer again after a takeover.
            {
                std::lock_guard lock(channel_mutex);
                if (channel == current && current->target != settings.endpoint)
                    channel = connect(settings.endpoint);
            }
            throwStatus(method, current->target, status, inodedb_error);
        }

        ProfileEvents::increment(ProfileEvents::InodeDBRetries);
        sleepForMilliseconds(backoff_ms);
        backoff_ms = std::min<UInt64>(backoff_ms * 2, 1000);
    }
}

pb::CommitResponse Client::commit(pb::CommitRequest request) const
{
    request.set_request_id(newRequestId());
    pb::CommitResponse response;
    invoke("Commit", ProfileEvents::InodeDBCommit, [&](pb::InodeDB::Stub & stub, grpc::ClientContext & context)
    {
        response.Clear();
        return stub.Commit(&context, request, &response);
    });
    return response;
}

pb::StatResponse Client::stat(const pb::StatRequest & request) const
{
    pb::StatResponse response;
    invoke("Stat", ProfileEvents::InodeDBStat, [&](pb::InodeDB::Stub & stub, grpc::ClientContext & context)
    {
        response.Clear();
        return stub.Stat(&context, request, &response);
    });
    return response;
}

pb::ReadResponse Client::read(const pb::ReadRequest & request) const
{
    pb::ReadResponse response;
    invoke("Read", ProfileEvents::InodeDBRead, [&](pb::InodeDB::Stub & stub, grpc::ClientContext & context)
    {
        response.Clear();
        return stub.Read(&context, request, &response);
    });
    return response;
}

Listing Client::list(pb::ListRequest request) const
{
    Listing listing;
    invoke("List", ProfileEvents::InodeDBList, [&](pb::InodeDB::Stub & stub, grpc::ClientContext & context)
    {
        /// A retry resumes after the last entry, in the directory the first page named.
        if (!listing.entries.empty())
            request.set_after(listing.entries.back().name());

        auto reader = stub.List(&context, request);
        pb::ListPage page;
        while (reader->Read(&page))
        {
            if (page.has_error())
                listing.error = page.error();
            if (listing.version == 0 || page.version() < listing.version)
                listing.version = page.version();
            if (page.has_dir() && !request.has_dir())
                *request.mutable_dir() = page.dir();
            for (auto & entry : *page.mutable_entries())
                listing.entries.push_back(std::move(entry));
        }
        return reader->Finish();
    });
    return listing;
}

pb::ClaimResponse Client::claimFreed(uint32_t limit) const
{
    pb::ClaimRequest request;
    request.set_limit(limit);
    pb::ClaimResponse response;
    invoke("ClaimFreed", ProfileEvents::InodeDBClaimFreed, [&](pb::InodeDB::Stub & stub, grpc::ClientContext & context)
    {
        response.Clear();
        return stub.ClaimFreed(&context, request, &response);
    });
    return response;
}

void Client::ackFreed(const pb::AckRequest & request) const
{
    pb::AckResponse response;
    invoke("AckFreed", ProfileEvents::InodeDBAckFreed, [&](pb::InodeDB::Stub & stub, grpc::ClientContext & context)
    {
        return stub.AckFreed(&context, request, &response);
    });
}

std::unique_ptr<LeaseStream> Client::openLease() const
{
    auto lease = std::make_unique<LeaseStream>();
    lease->context.AddMetadata("authorization", authorization);
    auto current = currentChannel();
    lease->stream = current->stub->Lease(&lease->context);
    lease->channel = current;
    return lease;
}

pb::LeaseState LeaseStream::beat(const pb::LeaseBeat & beat)
{
    pb::LeaseState state;
    if (!stream->Write(beat) || !stream->Read(&state))
    {
        grpc::Status status = stream->Finish();
        throw Exception(ErrorCodes::NETWORK_ERROR, "InodeDB lease stream broke: gRPC status {}: {}",
            static_cast<int>(status.error_code()), status.error_message());
    }
    return state;
}

void LeaseStream::release(UInt64 lease) noexcept
{
    pb::LeaseBeat beat;
    beat.set_lease(lease);
    beat.set_release(true);
    pb::LeaseState state;
    if (stream->Write(beat))
        stream->Read(&state);
    stream->WritesDone();
    while (stream->Read(&state))
    {
    }
    stream->Finish();
}

void LeaseStream::cancel()
{
    context.TryCancel();
}

pb::MintKeyResponse Client::mintKey(const pb::MintKeyRequest & request) const
{
    pb::MintKeyResponse response;
    invoke("MintKey", ProfileEvents::InodeDBMintKey, [&](pb::InodeDB::Stub & stub, grpc::ClientContext & context)
    {
        response.Clear();
        return stub.MintKey(&context, request, &response);
    });
    return response;
}

}

}

#endif
