/// BigHouse: the InodeDB client's redirect and retry, against an InodeDB server behind a proxy that plays a
/// standby or loses a commit's answer. Needs `INODEDB_TEST_ENDPOINT` and an admin key in `INODEDB_TEST_KEY`.

#include "config.h"

#if USE_GRPC

#include <Disks/DiskObjectStorage/MetadataStorages/InodeDB/InodeDBClient.h>
#include <Common/getRandomASCIIString.h>
#include <Common/randomSeed.h>

#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace idb = ::inodedb::v1;

namespace
{

/// Listens on a free port of 127.0.0.1 and answers `Commit` in one of two ways. Every other call fails.
class Proxy final : public idb::InodeDB::Service
{
public:
    enum class Mode
    {
        /// Answers as a standby does: `UNAVAILABLE`, with `inodedb-error: not-writer` and the writer's address.
        Standby,
        /// Forwards each commit to the writer and answers the first with `UNAVAILABLE`, as if its answer was lost.
        LoseFirstAnswer,
    };

    Proxy(std::string writer_, Mode mode_)
        : writer(std::move(writer_))
        , mode(mode_)
        , stub(idb::InodeDB::NewStub(grpc::CreateChannel(writer, grpc::InsecureChannelCredentials())))
    {
        grpc::ServerBuilder builder;
        builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(), &port);
        builder.RegisterService(this);
        server = builder.BuildAndStart();
    }

    ~Proxy() override { server->Shutdown(); }

    std::string address() const { return "127.0.0.1:" + std::to_string(port); }

    /// The request id of every commit the proxy received, in order.
    std::vector<std::string> requestIds() const
    {
        std::lock_guard lock(mutex);
        return request_ids;
    }

    grpc::Status Commit(grpc::ServerContext * context, const idb::CommitRequest * request, idb::CommitResponse * response) override
    {
        size_t received;
        {
            std::lock_guard lock(mutex);
            request_ids.push_back(request->request_id());
            received = request_ids.size();
        }

        if (mode == Mode::Standby)
        {
            context->AddTrailingMetadata("inodedb-error", "not-writer");
            context->AddTrailingMetadata("inodedb-writer", "http://" + writer);
            return grpc::Status(grpc::StatusCode::UNAVAILABLE, "this server is a standby; ask the writer");
        }

        grpc::ClientContext forward;
        for (const auto & [name, value] : context->client_metadata())
            if (name == "authorization")
                forward.AddMetadata("authorization", std::string(value.data(), value.size()));
        grpc::Status status = stub->Commit(&forward, *request, response);
        if (status.ok() && received == 1)
            return grpc::Status(grpc::StatusCode::UNAVAILABLE, "the answer was lost");
        return status;
    }

private:
    const std::string writer;
    const Mode mode;
    std::unique_ptr<idb::InodeDB::Stub> stub;
    int port = 0;
    std::unique_ptr<grpc::Server> server;
    mutable std::mutex mutex;
    std::vector<std::string> request_ids;
};

class InodeDBClientTest : public ::testing::Test
{
public:
    void SetUp() override
    {
        if (!getenv("INODEDB_TEST_ENDPOINT") || !getenv("INODEDB_TEST_KEY"))
            GTEST_SKIP() << "Set INODEDB_TEST_ENDPOINT and INODEDB_TEST_KEY to run the InodeDB client cases";
        endpoint = getenv("INODEDB_TEST_ENDPOINT");
        key = getenv("INODEDB_TEST_KEY");
        /// A generator of its own: `DiskObjectStorageTest` seeds `thread_local_rng`, and InodeDB keeps the roots of
        /// earlier runs.
        static pcg64 rng(randomSeed());
        root = "gtest-client/" + DB::getRandomASCIIString(16, rng);
    }

    DB::InodeDB::Client client(const std::string & address) const
    {
        return DB::InodeDB::Client({.endpoint = address, .key = key, .retry_timeout = std::chrono::milliseconds(10'000)});
    }

    /// A batch that makes one directory and fails with `EXISTS` once it exists.
    static idb::CommitRequest mkdir(const std::string & path)
    {
        idb::CommitRequest request;
        auto * op = request.add_ops()->mutable_mkdir();
        op->set_path(path);
        op->set_parents(true);
        return request;
    }

    bool exists(const std::string & path) const
    {
        idb::StatRequest request;
        request.add_targets()->set_path(path);
        auto response = client(endpoint).stat(request);
        return response.results_size() == 1 && response.results(0).has_inode();
    }

    std::string endpoint;
    std::string key;
    std::string root;
};

}

TEST_F(InodeDBClientTest, StandbyRedirectsToTheWriter)
{
    Proxy standby(endpoint, Proxy::Mode::Standby);
    auto through_standby = client(standby.address());

    auto first = through_standby.commit(mkdir(root + "/first"));
    ASSERT_EQ(first.outcomes_size(), 1);
    EXPECT_TRUE(first.outcomes(0).has_applied());
    EXPECT_TRUE(exists(root + "/first"));

    /// Later calls go straight to the writer.
    auto second = through_standby.commit(mkdir(root + "/second"));
    ASSERT_EQ(second.outcomes_size(), 1);
    EXPECT_TRUE(second.outcomes(0).has_applied());
    EXPECT_TRUE(exists(root + "/second"));
    EXPECT_EQ(standby.requestIds().size(), 1);
}

TEST_F(InodeDBClientTest, RetriedCommitAppliesOnce)
{
    Proxy lossy(endpoint, Proxy::Mode::LoseFirstAnswer);
    auto through_lossy = client(lossy.address());

    /// The writer applies the first attempt, whose answer the proxy drops. The retry carries the same request id,
    /// so the writer answers with the first attempt's outcome. Applied a second time, the mkdir would fail with `EXISTS`.
    idb::CommitRequest once = mkdir(root + "/once");
    once.mutable_ops(0)->mutable_mkdir()->set_parents(false);
    client(endpoint).commit(mkdir(root));
    auto response = through_lossy.commit(once);
    ASSERT_EQ(response.outcomes_size(), 1);
    EXPECT_TRUE(response.outcomes(0).has_applied()) << response.outcomes(0).DebugString();
    EXPECT_TRUE(exists(root + "/once"));

    auto ids = lossy.requestIds();
    ASSERT_EQ(ids.size(), 2);
    EXPECT_EQ(ids[0].size(), 16);
    EXPECT_EQ(ids[0], ids[1]);

    /// A new commit gets a new request id, so the same mkdir now fails.
    auto again = through_lossy.commit(once);
    ASSERT_EQ(again.outcomes_size(), 1);
    ASSERT_TRUE(again.outcomes(0).has_skipped());
    EXPECT_TRUE(again.outcomes(0).skipped().has_error());
}

#endif
