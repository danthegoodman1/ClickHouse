#include "Engine.h"
#include "Frames.h"
#include "Operations.h"

#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/ThreadStatus.h>
#include <Common/logger_useful.h>

#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Logger.h>
#include <Poco/PatternFormatter.h>

#include <libch.pb.h>

#include <iostream>
#include <string>
#include <string_view>

#include <unistd.h>

/// BigHouse: `libch-worker` serves one operation of the libch crate on one tenant's `inodedb` disk, and exits.
///
/// It starts the engine, reads one request from stdin, attaches the tenant the request names, runs the operation,
/// writes one response to stdout and exits with status zero. `libch.proto` holds the messages. Logs go to stderr.
/// Any other exit status means the worker failed, and whatever it wrote is no answer.

namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int AUTHENTICATION_FAILED;
    extern const int BAD_ARGUMENTS;
    extern const int SUPPORT_IS_DISABLED;
    extern const int TABLE_IS_READ_ONLY;
}

namespace FailPoints
{
    extern const char libch_pause_before_response[];
}

namespace LibchWorker
{

namespace
{

struct Options
{
    /// A Poco log level, as `warning` or `trace`.
    std::string log_level = "warning";
    /// The local directory for the engine's temporary files. Empty makes the engine create one.
    std::string path;
};

Options parseOptions(int argc, char ** argv)
{
    Options options;
    for (int i = 1; i < argc; ++i)
    {
        const std::string_view argument = argv[i];
        constexpr std::string_view log_level = "--log-level=";
        constexpr std::string_view path = "--path=";
        if (argument.starts_with(log_level))
            options.log_level = argument.substr(log_level.size());
        else if (argument.starts_with(path))
            options.path = argument.substr(path.size());
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown argument `{}`: libch-worker takes --log-level=LEVEL and --path=DIRECTORY", argument);
    }
    return options;
}

void logToStderr(const std::string & level)
{
    Poco::AutoPtr<Poco::ConsoleChannel> channel(new Poco::ConsoleChannel(std::cerr));
    Poco::AutoPtr<Poco::PatternFormatter> formatter(new Poco::PatternFormatter("%H:%M:%S.%i <%p> %s: %t"));
    Poco::AutoPtr<Poco::FormattingChannel> formatting(new Poco::FormattingChannel(formatter, channel));
    Poco::Logger::root().setChannel(formatting);
    Poco::Logger::root().setLevel(level);
}

/// The status the current exception stands for. An operation that committed nothing for a reason the caller can
/// act on has a status of its own; everything else is a failure.
libch::v1::Status statusOfCurrentException(int code, const std::string & message)
{
    if (code == ErrorCodes::AUTHENTICATION_FAILED || (code == ErrorCodes::TABLE_IS_READ_ONLY && message.contains("lost ownership")))
        return libch::v1::FENCED;
    if (code == ErrorCodes::ABORTED && message.contains("definition of"))
        return libch::v1::SCHEMA_CHANGED;
    if (code == ErrorCodes::ABORTED && message.contains("committed nothing"))
        return libch::v1::CLAIM_LOST;
    if (code == ErrorCodes::SUPPORT_IS_DISABLED && message.contains("needs an exclusive writer"))
        return libch::v1::REFUSED;
    return libch::v1::FAILED;
}

libch::v1::Response responseOfCurrentException()
{
    libch::v1::Response response;
    const int code = getCurrentExceptionCode();
    const std::string message = getCurrentExceptionMessage(/* with_stacktrace= */ false);
    response.set_status(statusOfCurrentException(code, message));
    response.set_message(message);
    if (response.status() == libch::v1::FAILED)
        response.set_exception_code(code);
    return response;
}

libch::v1::Response serve(Engine & engine, const libch::v1::Request & request, ReadBuffer & in)
{
    libch::v1::Response response;
    engine.attachTenant(
        request.tenant(),
        request.settings_xml(),
        Engine::Needs{
            .merge_selection = request.operation_case() == libch::v1::Request::kPlan,
            .blob_removal = request.operation_case() == libch::v1::Request::kHousekeep,
        });

    switch (request.operation_case())
    {
        case libch::v1::Request::kDescribe:
            *response.mutable_described() = describe(engine, request.describe());
            response.set_status(libch::v1::OK);
            break;
        case libch::v1::Request::kInsert:
            insert(engine, request.insert(), in, response);
            break;
        case libch::v1::Request::kMerge:
            merge(engine, request.merge(), response);
            break;
        case libch::v1::Request::kHousekeep:
            housekeep(engine, request.housekeep(), response);
            break;
        case libch::v1::Request::kPlan:
            plan(engine, request.plan(), response);
            break;
        case libch::v1::Request::OPERATION_NOT_SET:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The request names no operation");
    }
    return response;
}

int run(int argc, char ** argv)
{
    const Options options = parseOptions(argc, argv);
    logToStderr(options.log_level);
    auto log = getLogger("LibchWorker");

    /// Everything before the request depends on no tenant, so a worker started ahead of demand waits here ready.
    Engine engine(options.path);

    ReadBufferFromFileDescriptor in(STDIN_FILENO);
    auto frame = readFrame(in);
    if (!frame)
    {
        /// The caller closed the stream without a request, as a pool does when it lets a waiting worker go.
        LOG_DEBUG(log, "No request arrived");
        return 0;
    }
    libch::v1::Request request;
    if (!request.ParseFromString(*frame))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The request of {} bytes is no `libch.v1.Request`", frame->size());

    libch::v1::Response response;
    try
    {
        response = serve(engine, request, in);
    }
    catch (...)
    {
        tryLogCurrentException(log, "The operation failed");
        response = responseOfCurrentException();
    }

    try
    {
        engine.detachTenant();
    }
    catch (...)
    {
        /// What the worker held expires by itself.
        tryLogCurrentException(log, "Releasing the tenant's store");
    }

    *response.mutable_usage() = usage();

    FailPointInjection::pauseFailPoint(FailPoints::libch_pause_before_response);

    WriteBufferFromFileDescriptor out(STDOUT_FILENO);
    writeFrame(out, response.SerializeAsString());
    out.finalize();

    /// The response is the worker's last word, and its state lives in InodeDB and S3. Leaving without the engine's
    /// shutdown keeps the exit as short as the operation's own work.
    _exit(0);
}

}

}

}

int mainEntryClickHouseLibchWorker(int argc, char ** argv);
int mainEntryClickHouseLibchWorker(int argc, char ** argv)
{
    DB::MainThreadStatus::getInstance();
    try
    {
        return DB::LibchWorker::run(argc, argv);
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(/* with_stacktrace= */ true) << '\n';
        /// Without a response the caller takes the worker as failed, whatever the status.
        _exit(1);
    }
}
