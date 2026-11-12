#include "Engine.h"
#include "Frames.h"
#include "Operations.h"
#include "Sandbox.h"

#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <Common/ErrnoException.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/Jemalloc.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadPool.h>
#include <Common/ThreadStatus.h>
#include <Common/logger_useful.h>

#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/Logger.h>
#include <Poco/PatternFormatter.h>

#include <libch.pb.h>

#include "config.h"

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>

#include <filesystem>
#include <iostream>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <poll.h>
#include <signal.h>
#include <sys/ioctl.h>
#include <sys/prctl.h>
#include <sys/ptrace.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

namespace fs = std::filesystem;

/// BigHouse: `libch-worker` serves one operation of the libch crate on one tenant's `inodedb` disk, and exits.
///
/// It starts the engine, reads one request from stdin, attaches the tenant the request names, runs the operation,
/// writes one response to stdout and exits with status zero. `libch.proto` holds the messages. Logs go to stderr.
/// Any other exit status means the worker failed, and whatever it wrote is no answer.
///
/// With `--fork` the worker serves requests one after another, each in a child it forks once the engine has
/// started, as `serveByForking` describes.
///
/// With `--sandbox` the worker first confines itself to its own directory and the ports `--allow-connect` names,
/// as `Sandbox.h` describes. With `--probe-KIND=TARGET` it serves no request: it tries what the probes name and
/// reports what happened to each.

namespace ProfileEvents
{
    extern const Event BlobKillerThreadRemovedBlobs;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int AUTHENTICATION_FAILED;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_FORK;
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
    extern const int CANNOT_WAITPID;
    extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
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

/// Something a worker tries in place of serving a request, to show what its sandbox lets through.
struct Probe
{
    /// `read`, `write`, `execute`, `connect`, `unix`, `signal` or `trace`.
    std::string kind;
    /// A path, an IPv4 address and port, or a process id.
    std::string target;
};

/// What a forking worker writes on stdout after each child: this number, which no frame's length can be, and
/// then the child's wait status.
constexpr UInt32 FORK_TRAILER = 0xFFFFFFFF;
/// What it writes on stderr after each child: the record separator, which ends that child's log.
constexpr char FORK_LOG_END = '\x1e';

/// How many rounds of blob removal a housekeeping runs at most: a round claims up to a thousand freed files, so
/// this is the files of some thousands of removed parts.
constexpr size_t BLOB_REMOVAL_ROUNDS = 100;

struct Options
{
    /// A Poco log level, as `warning` or `trace`.
    std::string log_level = "warning";
    /// The local directory for the engine's temporary files. Empty makes the worker create one.
    std::string path;
    SandboxOptions sandbox;
    std::vector<Probe> probes;
    /// Whether the worker serves its requests one after another, each in a child it forks.
    bool forking = false;
};

Options parseOptions(int argc, char ** argv)
{
    Options options;
    for (int i = 1; i < argc; ++i)
    {
        const std::string_view argument = argv[i];
        constexpr std::string_view log_level = "--log-level=";
        constexpr std::string_view path = "--path=";
        constexpr std::string_view allow_connect = "--allow-connect=";
        constexpr std::string_view allow_read = "--allow-read=";
        constexpr std::string_view probe = "--probe-";
        if (argument.starts_with(log_level))
            options.log_level = argument.substr(log_level.size());
        else if (argument.starts_with(path))
            options.path = argument.substr(path.size());
        else if (argument == "--sandbox")
            options.sandbox.enabled = true;
        else if (argument == "--fork")
            options.forking = true;
        else if (argument.starts_with(allow_connect))
        {
            std::vector<std::string> ports;
            const std::string list{argument.substr(allow_connect.size())};
            boost::split(ports, list, boost::is_any_of(","), boost::token_compress_on);
            for (const auto & port : ports)
                if (!port.empty())
                    options.sandbox.connect_ports.push_back(parse<UInt16>(port));
        }
        else if (argument.starts_with(allow_read))
            options.sandbox.read_paths.emplace_back(argument.substr(allow_read.size()));
        else if (argument.starts_with(probe) && argument.contains('='))
        {
            const size_t equals = argument.find('=');
            options.probes.push_back({std::string{argument.substr(probe.size(), equals - probe.size())}, std::string{argument.substr(equals + 1)}});
        }
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Unknown argument `{}`: libch-worker takes --log-level=LEVEL, --path=DIRECTORY, --fork, --sandbox, --allow-connect=PORT[,PORT], "
                "--allow-read=PATH and --probe-KIND=TARGET", argument);
    }
    if (!options.sandbox.enabled && (!options.sandbox.connect_ports.empty() || !options.sandbox.read_paths.empty()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "--allow-connect and --allow-read say what a sandbox lets through, and --sandbox is absent");
    return options;
}

/// Tries each probe and writes one line for it to stdout: the probe, then `ok` or the number and name of the error
/// the attempt met. An operator runs it to see what the sandbox holds on a machine, and the escape suite checks it.
int probe(const std::vector<Probe> & probes)
{
    for (const auto & [kind, target] : probes)
    {
        int error = 0;
        if (kind == "read" || kind == "write")
        {
            const int fd = ::open(target.c_str(), kind == "read" ? O_RDONLY | O_CLOEXEC : O_WRONLY | O_CREAT | O_CLOEXEC, 0600);
            if (fd < 0)
                error = errno;
            else
                ::close(fd);
        }
        else if (kind == "execute")
        {
            const pid_t child = ::fork();
            if (child < 0)
            {
                /// A sandbox that starts no process: the program would replace this one, so a sandbox that let
                /// it run would leave the rest of the probes unanswered.
                ::execl(target.c_str(), target.c_str(), nullptr);
                error = errno;
            }
            else if (child == 0)
            {
                ::execl(target.c_str(), target.c_str(), nullptr);
                _exit(errno);
            }
            else
            {
                int status = 0;
                ::waitpid(child, &status, 0);
                error = WIFEXITED(status) ? WEXITSTATUS(status) : EINTR;
            }
        }
        else if (kind == "connect")
        {
            const size_t colon = target.rfind(':');
            sockaddr_in address{};
            address.sin_family = AF_INET;
            if (colon == std::string::npos || ::inet_pton(AF_INET, target.substr(0, colon).c_str(), &address.sin_addr) != 1)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "--probe-connect takes an IPv4 address and a port, as 127.0.0.1:9000");
            address.sin_port = htons(parse<UInt16>(target.substr(colon + 1)));
            const int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
            if (fd < 0 || ::connect(fd, reinterpret_cast<const sockaddr *>(&address), sizeof(address)) != 0)
                error = errno;
            if (fd >= 0)
                ::close(fd);
        }
        else if (kind == "unix")
        {
            const int fd = ::socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
            if (fd < 0)
                error = errno;
            else
                ::close(fd);
        }
        else if (kind == "signal")
        {
            if (::kill(parse<pid_t>(target), 0) != 0)
                error = errno;
        }
        else if (kind == "trace")
        {
            if (::ptrace(PTRACE_SEIZE, parse<pid_t>(target), nullptr, nullptr) != 0)
                error = errno;
        }
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown probe `{}`: a probe is read, write, execute, connect, unix, signal or trace", kind);

        std::cout << kind << ' ' << target << ": " << (error == 0 ? std::string{"ok"} : fmt::format("{} {}", error, std::generic_category().message(error))) << '\n';
    }
    std::cout.flush();
    return 0;
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
    if (code == ErrorCodes::AUTHENTICATION_FAILED)
        return libch::v1::FENCED;
    /// An exclusive writer took the disk during the operation, or owned it when the worker came to join.
    if (code == ErrorCodes::TABLE_IS_READ_ONLY && (message.contains("lost ownership") || message.contains("belongs to an exclusive writer")))
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
            .shared_owner = request.operation_case() == libch::v1::Request::kShare,
        });

    switch (request.operation_case())
    {
        case libch::v1::Request::kDescribe:
            engine.loadEveryTable();
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
        case libch::v1::Request::kShare:
            /// Attaching the tenant replaced an exclusive owner or joined a shared one.
            response.mutable_shared()->set_owner_version(engine.ownerVersion());
            response.set_status(libch::v1::OK);
            break;
        case libch::v1::Request::OPERATION_NOT_SET:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The request names no operation");
    }
    return response;
}

/// Reads one request from stdin, runs its operation and writes its response to stdout. Returns false when the
/// stream ended without a request, as it does when a pool lets a waiting worker go.
bool serveOne(Engine & engine, LoggerPtr log)
{
    ReadBufferFromFileDescriptor in(STDIN_FILENO);
    auto frame = readFrame(in);
    if (!frame)
    {
        LOG_DEBUG(log, "No request arrived");
        return false;
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

    /// Housekeeping ends by removing the blobs of what the tenant's disk freed, in this operation and before it.
    if (response.has_housekept())
    {
        try
        {
            const bool more = engine.removeFreedBlobs(BLOB_REMOVAL_ROUNDS);
            response.mutable_housekept()->set_blobs_remain(more);
        }
        catch (...)
        {
            /// The parts are removed whatever becomes of their blobs, which the next housekeeping finds freed.
            tryLogCurrentException(log, "Removing freed blobs");
            response.mutable_housekept()->set_blobs_remain(true);
        }
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

    if (response.has_housekept())
        response.mutable_housekept()->set_blobs_removed(ProfileEvents::global_counters[ProfileEvents::BlobKillerThreadRemovedBlobs]);

    *response.mutable_usage() = usage();

    FailPointInjection::pauseFailPoint(FailPoints::libch_pause_before_response);

    WriteBufferFromFileDescriptor out(STDOUT_FILENO);
    writeFrame(out, response.SerializeAsString());
    out.finalize();
    return true;
}

/// Writes all of `bytes` to a descriptor.
void writeAll(int fd, const char * bytes, size_t size)
{
    while (size > 0)
    {
        const ssize_t written = ::write(fd, bytes, size);
        if (written < 0 && errno == EINTR)
            continue;
        if (written < 0)
            throw ErrnoException(ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR, "Cannot write to descriptor {}", fd);
        bytes += written;
        size -= static_cast<size_t>(written);
    }
}

/// Serves requests one after another, each in a child of this process, which has started the engine and attached
/// no tenant. A fork gives the child the engine as it stands, so an operation pays for a fork where a worker of
/// its own pays for the engine's start.
///
/// The parent waits until a request's first bytes are on stdin and leaves them there. The child reads the request
/// and serves it as a worker of its own does, in a directory of its own under `path`, and exits. The parent then
/// writes two things for the caller, who cannot wait for the child itself: on stdout, after whatever the child
/// wrote, `FORK_TRAILER` and the child's wait status, each four bytes; and on stderr one `FORK_LOG_END` byte,
/// which ends the child's log. The parent leaves when stdin ends.
///
/// The parent holds one thread at every fork, so the child starts from a process whose every lock is free. The
/// child serves on a thread it starts, whose random numbers and thread-local state are its own: the thread that
/// forked carries its parent's, and every child of one parent would otherwise draw the same request ids.
///
/// The children of one parent read their requests from one pipe, so a child must leave nothing behind that
/// could read the next one. A sandboxed child starts no process, and a deployment that serves tenants who do not
/// trust each other from one forking worker runs it with `--sandbox`.
[[noreturn]] void serveByForking(Engine & engine, const std::string & path, const SandboxOptions & sandbox, LoggerPtr log)
{
    const pid_t parent = getpid();
    for (UInt64 number = 0;; ++number)
    {
        pollfd waiting{.fd = STDIN_FILENO, .events = POLLIN, .revents = 0};
        while (::poll(&waiting, 1, -1) < 0)
            if (errno != EINTR)
                throw ErrnoException(ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR, "Cannot wait for a request");
        int available = 0;
        if (::ioctl(STDIN_FILENO, FIONREAD, &available) != 0 || available == 0)
            _exit(0);

        if (const size_t threads = threadsOfThisProcess(); threads != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A worker forks from its only thread, and it has {}", threads);

        const std::string directory = fmt::format("{}/{}", path, number);
        fs::create_directories(directory);
        const pid_t child = ::fork();
        if (child < 0)
            throw ErrnoException(ErrorCodes::CANNOT_FORK, "Cannot fork a child for a request");
        if (child == 0)
        {
            /// The caller ends an operation by killing the parent, and the child goes with it.
            if (::prctl(PR_SET_PDEATHSIG, SIGKILL) != 0 || getppid() != parent)
                _exit(1);
            try
            {
                /// The child enters the sandbox, where the parent has none: the sandbox names the process's own
                /// entries in `/proc` and its own directory, and gives each child a domain of its own, out of
                /// reach of a sibling that serves another tenant. The child is one thread here.
                if (sandbox.enabled)
                    enterSandbox(sandbox, directory);
                engine.setPath(directory);
                ThreadFromGlobalPool serving([&]
                {
                    try
                    {
                        serveOne(engine, log);
                        _exit(0);
                    }
                    catch (...)
                    {
                        std::cerr << getCurrentExceptionMessage(/* with_stacktrace= */ true) << '\n';
                        _exit(1);
                    }
                });
                serving.join();
            }
            catch (...)
            {
                std::cerr << getCurrentExceptionMessage(/* with_stacktrace= */ true) << '\n';
            }
            _exit(1);
        }

        int status = 0;
        while (::waitpid(child, &status, 0) < 0)
            if (errno != EINTR)
                throw ErrnoException(ErrorCodes::CANNOT_WAITPID, "Cannot wait for the child that serves a request");
        std::error_code ignored;
        fs::remove_all(directory, ignored);

        writeAll(STDERR_FILENO, &FORK_LOG_END, 1);
        char trailer[8];
        memcpy(trailer, &FORK_TRAILER, 4);
        const Int32 reported = status;
        memcpy(trailer + 4, &reported, 4);
        writeAll(STDOUT_FILENO, trailer, sizeof(trailer));
    }
}

int run(int argc, char ** argv)
{
    const Options options = parseOptions(argc, argv);

    /// The worker's own directory: the caller's, which the caller removes, or one the worker makes for itself.
    const bool made_path = options.path.empty();
    const std::string path = made_path ? (fs::temp_directory_path() / fmt::format("libch-worker-{}", getpid())).string() : options.path;
    fs::create_directories(path);

    /// The sandbox comes before anything else runs: it binds the threads the engine starts because the thread
    /// that starts them is bound first.
#if USE_JEMALLOC
    /// The allocator started its background threads before `main`. A sandbox binds the thread that enters it and
    /// what that thread starts, and a fork carries one thread over, so they stop here. A worker that serves in
    /// this process starts them again, inside its sandbox when it has one.
    if (options.sandbox.enabled || options.forking)
        Jemalloc::setBackgroundThreads(false);
#endif
    /// A forking worker's children enter the sandbox, each for itself. The parent runs no tenant's SQL and reads no
    /// request, and it says here, before any request, when the kernel cannot hold the sandbox.
    if (options.sandbox.enabled && options.forking && landlockVersion() < 6)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The sandbox needs Landlock ABI 6 (Linux 6.12)");
    if (options.sandbox.enabled && !options.forking)
        enterSandbox(options.sandbox, path);
#if USE_JEMALLOC
    if (options.sandbox.enabled && !options.forking)
        Jemalloc::setBackgroundThreads(true);
#endif
    if (!options.probes.empty())
        return probe(options.probes);

    logToStderr(options.log_level);
    auto log = getLogger("LibchWorker");

    /// Everything before the request depends on no tenant, so a worker started ahead of demand waits here ready.
    Engine engine(path);

    if (options.forking)
        serveByForking(engine, path, options.sandbox, log);

    const bool served = serveOne(engine, log);
    if (made_path)
    {
        std::error_code ignored;
        fs::remove_all(path, ignored);
    }
    if (!served)
        return 0;

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
