#include "Operations.h"

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <Common/ProfileEvents.h>

#include <string>
#include <string_view>

#include <sys/resource.h>

namespace ProfileEvents
{
    extern const Event InodeDBCommit;
    extern const Event InodeDBStat;
    extern const Event InodeDBRead;
    extern const Event InodeDBList;
    extern const Event InodeDBMicroseconds;
    extern const Event InodeDBRetries;
    extern const Event S3ReadRequestsCount;
    extern const Event S3WriteRequestsCount;
    extern const Event S3ReadMicroseconds;
    extern const Event S3WriteMicroseconds;
    extern const Event S3ReadRequestsThrottling;
    extern const Event S3WriteRequestsThrottling;
    extern const Event ReadBufferFromS3Bytes;
    extern const Event WriteBufferFromS3Bytes;
}

namespace DB::LibchWorker
{

namespace
{

UInt32 count(ProfileEvents::Event event)
{
    return static_cast<UInt32>(ProfileEvents::global_counters[event]);
}

/// An event's total, for events that count time or bytes.
UInt64 total(ProfileEvents::Event event)
{
    return ProfileEvents::global_counters[event];
}

UInt64 microseconds(const timeval & time)
{
    return static_cast<UInt64>(time.tv_sec) * 1'000'000 + static_cast<UInt64>(time.tv_usec);
}

/// The number on a line of `/proc/self/status`, as `RssAnon:     1234 kB` gives 1234.
UInt64 statusField(std::string_view status, std::string_view name)
{
    const size_t line = status.find(name);
    if (line == std::string_view::npos)
        return 0;
    size_t at = line + name.size();
    while (at < status.size() && (status[at] == ' ' || status[at] == '\t'))
        ++at;
    UInt64 value = 0;
    while (at < status.size() && status[at] >= '0' && status[at] <= '9')
        value = value * 10 + static_cast<UInt64>(status[at++] - '0');
    return value;
}

}

libch::v1::Usage usage()
{
    libch::v1::Usage usage;
    usage.set_inodedb_commits(count(ProfileEvents::InodeDBCommit));
    usage.set_inodedb_stats(count(ProfileEvents::InodeDBStat));
    usage.set_inodedb_reads(count(ProfileEvents::InodeDBRead));
    usage.set_inodedb_lists(count(ProfileEvents::InodeDBList));
    usage.set_s3_reads(count(ProfileEvents::S3ReadRequestsCount));
    usage.set_s3_writes(count(ProfileEvents::S3WriteRequestsCount));
    usage.set_inodedb_microseconds(total(ProfileEvents::InodeDBMicroseconds));
    usage.set_inodedb_retries(count(ProfileEvents::InodeDBRetries));
    usage.set_s3_read_microseconds(total(ProfileEvents::S3ReadMicroseconds));
    usage.set_s3_write_microseconds(total(ProfileEvents::S3WriteMicroseconds));
    usage.set_s3_throttled(count(ProfileEvents::S3ReadRequestsThrottling) + count(ProfileEvents::S3WriteRequestsThrottling));
    usage.set_s3_bytes_read(total(ProfileEvents::ReadBufferFromS3Bytes));
    usage.set_s3_bytes_written(total(ProfileEvents::WriteBufferFromS3Bytes));

    rusage spent{};
    if (getrusage(RUSAGE_SELF, &spent) == 0)
        usage.set_cpu_microseconds(microseconds(spent.ru_utime) + microseconds(spent.ru_stime));

    std::string status;
    ReadBufferFromFile file("/proc/self/status");
    readStringUntilEOF(status, file);
    usage.set_anonymous_memory_bytes(statusField(status, "RssAnon:") * 1024);
    usage.set_threads(static_cast<UInt32>(statusField(status, "Threads:")));
    return usage;
}

}
