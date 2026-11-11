#pragma once

#include <base/types.h>

#include <string>
#include <vector>

namespace DB::LibchWorker
{

/// What a sandboxed worker may reach.
struct SandboxOptions
{
    bool enabled = false;
    /// The TCP ports the worker may connect to: its InodeDB store's and its S3 endpoint's.
    std::vector<UInt16> connect_ports;
    /// Files and directories the worker may read beside the few every worker reads.
    std::vector<std::string> read_paths;
};

/// Confines this process, and every thread and process it starts from here on, to one tenant's work:
///
/// - It reads the files a worker needs (its own `/proc` entries, the resolver's and the TLS library's
///   configuration, time zones) and `options.read_paths`, writes under `directory` alone, and executes nothing.
/// - It connects by TCP to `options.connect_ports` alone, listens on none, and opens sockets of the IPv4, IPv6
///   and netlink families alone.
/// - It signals and traces no process outside itself, and reaches no abstract Unix socket another process made.
/// - It makes none of the system calls that reach around these rules: mounts, namespaces, BPF, `io_uring`,
///   `ptrace` and its relatives, kernel modules and keyrings.
/// - It starts threads and no process, so nothing of it outlives it.
///
/// Landlock holds the first three and a seccomp filter the rest. Both bind the calling thread and what it
/// starts, so the call comes from the main thread while it is the process's only one. Each process that enters
/// has a domain of its own, so two sandboxed workers are out of each other's reach, as are two children of one
/// forking worker. A kernel without Landlock
/// ABI 6 (Linux 6.12) fails the call: a worker asked for a sandbox runs in one or not at all.
void enterSandbox(const SandboxOptions & options, const std::string & directory);

/// The Landlock ABI version of the running kernel, or zero when it has none.
int landlockVersion();

/// How many threads this process has.
size_t threadsOfThisProcess();

}
