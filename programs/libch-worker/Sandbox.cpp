#include "Sandbox.h"

#include <Common/ErrnoException.h>
#include <Common/Exception.h>
#include <Common/Stopwatch.h>

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <filesystem>

#if defined(OS_LINUX)
#    include <fcntl.h>
#    include <linux/audit.h>
#    include <linux/filter.h>
#    include <linux/sched.h>
#    include <linux/seccomp.h>
#    include <sched.h>
#    include <sys/prctl.h>
#    include <sys/socket.h>
#    include <sys/stat.h>
#    include <sys/syscall.h>
#    include <unistd.h>
#endif

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_OPEN_FILE;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int SYSTEM_ERROR;
}

namespace LibchWorker
{

#if defined(OS_LINUX)

namespace
{

/// Landlock's interface as `linux/landlock.h` gives it. The build's kernel headers predate that file, and the
/// numbers are the kernel's ABI.
namespace Landlock
{

constexpr long CREATE_RULESET = 444;
constexpr long ADD_RULE = 445;
constexpr long RESTRICT_SELF = 446;

constexpr UInt32 CREATE_RULESET_VERSION = 1U << 0;
constexpr int RULE_PATH_BENEATH = 1;
constexpr int RULE_NET_PORT = 2;

constexpr UInt64 FS_EXECUTE = 1ULL << 0;
constexpr UInt64 FS_WRITE_FILE = 1ULL << 1;
constexpr UInt64 FS_READ_FILE = 1ULL << 2;
constexpr UInt64 FS_READ_DIR = 1ULL << 3;
constexpr UInt64 FS_REMOVE_DIR = 1ULL << 4;
constexpr UInt64 FS_REMOVE_FILE = 1ULL << 5;
constexpr UInt64 FS_MAKE_CHAR = 1ULL << 6;
constexpr UInt64 FS_MAKE_DIR = 1ULL << 7;
constexpr UInt64 FS_MAKE_REG = 1ULL << 8;
constexpr UInt64 FS_MAKE_SOCK = 1ULL << 9;
constexpr UInt64 FS_MAKE_FIFO = 1ULL << 10;
constexpr UInt64 FS_MAKE_BLOCK = 1ULL << 11;
constexpr UInt64 FS_MAKE_SYM = 1ULL << 12;
constexpr UInt64 FS_REFER = 1ULL << 13;
constexpr UInt64 FS_TRUNCATE = 1ULL << 14;
constexpr UInt64 FS_IOCTL_DEV = 1ULL << 15;
/// Every right over files that ABI 6 knows. The sandbox handles them all, so each is refused wherever no rule
/// allows it.
constexpr UInt64 FS_EVERYTHING = FS_EXECUTE | FS_WRITE_FILE | FS_READ_FILE | FS_READ_DIR | FS_REMOVE_DIR | FS_REMOVE_FILE | FS_MAKE_CHAR
    | FS_MAKE_DIR | FS_MAKE_REG | FS_MAKE_SOCK | FS_MAKE_FIFO | FS_MAKE_BLOCK | FS_MAKE_SYM | FS_REFER | FS_TRUNCATE | FS_IOCTL_DEV;

constexpr UInt64 NET_BIND_TCP = 1ULL << 0;
constexpr UInt64 NET_CONNECT_TCP = 1ULL << 1;

constexpr UInt64 SCOPE_ABSTRACT_UNIX_SOCKET = 1ULL << 0;
constexpr UInt64 SCOPE_SIGNAL = 1ULL << 1;

/// The first ABI with every right above.
constexpr int REQUIRED_VERSION = 6;

struct RulesetAttr
{
    UInt64 handled_access_fs;
    UInt64 handled_access_net;
    UInt64 scoped;
};

struct __attribute__((packed)) PathBeneathAttr
{
    UInt64 allowed_access;
    Int32 parent_fd;
};

struct NetPortAttr
{
    UInt64 allowed_access;
    UInt64 port;
};

}

/// What a worker may do to a file it may read, to a directory it may read, and under the directory it owns.
constexpr UInt64 READ_A_FILE = Landlock::FS_READ_FILE;
constexpr UInt64 READ_A_DIRECTORY = Landlock::FS_READ_FILE | Landlock::FS_READ_DIR;
constexpr UInt64 OWN_A_DIRECTORY = Landlock::FS_READ_FILE | Landlock::FS_READ_DIR | Landlock::FS_WRITE_FILE | Landlock::FS_TRUNCATE
    | Landlock::FS_MAKE_REG | Landlock::FS_MAKE_DIR | Landlock::FS_REMOVE_FILE | Landlock::FS_REMOVE_DIR | Landlock::FS_REFER;

/// The files a worker's engine reads whatever its tenant: its own entries in `/proc`, what the kernel says of
/// the machine and of the process's limits, the resolver's and the TLS library's configuration, time zones and
/// debug symbols. One that is absent on this machine is left out.
constexpr const char * READ_BY_EVERY_WORKER[] = {
    "/proc/self",
    "/proc/cpuinfo",
    "/proc/meminfo",
    "/proc/stat",
    "/proc/loadavg",
    "/proc/uptime",
    "/proc/sys",
    "/proc/pressure",
    "/sys/fs/cgroup",
    "/sys/devices/system/cpu",
    "/sys/kernel/mm/transparent_hugepage",
    "/etc/resolv.conf",
    "/etc/hosts",
    "/etc/host.conf",
    "/etc/nsswitch.conf",
    "/etc/gai.conf",
    "/etc/localtime",
    "/etc/timezone",
    "/etc/ssl",
    "/etc/pki",
    "/etc/ca-certificates",
    "/usr/share/ca-certificates",
    "/usr/lib/ssl",
    "/usr/share/zoneinfo",
    /// Where the engine looks for symbols when it writes an exception's stack trace. Refused the file, it fails
    /// with that refusal in place of the exception it was writing.
    "/usr/lib/debug",
    "/dev/urandom",
    "/dev/random",
    "/proc/self/exe",
};

/// Allows `access` on a path and, for a directory, on everything beneath it. Returns false for a path that is
/// absent.
bool allowPath(int ruleset, const std::string & path, UInt64 file_access, UInt64 directory_access)
{
    const int fd = ::open(path.c_str(), O_PATH | O_CLOEXEC);
    if (fd < 0)
    {
        if (errno == ENOENT || errno == ENOTDIR)
            return false;
        throw ErrnoException(ErrorCodes::CANNOT_OPEN_FILE, "Cannot open `{}` to allow it in the sandbox", path);
    }
    struct stat status{};
    if (::fstat(fd, &status) != 0)
    {
        ::close(fd);
        throw ErrnoException(ErrorCodes::CANNOT_OPEN_FILE, "Cannot stat `{}` to allow it in the sandbox", path);
    }
    Landlock::PathBeneathAttr rule{.allowed_access = S_ISDIR(status.st_mode) ? directory_access : file_access, .parent_fd = fd};
    const long added = ::syscall(Landlock::ADD_RULE, ruleset, Landlock::RULE_PATH_BENEATH, &rule, 0);
    const int error = errno;
    ::close(fd);
    if (added != 0)
    {
        errno = error;
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot allow `{}` in the sandbox", path);
    }
    return true;
}

/// Landlock: the files, the ports, and the reach of signals and abstract sockets.
void confineWithLandlock(const SandboxOptions & options, const std::string & directory)
{
    const int version = landlockVersion();
    if (version < Landlock::REQUIRED_VERSION)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "The sandbox needs Landlock ABI {} (Linux 6.12), and this kernel has {}",
            Landlock::REQUIRED_VERSION, version == 0 ? "no Landlock" : fmt::format("ABI {}", version));

    Landlock::RulesetAttr attr{
        .handled_access_fs = Landlock::FS_EVERYTHING,
        .handled_access_net = Landlock::NET_BIND_TCP | Landlock::NET_CONNECT_TCP,
        .scoped = Landlock::SCOPE_ABSTRACT_UNIX_SOCKET | Landlock::SCOPE_SIGNAL,
    };
    const int ruleset = static_cast<int>(::syscall(Landlock::CREATE_RULESET, &attr, sizeof(attr), 0));
    if (ruleset < 0)
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot create a Landlock ruleset");

    for (const char * path : READ_BY_EVERY_WORKER)
        allowPath(ruleset, path, READ_A_FILE, READ_A_DIRECTORY);
    for (const auto & path : options.read_paths)
        if (!allowPath(ruleset, path, READ_A_FILE, READ_A_DIRECTORY))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "`--allow-read={}` names a path that does not exist", path);
    if (!allowPath(ruleset, directory, READ_A_FILE, OWN_A_DIRECTORY))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The worker's own directory `{}` is gone", directory);
    /// Programs write to it to discard output.
    allowPath(ruleset, "/dev/null", Landlock::FS_READ_FILE | Landlock::FS_WRITE_FILE, 0);

    for (const UInt16 port : options.connect_ports)
    {
        Landlock::NetPortAttr rule{.allowed_access = Landlock::NET_CONNECT_TCP, .port = port};
        if (::syscall(Landlock::ADD_RULE, ruleset, Landlock::RULE_NET_PORT, &rule, 0) != 0)
            throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot allow connections to port {} in the sandbox", port);
    }

    if (::prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0)
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot give up new privileges");
    if (::syscall(Landlock::RESTRICT_SELF, ruleset, 0) != 0)
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot enter the Landlock ruleset");
    ::close(ruleset);
}

/// A system call's number where the build's headers name it, and the kernel's own number where they predate it.
/// The calls added since Linux 5.1 have one number on every architecture.
#    if defined(SYS_io_uring_setup)
constexpr long NR_IO_URING_SETUP = SYS_io_uring_setup;
#    else
constexpr long NR_IO_URING_SETUP = 425;
#    endif
#    if defined(SYS_pidfd_send_signal)
constexpr long NR_PIDFD_SEND_SIGNAL = SYS_pidfd_send_signal;
#    else
constexpr long NR_PIDFD_SEND_SIGNAL = 424;
#    endif
#    if defined(SYS_pidfd_getfd)
constexpr long NR_PIDFD_GETFD = SYS_pidfd_getfd;
#    else
constexpr long NR_PIDFD_GETFD = 438;
#    endif
#    if defined(SYS_process_madvise)
constexpr long NR_PROCESS_MADVISE = SYS_process_madvise;
#    else
constexpr long NR_PROCESS_MADVISE = 440;
#    endif
#    if defined(SYS_clone3)
constexpr long NR_CLONE3 = SYS_clone3;
#    else
constexpr long NR_CLONE3 = 435;
#    endif
#    if defined(SYS_open_tree)
constexpr long NR_OPEN_TREE = SYS_open_tree;
#    else
constexpr long NR_OPEN_TREE = 428;
#    endif
#    if defined(SYS_move_mount)
constexpr long NR_MOVE_MOUNT = SYS_move_mount;
#    else
constexpr long NR_MOVE_MOUNT = 429;
#    endif
#    if defined(SYS_fsopen)
constexpr long NR_FSOPEN = SYS_fsopen;
#    else
constexpr long NR_FSOPEN = 430;
#    endif

/// The system calls a worker never makes, each a way around the rules Landlock holds or a way to change the
/// kernel under them.
constexpr long REFUSED_CALLS[] = {
    SYS_ptrace, SYS_process_vm_readv, SYS_process_vm_writev, NR_PIDFD_GETFD, NR_PIDFD_SEND_SIGNAL, NR_PROCESS_MADVISE,
    SYS_mount, SYS_umount2, SYS_pivot_root, SYS_chroot, NR_OPEN_TREE, NR_MOVE_MOUNT, NR_FSOPEN,
    SYS_unshare, SYS_setns,
    SYS_bpf, SYS_perf_event_open, SYS_userfaultfd, NR_IO_URING_SETUP,
    SYS_init_module, SYS_finit_module, SYS_delete_module, SYS_kexec_load,
    SYS_add_key, SYS_request_key, SYS_keyctl,
    SYS_swapon, SYS_swapoff, SYS_reboot, SYS_acct, SYS_settimeofday, SYS_clock_settime, SYS_open_by_handle_at,
};

#    if defined(__x86_64__)
constexpr UInt32 THIS_ARCHITECTURE = AUDIT_ARCH_X86_64;
#    elif defined(__aarch64__)
constexpr UInt32 THIS_ARCHITECTURE = AUDIT_ARCH_AARCH64;
#    else
#        error "The sandbox's system call filter knows x86-64 and AArch64"
#    endif

/// A seccomp filter: the refused calls fail with `EPERM`, as does starting a process; a socket of a family other
/// than IPv4, IPv6 or netlink fails with `EAFNOSUPPORT`; and a call made through another architecture's table
/// kills the process.
void confineWithSeccomp()
{
    constexpr UInt32 refuse = SECCOMP_RET_ERRNO | EPERM;
    std::vector<sock_filter> program = {
        BPF_STMT(BPF_LD | BPF_W | BPF_ABS, offsetof(seccomp_data, arch)),
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, THIS_ARCHITECTURE, 1, 0),
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_KILL_PROCESS),
        BPF_STMT(BPF_LD | BPF_W | BPF_ABS, offsetof(seccomp_data, nr)),
#    if defined(__x86_64__)
        /// The x32 table shares the architecture's number and sets this bit in the call's.
        BPF_JUMP(BPF_JMP | BPF_JGE | BPF_K, 0x40000000U, 0, 1),
        BPF_STMT(BPF_RET | BPF_K, refuse),
#    endif
        /// A new thread, and no new process: `clone` with `CLONE_THREAD` alone. `clone3` keeps its flags behind a
        /// pointer the filter cannot follow, so it answers that the call does not exist, and the C library falls
        /// back to `clone`.
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, static_cast<UInt32>(NR_CLONE3), 0, 1),
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ERRNO | ENOSYS),
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, static_cast<UInt32>(SYS_clone), 0, 4),
        BPF_STMT(BPF_LD | BPF_W | BPF_ABS, offsetof(seccomp_data, args[0])),
        BPF_JUMP(BPF_JMP | BPF_JSET | BPF_K, CLONE_THREAD, 1, 0),
        BPF_STMT(BPF_RET | BPF_K, refuse),
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ALLOW),
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, static_cast<UInt32>(SYS_socket), 0, 6),
        BPF_STMT(BPF_LD | BPF_W | BPF_ABS, offsetof(seccomp_data, args[0])),
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, AF_INET, 3, 0),
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, AF_INET6, 2, 0),
        BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, AF_NETLINK, 1, 0),
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ERRNO | EAFNOSUPPORT),
        BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ALLOW),
    };
#    if defined(SYS_fork)
    for (const long call : {static_cast<long>(SYS_fork), static_cast<long>(SYS_vfork)})
    {
        program.push_back(BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, static_cast<UInt32>(call), 0, 1));
        program.push_back(BPF_STMT(BPF_RET | BPF_K, refuse));
    }
#    endif
    for (const long call : REFUSED_CALLS)
    {
        program.push_back(BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, static_cast<UInt32>(call), 0, 1));
        program.push_back(BPF_STMT(BPF_RET | BPF_K, refuse));
    }
    program.push_back(BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ALLOW));

    sock_fprog filter{.len = static_cast<unsigned short>(program.size()), .filter = program.data()};
    if (::syscall(SYS_seccomp, SECCOMP_SET_MODE_FILTER, 0, &filter) != 0)
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot install the sandbox's system call filter");
}

}

int landlockVersion()
{
    const long version = ::syscall(Landlock::CREATE_RULESET, nullptr, 0, Landlock::CREATE_RULESET_VERSION);
    return version < 0 ? 0 : static_cast<int>(version);
}

size_t threadsOfThisProcess()
{
    return static_cast<size_t>(std::distance(fs::directory_iterator("/proc/self/task"), fs::directory_iterator{}));
}

void enterSandbox(const SandboxOptions & options, const std::string & directory)
{
    /// The rules bind this thread and what it starts. A thread that already runs would stay outside them. A thread
    /// joined a moment ago, as the allocator's are, is still on the kernel's list of the process's tasks until
    /// its exit completes, so the count gets a second to reach one.
    size_t threads = threadsOfThisProcess();
    for (Stopwatch watch; threads != 1 && watch.elapsedMilliseconds() < 1000; threads = threadsOfThisProcess())
        sched_yield();
    if (threads != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The sandbox starts on a process's only thread, and this process has {}", threads);

    /// No core file, and no other process of this user reads this one's memory or its files in `/proc`.
    if (::prctl(PR_SET_DUMPABLE, 0, 0, 0, 0) != 0)
        throw ErrnoException(ErrorCodes::SYSTEM_ERROR, "Cannot make the process undumpable");

    confineWithLandlock(options, directory);
    confineWithSeccomp();
}

#else

int landlockVersion()
{
    return 0;
}

size_t threadsOfThisProcess()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Counting a process's threads needs Linux");
}

void enterSandbox(const SandboxOptions &, const std::string &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "The sandbox needs Linux");
}


#endif

}

}
