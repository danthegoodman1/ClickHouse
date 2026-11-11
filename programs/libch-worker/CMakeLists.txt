# BigHouse: `libch-worker` serves one operation of the libch crate on one tenant's `inodedb` disk, and exits.

# The messages of `libch.proto`, which the libch repository shares with this directory byte for byte.
protobuf_generate_cpp(clickhouse_libch_worker_proto_sources clickhouse_libch_worker_proto_headers libch.proto)
add_library(clickhouse_libch_worker_proto ${clickhouse_libch_worker_proto_headers} ${clickhouse_libch_worker_proto_sources})
target_include_directories(clickhouse_libch_worker_proto SYSTEM PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(clickhouse_libch_worker_proto PUBLIC ch_contrib::protobuf)
# Ignore warnings while compiling protobuf-generated *.pb.h and *.pb.cpp files.
target_compile_options(clickhouse_libch_worker_proto PRIVATE "-w")
set_target_properties(clickhouse_libch_worker_proto PROPERTIES CXX_CLANG_TIDY "")

set (CLICKHOUSE_LIBCH_WORKER_SOURCES
    Describe.cpp
    Engine.cpp
    Frames.cpp
    Housekeep.cpp
    Insert.cpp
    LibchWorker.cpp
    Merge.cpp
    Plan.cpp
    Sandbox.cpp
    Usage.cpp
)

set (CLICKHOUSE_LIBCH_WORKER_LINK
    PRIVATE
        clickhouse_aggregate_functions
        clickhouse_common_config
        clickhouse_common_io
        clickhouse_functions
        clickhouse_libch_worker_proto
        clickhouse_parsers
        dbms
)

clickhouse_program_add(libch-worker)
