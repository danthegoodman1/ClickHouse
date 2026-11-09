# BigHouse: the InodeDB client's generated gRPC code.
PROTOBUF_GENERATE_GRPC_CPP(inodedb_proto_sources inodedb_proto_headers inodedb.proto)

add_library(clickhouse_inodedb_protos ${inodedb_proto_headers} ${inodedb_proto_sources})
target_include_directories(clickhouse_inodedb_protos SYSTEM PUBLIC ${CMAKE_CURRENT_BINARY_DIR})
target_link_libraries(clickhouse_inodedb_protos PUBLIC ch_contrib::grpc)
# Ignore warnings while compiling protobuf-generated *.pb.h and *.pb.cpp files.
target_compile_options(clickhouse_inodedb_protos PRIVATE "-w")
set_target_properties(clickhouse_inodedb_protos PROPERTIES CXX_CLANG_TIDY "")
