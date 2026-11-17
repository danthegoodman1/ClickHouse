#include "grpc_event_engine_threads.h"

#include <grpc/event_engine/event_engine.h>

#include "src/core/lib/event_engine/posix_engine/posix_engine.h"

void grpcSetEventEngineReserveThreads(size_t threads)
{
    grpc_event_engine::experimental::SetEventEngineFactory(
        [threads]() -> std::shared_ptr<grpc_event_engine::experimental::EventEngine>
        {
            grpc_event_engine::experimental::PosixEventEngine::Options options;
            options.reserve_threads = static_cast<int>(threads);
            return grpc_event_engine::experimental::PosixEventEngine::MakePosixEventEngine(options);
        });
}
