#pragma once

#include <cstddef>

/// BigHouse: sets how many threads the event engines that gRPC creates from now on keep while idle. gRPC's own
/// number is one for each core, between 4 and 16, started with the engine. An engine starts more threads while
/// all of its threads are busy, and stops those after 20 seconds without work.
void grpcSetEventEngineReserveThreads(size_t threads);
