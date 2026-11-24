// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
//
// C03 async bootstrap seam (pooled RM).
//
// Before this component existed, every posting worker that missed the endpoint
// cache ran the bootstrap RPC inline: a synchronous handshake plus native
// import on the post thread. C03 requires cold preparation to be queued and
// coalesced on a bounded control executor, so a post worker never performs RPC
// and never holds a lease while a handshake is in flight.
//
// Contract:
//   * request() is non-blocking. It coalesces by endpoint identity: at most one
//     resolve per key is in flight, and a failed key is not retried before the
//     cooldown expires. A full table/queue refuses the request and the caller
//     keeps deferring.
//   * The resolve function runs on the control executor thread. Transport
//     supplies the same single-flight resolver the synchronous path uses, so
//     concurrent async and inline callers still share one handshake.
//   * A successful resolve invokes the ready hint so parked business slices can
//     be reflowed immediately instead of waiting for the bounded tick.
//   * This class never touches the native provider itself and never holds a
//     lock across the resolve call.

#ifndef TENT_TRANSPORT_UB_COLD_RESOLVE_SCHEDULER_H_
#define TENT_TRANSPORT_UB_COLD_RESOLVE_SCHEDULER_H_

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "tent/common/status.h"
#include "tent/transport/ub/path_registry.h"
#include "tent/transport/ub/workers.h"

namespace mooncake::tent::ub {

struct ColdResolveStats {
    // request() calls, including coalesced duplicates.
    uint64_t requested = 0;
    // Distinct resolve jobs handed to the control executor.
    uint64_t enqueued = 0;
    // Duplicates dropped because the same key was in flight or cooling down.
    uint64_t coalesced = 0;
    uint64_t completed = 0;
    uint64_t failed = 0;
    // Refused because the entry table or the bounded executor was full.
    uint64_t dropped_full = 0;
};

class AsyncColdResolveScheduler final
    : public std::enable_shared_from_this<AsyncColdResolveScheduler> {
   public:
    using ResolveFn = std::function<Status(const EndpointResolveRequest&,
                                           std::shared_ptr<UbEndpoint>&)>;

    AsyncColdResolveScheduler(const AsyncColdResolveScheduler&) = delete;
    AsyncColdResolveScheduler& operator=(const AsyncColdResolveScheduler&) =
        delete;

    // `executor` bounds the work (its enqueue is non-blocking); `max_entries`
    // bounds the coalescing table (one entry per endpoint identity); a failed
    // key is not retried before `retry_cooldown_ns`.
    static Status create(std::shared_ptr<UbPathControlExecutor> executor,
                         ResolveFn resolve, size_t max_entries,
                         uint64_t retry_cooldown_ns,
                         std::shared_ptr<AsyncColdResolveScheduler>& out);

    // Non-blocking. Returns false only when the request was refused (bounded
    // table/queue full); the caller keeps its deferring state and retries.
    bool request(const EndpointResolveRequest& request);

    // Invoked on the control executor after a successful resolve. Set once at
    // install time; may be null (tests).
    void setReadyHint(std::function<void()> hint);

    [[nodiscard]] ColdResolveStats stats() const;

    ~AsyncColdResolveScheduler() = default;

   private:
    struct Entry {
        bool in_flight = false;
        uint64_t last_failure_ns = 0;
    };

    AsyncColdResolveScheduler(std::shared_ptr<UbPathControlExecutor> executor,
                              ResolveFn resolve, size_t max_entries,
                              uint64_t retry_cooldown_ns);

    // Endpoint identity, independent of the caller's route object.
    [[nodiscard]] static std::string keyFor(
        const EndpointResolveRequest& request);

    void run(const EndpointResolveRequest& request, const std::string& key);

    const std::shared_ptr<UbPathControlExecutor> executor_;
    const ResolveFn resolve_;
    const size_t max_entries_;
    const uint64_t retry_cooldown_ns_;

    mutable std::mutex mutex_;
    std::unordered_map<std::string, Entry> entries_;
    std::function<void()> ready_hint_;
    ColdResolveStats stats_;
};

}  // namespace mooncake::tent::ub

#endif  // TENT_TRANSPORT_UB_COLD_RESOLVE_SCHEDULER_H_
