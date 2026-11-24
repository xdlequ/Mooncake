// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/transport/ub/cold_resolve_scheduler.h"

#include <utility>

#include "tent/transport/ub/endpoint.h"
#include "tent/transport/ub/slice.h"

namespace mooncake::tent::ub {

AsyncColdResolveScheduler::AsyncColdResolveScheduler(
    std::shared_ptr<UbPathControlExecutor> executor, ResolveFn resolve,
    size_t max_entries, uint64_t retry_cooldown_ns)
    : executor_(std::move(executor)),
      resolve_(std::move(resolve)),
      max_entries_(max_entries),
      retry_cooldown_ns_(retry_cooldown_ns) {}

Status AsyncColdResolveScheduler::create(
    std::shared_ptr<UbPathControlExecutor> executor, ResolveFn resolve,
    size_t max_entries, uint64_t retry_cooldown_ns,
    std::shared_ptr<AsyncColdResolveScheduler>& out) {
    if (!executor) {
        return Status::InvalidArgument(
            "async cold resolve requires a control executor");
    }
    if (!resolve) {
        return Status::InvalidArgument("async cold resolve requires a resolver");
    }
    if (max_entries == 0) {
        return Status::InvalidArgument(
            "async cold resolve entry bound must be nonzero");
    }
    out = std::shared_ptr<AsyncColdResolveScheduler>(
        new AsyncColdResolveScheduler(std::move(executor), std::move(resolve),
                                      max_entries, retry_cooldown_ns));
    return Status::OK();
}

std::string AsyncColdResolveScheduler::keyFor(
    const EndpointResolveRequest& request) {
    // The same four fields EndpointStore uses, rendered as a stable string so
    // the scheduler does not need the store's key type or its locks.
    std::string key;
    key += std::to_string(request.local_context
                              ? request.local_context->topologyId()
                              : -1);
    key += ':';
    key += std::to_string(static_cast<int64_t>(request.remote_segment_id));
    key += ':';
    key += std::to_string(request.remote_topology_id);
    key += ':';
    if (request.remote_segment) {
        key += request.remote_segment->name;
        key += '/';
        if (const auto* nic =
                request.remote_segment->getMemory().topology.getNicEntry(
                    request.remote_topology_id)) {
            key += nic->name;
        }
    }
    return key;
}

bool AsyncColdResolveScheduler::request(
    const EndpointResolveRequest& request) {
    const std::string key = keyFor(request);
    const uint64_t now_ns = steadyNowNs();
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ++stats_.requested;
        auto it = entries_.find(key);
        if (it != entries_.end()) {
            Entry& entry = it->second;
            if (entry.in_flight) {
                ++stats_.coalesced;
                return true;
            }
            if (entry.last_failure_ns != 0 &&
                now_ns - entry.last_failure_ns < retry_cooldown_ns_) {
                ++stats_.coalesced;
                return true;
            }
        } else if (entries_.size() >= max_entries_) {
            ++stats_.dropped_full;
            return false;
        }
        Entry& entry = entries_[key];
        entry.in_flight = true;
        entry.last_failure_ns = 0;
    }

    const auto self = shared_from_this();
    if (!executor_->enqueue([self, request, key] { self->run(request, key); })) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = entries_.find(key);
        if (it != entries_.end()) it->second.in_flight = false;
        ++stats_.dropped_full;
        return false;
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        ++stats_.enqueued;
    }
    return true;
}

void AsyncColdResolveScheduler::run(const EndpointResolveRequest& request,
                                    const std::string& key) {
    // Control executor thread only. The registry/transport lock is never held
    // across the resolver, which may perform the bootstrap RPC.
    std::shared_ptr<UbEndpoint> endpoint;
    Status status = Status::InternalError(
        "async cold resolve has no resolver" LOC_MARK);
    if (resolve_) status = resolve_(request, endpoint);

    std::function<void()> hint;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = entries_.find(key);
        if (it != entries_.end()) {
            it->second.in_flight = false;
            if (status.ok() && endpoint && endpoint->ready()) {
                it->second.last_failure_ns = 0;
                ++stats_.completed;
            } else {
                // A resolve that "succeeded" without a ready endpoint is a
                // failure for reflow purposes: retrying through the cooldown
                // keeps a flapping peer from producing an RPC storm.
                it->second.last_failure_ns = steadyNowNs();
                ++stats_.failed;
            }
        }
        hint = ready_hint_;
    }
    if (status.ok() && endpoint && endpoint->ready() && hint) hint();
}

void AsyncColdResolveScheduler::setReadyHint(std::function<void()> hint) {
    std::lock_guard<std::mutex> lock(mutex_);
    ready_hint_ = std::move(hint);
}

ColdResolveStats AsyncColdResolveScheduler::stats() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return stats_;
}

}  // namespace mooncake::tent::ub
