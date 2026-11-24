// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "tent/runtime/segment_manager.h"
#include "tent/runtime/segment_registry.h"
#include "tent/transport/ub/buffers.h"
#include "tent/transport/ub/cold_resolve_scheduler.h"
#include "tent/transport/ub/context.h"
#include "tent/transport/ub/endpoint_store.h"
#include "tent/transport/ub/node_target_quota.h"
#include "tent/transport/ub/quota.h"
#include "tent/transport/ub/rail_monitor.h"
#include "tent/transport/ub/topology_attrs.h"
#include "tent/transport/ub/ub_transport.h"
#include "tent/transport/ub/workers.h"
#include "ub_fake_topology.h"
#include "ub_fake_urma.h"

#include "../src/transport/ub/jetty_pool.h"

namespace mooncake::tent::ub {
namespace {

class FakeContext final : public Context {
   public:
    explicit FakeContext(DeviceInfo info) : info_(std::move(info)) {}
    bool valid() const noexcept override { return valid_; }
    const DeviceInfo& deviceInfo() const noexcept override { return info_; }
    int asyncFd() const noexcept override { return -1; }
    void close() { valid_ = false; }

   private:
    DeviceInfo info_;
    bool valid_{true};
};

class FakeJfc final : public Jfc {
   public:
    bool valid() const noexcept override { return valid_; }
    int eventFd() const noexcept override { return -1; }

    void push(Completion completion) {
        std::lock_guard<std::mutex> lock(mutex_);
        completions_.push_back(completion);
    }
    void poll(size_t maximum, std::vector<Completion>& output) {
        std::lock_guard<std::mutex> lock(mutex_);
        while (!completions_.empty() && output.size() < maximum) {
            output.push_back(completions_.front());
            completions_.pop_front();
        }
    }
    // Test seam for the T-11 contradiction injection: lets a post call that
    // pushed a completion wait until a poller has drained it.
    size_t pendingCount() {
        std::lock_guard<std::mutex> lock(mutex_);
        return completions_.size();
    }
    void close() { valid_ = false; }

   private:
    std::mutex mutex_;
    std::deque<Completion> completions_;
    bool valid_{true};
};

SegmentDescriptor makeDescriptor(uint64_t address, uint64_t length) {
    return SegmentDescriptor{
        SegmentDescriptor::kSchemaVersion, 1, 16,
        std::to_string(address) + ":" + std::to_string(length)};
}

bool parseDescriptor(const SegmentDescriptor& descriptor, uint64_t& address,
                     uint64_t& length) {
    const auto colon = descriptor.hex.find(':');
    if (descriptor.schema_version != SegmentDescriptor::kSchemaVersion ||
        descriptor.urma_api_version != 1 || descriptor.urma_abi_size != 16 ||
        colon == std::string::npos) {
        return false;
    }
    try {
        address = std::stoull(descriptor.hex.substr(0, colon));
        length = std::stoull(descriptor.hex.substr(colon + 1));
        return length != 0;
    } catch (...) {
        return false;
    }
}

class FakeLocalSegment final : public LocalSegment {
   public:
    FakeLocalSegment(uint64_t address, uint64_t length)
        : address_(address),
          length_(length),
          descriptor_(makeDescriptor(address, length)) {}
    bool valid() const noexcept override { return valid_; }
    uint64_t address() const noexcept override { return address_; }
    uint64_t length() const noexcept override { return length_; }
    const SegmentDescriptor& descriptor() const noexcept override {
        return descriptor_;
    }
    void close() { valid_ = false; }

   private:
    uint64_t address_;
    uint64_t length_;
    SegmentDescriptor descriptor_;
    bool valid_{true};
};

class FakeRemoteSegment final : public RemoteSegment {
   public:
    FakeRemoteSegment(uint64_t address, uint64_t length,
                      SegmentDescriptor descriptor)
        : address_(address),
          length_(length),
          descriptor_(std::move(descriptor)) {}
    bool valid() const noexcept override { return valid_; }
    uint64_t address() const noexcept override { return address_; }
    uint64_t length() const noexcept override { return length_; }
    const SegmentDescriptor& descriptor() const noexcept override {
        return descriptor_;
    }
    void close() { valid_ = false; }

   private:
    uint64_t address_;
    uint64_t length_;
    SegmentDescriptor descriptor_;
    bool valid_{true};
};

class FakeJetty final : public Jetty {
   public:
    FakeJetty(uint32_t id, std::shared_ptr<FakeJfc> jfc, bool rm_mode = false)
        : id_(id), jfc_(std::move(jfc)), rm_mode_(rm_mode) {}
    bool valid() const noexcept override { return valid_; }
    uint32_t id() const noexcept override { return id_; }
    uint32_t uasid() const noexcept override { return 0; }
    std::shared_ptr<FakeJfc> jfc() const { return jfc_; }
    bool bound() const { return bound_; }
    bool rmMode() const { return rm_mode_; }
    void bind() { bound_ = true; }
    void unbind() { bound_ = false; }
    void close() {
        valid_ = false;
        bound_ = false;
    }

   private:
    uint32_t id_;
    std::shared_ptr<FakeJfc> jfc_;
    bool rm_mode_{false};
    bool valid_{true};
    bool bound_{false};
};

class FakeRemoteTarget final : public RemoteTarget {
   public:
    FakeRemoteTarget(std::string eid, uint32_t uasid, uint32_t id)
        : eid_(std::move(eid)), uasid_(uasid), id_(id) {}
    bool valid() const noexcept override { return valid_; }
    uint32_t id() const noexcept override { return id_; }
    uint32_t uasid() const noexcept override { return uasid_; }
    const std::string& eid() const noexcept override { return eid_; }
    void close() { valid_ = false; }

   private:
    std::string eid_;
    uint32_t uasid_{0};
    uint32_t id_{0};
    bool valid_{true};
};

class FakeUrmaAdapter final : public UrmaAdapter {
   public:
    explicit FakeUrmaAdapter(DeviceInfo device)
        : FakeUrmaAdapter(std::vector<DeviceInfo>{std::move(device)}) {}
    explicit FakeUrmaAdapter(std::vector<DeviceInfo> devices)
        : devices_(std::move(devices)) {}

    bool available() const noexcept override { return true; }
    uint32_t nativeApiVersion() const noexcept override { return 1; }
    size_t nativeSegmentDescriptorSize() const noexcept override { return 16; }
    Status initialize() override {
        initialized_ = true;
        return Status::OK();
    }
    Status shutdown() override {
        if (fail_next_shutdown_.exchange(false, std::memory_order_acq_rel)) {
            return Status::InternalError("injected adapter shutdown failure");
        }
        initialized_ = false;
        return Status::OK();
    }
    Status discoverDevices(std::vector<DeviceInfo>& devices) override {
        if (!initialized_) return Status::InvalidArgument("not initialized");
        devices = devices_;
        return Status::OK();
    }
    Status openContext(const DeviceInfo& device, ContextPtr& context) override {
        open_context_calls_.fetch_add(1, std::memory_order_relaxed);
        if (!initialized_) return Status::InvalidArgument("not initialized");
        for (const auto& candidate : devices_) {
            if (candidate.topology_name == device.topology_name) {
                context = std::make_shared<FakeContext>(device);
                return Status::OK();
            }
        }
        return Status::DeviceNotFound("fake device missing");
    }
    Status closeContext(ContextPtr& context) override {
        if (auto fake = std::dynamic_pointer_cast<FakeContext>(context)) {
            fake->close();
        }
        context.reset();
        return Status::OK();
    }
    Status createJfc(const ContextPtr&, const JfcOptions&,
                     JfcPtr& jfc) override {
        jfc = std::make_shared<FakeJfc>();
        return Status::OK();
    }
    Status deleteJfc(JfcPtr& jfc) override {
        if (auto fake = std::dynamic_pointer_cast<FakeJfc>(jfc)) fake->close();
        jfc.reset();
        return Status::OK();
    }
    Status registerLocalSegment(const ContextPtr&, uint64_t address,
                                size_t length, const SegmentOptions& options,
                                LocalSegmentPtr& segment) override {
        if (length == 0) return Status::InvalidArgument("empty segment");
        last_registered_access_.store(options.access,
                                      std::memory_order_release);
        segment = std::make_shared<FakeLocalSegment>(address, length);
        return Status::OK();
    }
    Status unregisterLocalSegment(LocalSegmentPtr& segment) override {
        if (auto fake = std::dynamic_pointer_cast<FakeLocalSegment>(segment)) {
            fake->close();
        }
        segment.reset();
        return Status::OK();
    }
    Status importRemoteSegment(const ContextPtr&,
                               const SegmentDescriptor& descriptor,
                               const SegmentOptions&,
                               RemoteSegmentPtr& segment) override {
        uint64_t address = 0;
        uint64_t length = 0;
        if (!parseDescriptor(descriptor, address, length)) {
            return Status::InvalidArgument("bad descriptor");
        }
        segment =
            std::make_shared<FakeRemoteSegment>(address, length, descriptor);
        return Status::OK();
    }
    Status unimportRemoteSegment(RemoteSegmentPtr& segment) override {
        if (auto fake = std::dynamic_pointer_cast<FakeRemoteSegment>(segment)) {
            fake->close();
        }
        segment.reset();
        return Status::OK();
    }
    Status createJetty(const ContextPtr& context, const JfcPtr& jfc,
                       const JettyOptions& options, JettyPtr& jetty) override {
        create_jetty_calls_.fetch_add(1, std::memory_order_relaxed);
        auto fake_jfc = std::dynamic_pointer_cast<FakeJfc>(jfc);
        if (!fake_jfc) return Status::InvalidArgument("bad JFC");
        const bool rm_mode = options.trans_mode == kUrmaTransportModeRm;
        if (rm_mode && (options.bind_tp_type_mask & kUrmaTpTypeCtpMask) == 0) {
            return Status::InvalidArgument(
                "RM JFS bind mask does not cover URMA_CTP");
        }
        // Record the exact options each Context's members were created with so
        // a heterogeneous install can be verified per device (C01 / G-03).
        if (auto fake_context =
                std::dynamic_pointer_cast<FakeContext>(context)) {
            std::lock_guard<std::mutex> lock(created_options_mutex_);
            created_jetty_options_[fake_context->deviceInfo().topology_name]
                .push_back(options);
        }
        jetty =
            std::make_shared<FakeJetty>(next_jetty_id_++, fake_jfc, rm_mode);
        return Status::OK();
    }
    Status deleteJetty(JettyPtr& jetty) override {
        delete_jetty_calls_.fetch_add(1, std::memory_order_relaxed);
        if (auto fake = std::dynamic_pointer_cast<FakeJetty>(jetty)) {
            fake->close();
        }
        jetty.reset();
        return Status::OK();
    }
    Status bindJetty(const JettyPtr& jetty, const RemoteJettyInfo&) override {
        auto fake = std::dynamic_pointer_cast<FakeJetty>(jetty);
        if (!fake || !fake->valid())
            return Status::InvalidArgument("bad Jetty");
        if (fake->rmMode()) {
            return Status::InvalidArgument(
                "RM Legacy Jettys never bind a remote Jetty");
        }
        bind_jetty_calls_.fetch_add(1, std::memory_order_relaxed);
        fake->bind();
        return Status::OK();
    }
    Status unbindJetty(const JettyPtr& jetty) override {
        if (auto fake = std::dynamic_pointer_cast<FakeJetty>(jetty)) {
            fake->unbind();
        }
        return Status::OK();
    }
    Status resetJetty(const JettyPtr&) override {
        if (fail_resets_.load(std::memory_order_acquire)) {
            return Status::RdmaError("injected Jetty reset failure");
        }
        return Status::OK();
    }
    Status importRemoteTarget(const ContextPtr&, const std::string& eid,
                              uint32_t uasid, uint32_t id, uint16_t trans_mode,
                              uint8_t target_type, uint8_t tp_type,
                              RemoteTargetPtr& target) override {
        target.reset();
        import_calls_.fetch_add(1, std::memory_order_relaxed);
        if (trans_mode != kUrmaTransportModeRm ||
            target_type != kUrmaTargetTypeJetty || tp_type != kUrmaTpTypeCtp) {
            return Status::InvalidArgument(
                "RM target profile must be RM/JETTY/CTP");
        }
        if (id == 0) {
            return Status::InvalidArgument("RM target Jetty ID is zero");
        }
        if (fail_imports_.load(std::memory_order_acquire) > 0) {
            fail_imports_.fetch_sub(1, std::memory_order_acq_rel);
            if (partial_import_on_failure_.load(std::memory_order_acquire)) {
                // Provider contract: a failed import may still return a
                // partially allocated handle the caller must adopt.
                target = std::make_shared<FakeRemoteTarget>(eid, uasid, id);
            }
            return Status::RdmaError("injected import failure");
        }
        imported_ids_.push_back(id);
        target = std::make_shared<FakeRemoteTarget>(eid, uasid, id);
        return Status::OK();
    }
    Status unimportRemoteTarget(RemoteTargetPtr& target) override {
        unimport_calls_.fetch_add(1, std::memory_order_relaxed);
        if (fail_unimports_.load(std::memory_order_acquire) > 0) {
            fail_unimports_.fetch_sub(1, std::memory_order_acq_rel);
            return Status::RdmaError("injected unimport failure");
        }
        if (auto fake = std::dynamic_pointer_cast<FakeRemoteTarget>(target)) {
            fake->close();
        }
        target.reset();
        return Status::OK();
    }
    Status quiesceJetty(const JettyPtr& jetty, uint32_t timeout_ms,
                        std::vector<Completion>& completions) override {
        completions.clear();
        auto fake = std::dynamic_pointer_cast<FakeJetty>(jetty);
        if (!fake || !fake->valid() || timeout_ms == 0) {
            return Status::InvalidArgument("bad Jetty quiesce");
        }
        if (fail_next_quiesce_.exchange(false, std::memory_order_acq_rel)) {
            return Status::RdmaError("injected quiesce failure");
        }
        const bool drop_completion = drop_next_quiesced_completion_.exchange(
            false, std::memory_order_acq_rel);
        std::lock_guard<std::mutex> lock(pending_mutex_);
        if (!quiesce_drops_completions_.load(std::memory_order_acquire)) {
            auto it = pending_.begin();
            while (it != pending_.end()) {
                if (it->jetty_id != fake->id()) {
                    ++it;
                    continue;
                }
                if (!drop_completion) {
                    completions.push_back(
                        Completion{CompletionCategory::ENDPOINT_ERROR, 0,
                                   it->request.token, 0, fake->id()});
                }
                it = pending_.erase(it);
            }
        }
        quiesce_calls_.fetch_add(1, std::memory_order_relaxed);
        return Status::OK();
    }
    // Asynchronous drain primitives (design tent-ub-async-drain-design §4.1).
    // The pooled error path runs the chain on the control executor, so the fake
    // provider emits the flush-done marker the way a real one does: because the
    // Jetty entered ERROR. The marker travels through the JFC poller, which is
    // exactly the path this integration test covers.
    static constexpr int kFlushDoneStatus = 0x21;

    Status modifyJettyError(const JettyPtr& jetty, uint64_t) override {
        auto fake = std::dynamic_pointer_cast<FakeJetty>(jetty);
        if (!fake || !fake->valid()) {
            return Status::InvalidArgument("bad Jetty modify");
        }
        modify_error_calls_.fetch_add(1, std::memory_order_relaxed);
        if (auto jfc = fake->jfc()) {
            jfc->push(Completion{CompletionCategory::ENDPOINT_ERROR,
                                 kFlushDoneStatus, 0, 0, fake->id()});
        }
        return Status::OK();
    }

    EntityMarkerKind classifyEntityMarker(
        const Completion& completion) const override {
        return completion.native_status == kFlushDoneStatus
                   ? EntityMarkerKind::kFlushDone
                   : EntityMarkerKind::kNone;
    }

    Status flushJettyOnce(const JettyPtr& jetty,
                          std::vector<Completion>& out) override {
        out.clear();
        auto fake = std::dynamic_pointer_cast<FakeJetty>(jetty);
        if (!fake || !fake->valid()) {
            return Status::InvalidArgument("bad Jetty flush");
        }
        flush_jetty_calls_.fetch_add(1, std::memory_order_relaxed);
        return Status::OK();
    }

    Status finishDrain(const JettyPtr& jetty) override {
        auto fake = std::dynamic_pointer_cast<FakeJetty>(jetty);
        if (!fake || !fake->valid()) {
            return Status::InvalidArgument("bad Jetty finish");
        }
        finish_drain_calls_.fetch_add(1, std::memory_order_relaxed);
        return Status::OK();
    }

    DeleteOutcome deleteJettyWithOutcome(JettyPtr& jetty) override {
        DeleteOutcome outcome;
        outcome.status = deleteJetty(jetty);
        outcome.kind = outcome.status.ok() ? DeleteOutcome::Kind::kConsumed
                                           : DeleteOutcome::Kind::kUnknown;
        return outcome;
    }

    Status post(const JettyPtr& jetty, const std::vector<WorkRequest>& requests,
                size_t& posted_count) override {
        posted_count = 0;
        // T-10 diagnostics: every invocation and every WR crossing this
        // boundary, so a test can prove one post call carries exactly one
        // WR per pooled attempt.
        post_calls_.fetch_add(1, std::memory_order_relaxed);
        posted_wrs_.fetch_add(requests.size(), std::memory_order_relaxed);
        auto fake = std::dynamic_pointer_cast<FakeJetty>(jetty);
        if (!fake || !fake->valid() || (!fake->bound() && !fake->rmMode())) {
            return Status::InvalidArgument("unbound Jetty");
        }
        const bool rm_mode = fake->rmMode();
        for (const auto& request : requests) {
            if (rm_mode != (request.target != nullptr)) {
                return Status::InvalidArgument(
                    rm_mode ? "RM work requests must set an explicit target"
                            : "RC work requests must not set a target");
            }
            if (!request.local_segment || !request.remote_segment ||
                request.token == 0 || request.length == 0) {
                return Status::InvalidArgument("bad WR");
            }
        }
        if (fail_next_post_.exchange(false, std::memory_order_acq_rel)) {
            return Status::RdmaError("injected post rejection");
        }
        if (accept_then_fail_post_.exchange(false, std::memory_order_acq_rel)) {
            // Mirrors a contradictory provider result: the WR crossed the
            // native boundary but the post still reports an error. The RM
            // acceptance classifier must map this to Unknown.
            for (const auto& request : requests) {
                std::lock_guard<std::mutex> lock(pending_mutex_);
                pending_.push_back(Pending{fake->id(), request});
                ++posted_count;
            }
            return Status::RdmaError("injected post error after acceptance");
        }
        if (not_accept_but_complete_.exchange(false,
                                              std::memory_order_acq_rel)) {
            // Mirrors the T-11/C07 contradiction: the post reports a
            // definite zero acceptance (posted_count stays zero), yet the
            // hardware executed the WR and a real success CQE is delivered
            // for its token.
            for (const auto& request : requests) {
                if (request.operation == Operation::WRITE) {
                    std::memcpy(
                        reinterpret_cast<void*>(request.remote_address),
                        reinterpret_cast<const void*>(request.local_address),
                        request.length);
                }
                Completion completion{
                    CompletionCategory::SUCCESS, 0, request.token,
                    static_cast<uint32_t>(request.length), fake->id()};
                if (hold_completions_.load(std::memory_order_acquire)) {
                    std::lock_guard<std::mutex> lock(held_completions_mutex_);
                    held_completions_.emplace_back(fake->jfc(),
                                                   std::move(completion));
                } else {
                    fake->jfc()->push(std::move(completion));
                }
            }
            if (!hold_completions_.load(std::memory_order_acquire)) {
                // Deterministic early-CQE window: hold this contradictory
                // post until the poller has drained the CQE (and, with the
                // extra margin, dispatched it into the completion gate), so
                // the zero-acceptance fact always lands on a record that
                // already carries the real completion. Without this, the
                // gate may legally take the clean reject exit before the
                // CQE becomes visible, which is a different (also legal)
                // timing, not the contradiction this test targets.
                const auto wait_until =
                    std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(500);
                while (fake->jfc()->pendingCount() != 0 &&
                       std::chrono::steady_clock::now() < wait_until) {
                    std::this_thread::sleep_for(
                        std::chrono::microseconds(50));
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
            return Status::RdmaError("injected zero acceptance with execution");
        }
        for (const auto& request : requests) {
            if (rm_mode) {
                last_post_target_id_.store(request.target->id(),
                                           std::memory_order_release);
                rm_post_calls_.fetch_add(1, std::memory_order_relaxed);
            }
            const auto category = next_completion_.exchange(
                CompletionCategory::SUCCESS, std::memory_order_acq_rel);
            if (hold_next_completion_.exchange(false,
                                               std::memory_order_acq_rel)) {
                std::lock_guard<std::mutex> lock(pending_mutex_);
                pending_.push_back(Pending{fake->id(), request});
                ++posted_count;
                continue;
            }
            if (category == CompletionCategory::SUCCESS) {
                if (request.operation == Operation::WRITE) {
                    std::memcpy(
                        reinterpret_cast<void*>(request.remote_address),
                        reinterpret_cast<const void*>(request.local_address),
                        request.length);
                } else {
                    std::memcpy(
                        reinterpret_cast<void*>(request.local_address),
                        reinterpret_cast<const void*>(request.remote_address),
                        request.length);
                }
            }
            uint32_t reported_bytes = category == CompletionCategory::SUCCESS
                                          ? static_cast<uint32_t>(request.length)
                                          : 0;
            const int32_t length_delta = mismatch_next_completion_bytes_
                .exchange(0, std::memory_order_acq_rel);
            if (length_delta != 0 &&
                category == CompletionCategory::SUCCESS) {
                // T-11 / G-04: a success CQE that reports a byte count
                // different from the submitted WR length.
                reported_bytes = static_cast<uint32_t>(
                    static_cast<int64_t>(reported_bytes) + length_delta);
            }
            Completion completion{category, 0, request.token, reported_bytes,
                                  fake->id()};
            if (hold_completions_.load(std::memory_order_acquire)) {
                // Parked instead of delivered: the test releases them once
                // every slice of the task has been dispatched, so rail load
                // (quota + inflight) stays visible to path selection.
                std::lock_guard<std::mutex> lock(held_completions_mutex_);
                held_completions_.emplace_back(fake->jfc(),
                                               std::move(completion));
            } else {
                fake->jfc()->push(std::move(completion));
            }
            ++posted_count;
        }
        return Status::OK();
    }
    Status poll(const JfcPtr& jfc, size_t maximum,
                std::vector<Completion>& completions) override {
        completions.clear();
        auto fake = std::dynamic_pointer_cast<FakeJfc>(jfc);
        if (!fake || !fake->valid()) return Status::InvalidArgument("bad JFC");
        fake->poll(maximum, completions);
        return Status::OK();
    }

    void failNextCompletion(CompletionCategory category) {
        next_completion_.store(category, std::memory_order_release);
    }
    // T-13 / C02: the capability injector. Tests may pin any contract (e.g.
    // strip a capability to make the production pool gate fail closed, or
    // declare kStrictMatch length semantics); the default reports the full
    // fake capability set so the async drain chain stays verifiable.
    ProviderContract contract_;
    [[nodiscard]] ProviderContract providerContract() const override {
        if (!contract_.provider_id.empty()) return contract_;
        ProviderContract full;
        full.provider_id = "fake-urma";
        full.native_api_version = 1;
        full.abi_size = 64;
        full.software_flush = true;
        full.marker_flush = true;
        full.delete_tristate = true;
        full.async_drain = true;
        full.event_barrier = true;
        full.length_semantics = LengthSemantics::kZeroIsUnreported;
        return full;
    }
    void injectProviderContract(const ProviderContract& contract) {
        contract_ = contract;
    }
    // Reject the next post before accepting any WR: posted_count stays zero,
    // which exercises the synchronous rollback path.
    void failNextPostWithoutAccept() {
        fail_next_post_.store(true, std::memory_order_release);
    }
    // Accept the next post then report an error: posted_count advances while
    // the status fails, which the RM classifier must treat as Unknown.
    void failNextPostAfterAccept() {
        accept_then_fail_post_.store(true, std::memory_order_release);
    }
    // T-11 / C07: the next post reports a definite zero acceptance while the
    // fake still executes the WR and delivers a real success CQE for it.
    void failNextPostWithoutAcceptButComplete() {
        not_accept_but_complete_.store(true, std::memory_order_release);
    }
    // T-11 / G-04: the next success completion reports a byte count that
    // differs from the submitted WR length by `delta` (short or overlong).
    void reportNextCompletionLengthDelta(int32_t delta) {
        mismatch_next_completion_bytes_.store(delta,
                                             std::memory_order_release);
    }
    void failNextImports(int count) {
        fail_imports_.store(count, std::memory_order_release);
    }
    void setPartialImportOnFailure(bool enabled) {
        partial_import_on_failure_.store(enabled, std::memory_order_release);
    }
    void failNextUnimports(int count) {
        fail_unimports_.store(count, std::memory_order_release);
    }
    void holdNextCompletion() {
        hold_next_completion_.store(true, std::memory_order_release);
    }
    void dropNextQuiescedCompletion() {
        drop_next_quiesced_completion_.store(true, std::memory_order_release);
    }
    void failNextQuiesce() {
        fail_next_quiesce_.store(true, std::memory_order_release);
    }
    void dropQuiesceCompletions() {
        quiesce_drops_completions_.store(true, std::memory_order_release);
    }
    void failResets() { fail_resets_.store(true, std::memory_order_release); }
    void allowResets() { fail_resets_.store(false, std::memory_order_release); }
    void failNextShutdown() {
        fail_next_shutdown_.store(true, std::memory_order_release);
    }
    // Snapshot of the JettyOptions every member on `topology_name` was created
    // with, in creation order (C01 / G-03 per-Context profile verification).
    std::vector<JettyOptions> createdJettyOptions(
        const std::string& topology_name) const {
        std::lock_guard<std::mutex> lock(created_options_mutex_);
        auto found = created_jetty_options_.find(topology_name);
        if (found == created_jetty_options_.end()) return {};
        return found->second;
    }
    size_t pendingCount() const {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        return pending_.size();
    }
    uint64_t quiesceCalls() const {
        return quiesce_calls_.load(std::memory_order_relaxed);
    }
    uint64_t modifyErrorCalls() const {
        return modify_error_calls_.load(std::memory_order_relaxed);
    }
    uint64_t flushJettyCalls() const {
        return flush_jetty_calls_.load(std::memory_order_relaxed);
    }
    uint64_t finishDrainCalls() const {
        return finish_drain_calls_.load(std::memory_order_relaxed);
    }
    uint64_t openContextCalls() const {
        return open_context_calls_.load(std::memory_order_relaxed);
    }
    uint64_t createJettyCalls() const {
        return create_jetty_calls_.load(std::memory_order_relaxed);
    }
    uint64_t deleteJettyCalls() const {
        return delete_jetty_calls_.load(std::memory_order_relaxed);
    }
    uint64_t bindJettyCalls() const {
        return bind_jetty_calls_.load(std::memory_order_relaxed);
    }
    uint64_t importCalls() const {
        return import_calls_.load(std::memory_order_relaxed);
    }
    uint64_t unimportCalls() const {
        return unimport_calls_.load(std::memory_order_relaxed);
    }
    uint64_t rmPostCalls() const {
        return rm_post_calls_.load(std::memory_order_relaxed);
    }
    // T-10: total post invocations and total WRs handed to post(), across
    // every mode; equal counts prove each call carried exactly one WR.
    uint64_t postCalls() const {
        return post_calls_.load(std::memory_order_relaxed);
    }
    uint64_t postedWrs() const {
        return posted_wrs_.load(std::memory_order_relaxed);
    }
    // Parks every posted WR's completion instead of delivering it to the
    // JFC (successful WRITE data still lands at post time). Lets a test
    // keep rails loaded while the slices of one task dispatch
    // back-to-back, making device spreading deterministic.
    void holdAllCompletions() {
        hold_completions_.store(true, std::memory_order_release);
    }
    void releaseHeldCompletions() {
        hold_completions_.store(false, std::memory_order_release);
        std::vector<std::pair<std::shared_ptr<FakeJfc>, Completion>> drained;
        {
            std::lock_guard<std::mutex> lock(held_completions_mutex_);
            drained = std::move(held_completions_);
            held_completions_.clear();
        }
        for (auto& entry : drained) {
            if (entry.first && entry.first->valid()) {
                entry.first->push(std::move(entry.second));
            }
        }
    }
    uint32_t lastPostedTargetId() const {
        return last_post_target_id_.load(std::memory_order_relaxed);
    }
    const std::vector<uint32_t>& importedIds() const { return imported_ids_; }
    uint32_t lastRegisteredAccess() const {
        return last_registered_access_.load(std::memory_order_acquire);
    }

   private:
    struct Pending {
        uint32_t jetty_id{0};
        WorkRequest request;
    };

    std::vector<DeviceInfo> devices_;
    bool initialized_{false};
    uint32_t next_jetty_id_{1};
    std::atomic<CompletionCategory> next_completion_{
        CompletionCategory::SUCCESS};
    std::atomic<bool> hold_next_completion_{false};
    std::atomic<bool> hold_completions_{false};
    mutable std::mutex held_completions_mutex_;
    std::vector<std::pair<std::shared_ptr<FakeJfc>, Completion>>
        held_completions_;
    std::atomic<bool> fail_next_post_{false};
    std::atomic<bool> accept_then_fail_post_{false};
    // T-11 injections: zero acceptance with real execution, and success
    // completions whose reported byte count contradicts the WR length.
    std::atomic<bool> not_accept_but_complete_{false};
    std::atomic<int32_t> mismatch_next_completion_bytes_{0};
    std::atomic<int> fail_imports_{0};
    std::atomic<bool> partial_import_on_failure_{false};
    std::atomic<int> fail_unimports_{0};
    std::atomic<bool> drop_next_quiesced_completion_{false};
    std::atomic<bool> fail_next_quiesce_{false};
    std::atomic<bool> quiesce_drops_completions_{false};
    std::atomic<bool> fail_resets_{false};
    std::atomic<bool> fail_next_shutdown_{false};
    mutable std::mutex pending_mutex_;
    std::vector<Pending> pending_;
    std::vector<uint32_t> imported_ids_;
    std::atomic<uint64_t> quiesce_calls_{0};
    std::atomic<uint64_t> modify_error_calls_{0};
    std::atomic<uint64_t> flush_jetty_calls_{0};
    std::atomic<uint64_t> finish_drain_calls_{0};
    std::atomic<uint64_t> open_context_calls_{0};
    std::atomic<uint64_t> create_jetty_calls_{0};
    std::atomic<uint64_t> delete_jetty_calls_{0};
    std::atomic<uint64_t> bind_jetty_calls_{0};
    std::atomic<uint64_t> import_calls_{0};
    std::atomic<uint64_t> unimport_calls_{0};
    std::atomic<uint64_t> rm_post_calls_{0};
    std::atomic<uint64_t> post_calls_{0};
    std::atomic<uint64_t> posted_wrs_{0};
    std::atomic<uint32_t> last_post_target_id_{0};
    std::atomic<uint32_t> last_registered_access_{0};
    mutable std::mutex created_options_mutex_;
    std::map<std::string, std::vector<JettyOptions>> created_jetty_options_;
};

class NullRegistry final : public SegmentRegistry {
   public:
    Status getSegmentDesc(SegmentDescRef&, const std::string&) override {
        return Status::InvalidEntry("not used");
    }
    Status putSegmentDesc(SegmentDescRef&) override { return Status::OK(); }
    Status deleteSegmentDesc(const std::string&) override {
        return Status::OK();
    }
};

DeviceInfo fakeDevice() {
    DeviceInfo info;
    info.topology_name = "ub:fake0:eid0";
    info.native_device_name = "fake0";
    info.native_device_path = "/fake/fake0";
    info.eid_index = 0;
    info.eid = "0001:0000:0000:0000:0000:0000:0000:0000";
    info.active = true;
    info.capabilities.max_jfc = 4;
    info.capabilities.max_jetty = 64;
    return info;
}

std::shared_ptr<Topology> fakeTopology(bool discovery_active = true) {
    auto topology = std::make_shared<Topology>();
    Topology::NicEntry nic{.name = "ub:fake0:eid0",
                           .pci_bus_id = "0000:00:00.0",
                           .type = Topology::NIC_UB,
                           .numa_node = 0};
    auto device = fakeDevice();
    device.active = discovery_active;
    encodeTopologyDeviceAttributes(device, 0, nic.device_attrs);
    topology->nic_list_.push_back(std::move(nic));
    Topology::MemEntry memory;
    memory.name = kWildcardLocation;
    memory.type = Topology::MEM_HOST;
    memory.numa_node = -1;
    memory.device_list[0].push_back(0);
    topology->mem_list_.push_back(std::move(memory));
    return topology;
}

DeviceInfo fakeSecondaryDevice() {
    DeviceInfo info;
    info.topology_name = "ub:fake1:eid0";
    info.native_device_name = "fake1";
    info.native_device_path = "/fake/fake1";
    info.eid_index = 0;
    info.eid = "0002:0000:0000:0000:0000:0000:0000:0000";
    info.active = true;
    info.capabilities.max_jfc = 4;
    info.capabilities.max_jetty = 64;
    return info;
}

UbBootstrapDesc makeRmBootstrapV2Request() {
    UbBootstrapDesc desc;
    desc.protocol_version = 2;
    desc.segment_name = "fake-segment";
    desc.local_nic_path = "fake-segment/ub:fake0:eid0";
    desc.peer_nic_path = "fake-segment/ub:fake0:eid0";
    desc.local_device_name = "fake0";
    desc.local_device_id = 0;
    desc.local_eid_index = 0;
    desc.local_eid = fakeDevice().eid;
    desc.endpoint_generation = 1;
    desc.segment_generation = 1;

    UbRmTargetDescriptor target;
    target.target_identity = "P0";
    target.eid = desc.local_eid;
    target.uasid = 0;
    target.id = 1;
    target.target_epoch = 1;
    target.profile.trans_mode = kUrmaTransportModeRm;
    target.profile.target_type = kUrmaTargetTypeJetty;
    target.profile.tp_type = kUrmaTpTypeCtp;

    UbRmBootstrapDesc rm;
    rm.transport_mode = "RM";
    rm.device_kind = "PHYSICAL";
    rm.physical_device_identity = "/fake/fake0#0001";
    rm.node_incarnation = 1;
    rm.context_incarnation = 1;
    rm.topology_epoch = 1;
    rm.transport_modes = 3;
    rm.provider_capabilities = {"read", "write", "rm"};
    rm.targets = {target};
    rm.probe_target_map = {{"P0", {"P0"}}};
    desc.rm = std::move(rm);
    return desc;
}

void setDeviceIdentity(Config& config, const DeviceInfo& device,
                       const char* kind) {
    config.set("transports/ub/physical_only", true);
    config.set(
        "transports/ub/device_identities",
        nlohmann::json::array(
            {{{"native_device_name", device.native_device_name},
              {"native_device_path", device.native_device_path},
              {"eid_index", device.eid_index},
              {"eid", device.eid},
              {"kind", kind},
              {"transport_modes", device.capabilities.transport_modes}}}));
}

void setPhysicalOnlyIdentity(Config& config, const DeviceInfo& device) {
    setDeviceIdentity(config, device, "physical");
}

void setPhysicalOnlyIdentities(Config& config,
                               const std::vector<DeviceInfo>& devices) {
    nlohmann::json array = nlohmann::json::array();
    for (const auto& device : devices) {
        array.push_back({{"native_device_name", device.native_device_name},
                         {"native_device_path", device.native_device_path},
                         {"eid_index", device.eid_index},
                         {"eid", device.eid},
                         {"kind", "physical"},
                         {"transport_modes",
                          device.capabilities.transport_modes}});
    }
    config.set("transports/ub/physical_only", true);
    config.set("transports/ub/device_identities", array);
}

// Two UB NICs (device ids 0 and 1) both reachable from the wildcard memory
// location, so a buffer registered on both contexts advertises two remote
// devices and one task's slices can split across two endpoints.
std::shared_ptr<Topology> fakeTwoDeviceTopology() {
    auto topology = std::make_shared<Topology>();
    Topology::NicEntry nic0{.name = "ub:fake0:eid0",
                            .pci_bus_id = "0000:00:00.0",
                            .type = Topology::NIC_UB,
                            .numa_node = 0};
    Topology::NicEntry nic1{.name = "ub:fake1:eid0",
                            .pci_bus_id = "0000:00:01.0",
                            .type = Topology::NIC_UB,
                            .numa_node = 0};
    encodeTopologyDeviceAttributes(fakeDevice(), 0, nic0.device_attrs);
    encodeTopologyDeviceAttributes(fakeSecondaryDevice(), 1, nic1.device_attrs);
    topology->nic_list_.push_back(std::move(nic0));
    topology->nic_list_.push_back(std::move(nic1));
    Topology::MemEntry memory;
    memory.name = kWildcardLocation;
    memory.type = Topology::MEM_HOST;
    memory.numa_node = -1;
    memory.device_list[0].push_back(0);
    memory.device_list[0].push_back(1);
    topology->mem_list_.push_back(std::move(memory));
    return topology;
}

// Exercises one worker lane with a single 16-byte WR, so every admitted slice
// maps to exactly one addInflight/removeInflight pair. The per-slice retry
// budget is supplied by the caller to pin terminal outcomes.
class SingleSliceHarness {
   public:
    static constexpr size_t kSliceBytes = 16;

    ~SingleSliceHarness() {
        if (workers) (void)workers->stop();
        if (endpoints) (void)endpoints->clear();
        if (buffers) (void)buffers->clear();
        if (context) (void)context->shutdown();
        if (adapter) (void)adapter->shutdown();
    }

    bool start(uint32_t slice_timeout_ms = 20) {
        adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
        if (!adapter->initialize().ok()) return false;
        context = std::make_shared<UbContext>(0, fakeDevice(), adapter);
        if (!context->initialize(1, JfcOptions{}).ok()) return false;
        contexts = {context};
        topology = fakeTopology();
        buffers = std::make_unique<UbBufferManager>(adapter, contexts);

        source_desc.addr = reinterpret_cast<uint64_t>(source.data());
        source_desc.length = source.size();
        source_desc.location = kWildcardLocation;
        target_desc.addr = reinterpret_cast<uint64_t>(target.data());
        target_desc.length = target.size();
        target_desc.location = kWildcardLocation;
        MemoryOptions options;
        options.perm = kGlobalReadWrite;
        if (!buffers->addBuffer(source_desc, options).ok()) return false;
        if (!buffers->addBuffer(target_desc, options).ok()) return false;

        manager =
            std::make_unique<SegmentManager>(std::make_unique<NullRegistry>());
        if (!manager
                 ->updateLocal([&](SegmentDesc& segment) {
                     segment.name = "local";
                     segment.type = SegmentType::Memory;
                     segment.detail = MemorySegmentDesc{};
                     auto& memory = std::get<MemorySegmentDesc>(segment.detail);
                     memory.topology = *topology;
                     memory.buffers = {source_desc, target_desc};
                     return Status::OK();
                 })
                 .ok()) {
            return false;
        }

        endpoints = std::make_unique<EndpointStore>(adapter, 16, 1);
        params.worker_count = 1;
        params.poller_count = 1;
        params.slice_size = kSliceBytes;
        params.max_retries = 1;
        params.slice_timeout_ms = slice_timeout_ms;

        EndpointResolver resolver = [this](
                                        const EndpointResolveRequest& request,
                                        std::shared_ptr<UbEndpoint>& resolved) {
            UbEndpointKey key{
                request.local_context->topologyId(), request.remote_segment_id,
                request.remote_topology_id, "local@ub:fake0:eid0"};
            auto status =
                endpoints->getOrCreate(key, request.local_context, resolved);
            if (!status.ok()) return status;
            endpoint = resolved;
            if (resolved->ready()) return status;
            UbBootstrapDesc peer;
            peer.local_eid = fakeDevice().eid;
            peer.endpoint_generation = 100;
            peer.jetty_ids = {777};
            return resolved->bind(peer);
        };
        workers = std::make_unique<UbWorkers>(
            adapter, contexts, topology, manager.get(), buffers.get(), &rails,
            &quota, params, std::move(resolver),
            [this](const std::shared_ptr<UbEndpoint>& retired) {
                (void)endpoints->retire(retired);
            });
        return workers->start().ok();
    }

    UbTask::Ptr submit(uint32_t slice_max_retries,
                       Request::OpCode opcode = Request::WRITE) {
        Request request{};
        request.opcode = opcode;
        request.source = source.data();
        request.target_id = LOCAL_SEGMENT_ID;
        request.target_offset = reinterpret_cast<uint64_t>(target.data());
        request.length = kSliceBytes;
        auto task = UbTask::create(request);
        if (!task->addSlice(UbSliceSpec{
                source.data(), reinterpret_cast<uint64_t>(target.data()),
                kSliceBytes, 0, slice_max_retries})) {
            return nullptr;
        }
        if (!task->seal()) return nullptr;
        if (!workers->submit(task).ok()) return nullptr;
        return task;
    }

    bool waitForPending(size_t expected,
                        std::chrono::milliseconds timeout) const {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (adapter->pendingCount() != expected &&
               std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return adapter->pendingCount() == expected;
    }

    static bool waitForTerminal(const UbTask::Ptr& task,
                                std::chrono::milliseconds timeout) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (task->transferStatus().s == PENDING &&
               std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return task->transferStatus().s != PENDING;
    }

    // Snapshot of every counter a P0-3 assertion can depend on. Attaching it to
    // assertions makes a CI failure print the whole state instead of a single
    // delta that says nothing about which side of the pair leaked.
    std::string diagnostics() const {
        std::ostringstream out;
        out << "workers_inflight=" << (workers ? workers->inflightCount() : 0)
            << " context_state="
            << (context ? static_cast<int>(context->state()) : -1)
            << " context_wrs=" << (context ? context->outstandingWrs() : 0)
            << " context_bytes=" << (context ? context->inflightBytes() : 0)
            << " quota=" << quota.activeReservationCount()
            << " endpoint_wrs=" << (endpoint ? endpoint->outstandingWrs() : 0)
            << " endpoint_bytes="
            << (endpoint ? endpoint->outstandingBytes() : 0)
            << " adapter_pending=" << (adapter ? adapter->pendingCount() : 0)
            << " quiesce_calls=" << (adapter ? adapter->quiesceCalls() : 0);
        return out.str();
    }

    static std::string describeTask(const UbTask::Ptr& task) {
        if (!task) return "task=null";
        const auto status = task->transferStatus();
        std::ostringstream out;
        out << "task_status=" << static_cast<int>(status.s)
            << " task_bytes=" << status.transferred_bytes;
        return out.str();
    }

    std::shared_ptr<FakeUrmaAdapter> adapter;
    std::shared_ptr<UbContext> context;
    std::vector<UbContextPtr> contexts;
    std::shared_ptr<Topology> topology;
    std::unique_ptr<UbBufferManager> buffers;
    std::unique_ptr<SegmentManager> manager;
    std::unique_ptr<EndpointStore> endpoints;
    RailMonitor rails;
    QuotaManager quota;
    UbParams params;
    std::shared_ptr<UbEndpoint> endpoint;
    std::unique_ptr<UbWorkers> workers;
    std::array<char, kSliceBytes> source{};
    std::array<char, kSliceBytes> target{};
    BufferDesc source_desc{};
    BufferDesc target_desc{};
};

TEST(UbNativeDataPathTest, LocalBuffersRequestProviderLocalOnlyAccess) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    auto context = std::make_shared<UbContext>(0, fakeDevice(), adapter);
    ASSERT_TRUE(context->initialize(1, JfcOptions{}).ok());
    UbBufferManager buffers(adapter, {context});

    std::array<char, 64> storage{};
    BufferDesc descriptor{};
    descriptor.addr = reinterpret_cast<uint64_t>(storage.data());
    descriptor.length = storage.size();
    descriptor.location = kWildcardLocation;
    MemoryOptions options;
    options.perm = kLocalReadWrite;

    ASSERT_TRUE(buffers.addBuffer(descriptor, options).ok());
    EXPECT_EQ(adapter->lastRegisteredAccess(), SEGMENT_ACCESS_LOCAL_ONLY);

    EXPECT_TRUE(buffers.clear().ok());
    EXPECT_TRUE(context->shutdown().ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbNativeDataPathTest,
     MissingCompletionIsFencedAndUnknownWriteFailsClosed) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    auto context = std::make_shared<UbContext>(0, fakeDevice(), adapter);
    ASSERT_TRUE(context->initialize(1, JfcOptions{}).ok());
    std::vector<UbContextPtr> contexts{context};
    auto topology = fakeTopology();
    UbBufferManager buffers(adapter, contexts);

    std::array<char, 64> source{};
    std::array<char, 64> target{};
    for (size_t i = 0; i < source.size(); ++i) {
        source[i] = static_cast<char>(i + 1);
    }
    BufferDesc source_desc{};
    source_desc.addr = reinterpret_cast<uint64_t>(source.data());
    source_desc.length = source.size();
    source_desc.location = kWildcardLocation;
    BufferDesc target_desc{};
    target_desc.addr = reinterpret_cast<uint64_t>(target.data());
    target_desc.length = target.size();
    target_desc.location = kWildcardLocation;
    MemoryOptions options;
    options.perm = kGlobalReadWrite;
    ASSERT_TRUE(buffers.addBuffer(source_desc, options).ok());
    ASSERT_TRUE(buffers.addBuffer(target_desc, options).ok());

    SegmentManager manager(std::make_unique<NullRegistry>());
    ASSERT_TRUE(manager
                    .updateLocal([&](SegmentDesc& segment) {
                        segment.name = "local";
                        segment.type = SegmentType::Memory;
                        segment.detail = MemorySegmentDesc{};
                        auto& memory =
                            std::get<MemorySegmentDesc>(segment.detail);
                        memory.topology = *topology;
                        memory.buffers = {source_desc, target_desc};
                        return Status::OK();
                    })
                    .ok());

    EndpointStore endpoints(adapter, 16, 1);
    RailMonitor rails;
    QuotaManager quota;
    UbParams params;
    params.worker_count = 1;
    params.poller_count = 1;
    params.slice_size = 16;
    params.max_retries = 1;
    params.slice_timeout_ms = 20;

    EndpointResolver resolver = [&](const EndpointResolveRequest& request,
                                    std::shared_ptr<UbEndpoint>& endpoint) {
        UbEndpointKey key{request.local_context->topologyId(),
                          request.remote_segment_id, request.remote_topology_id,
                          "local@ub:fake0:eid0"};
        auto status =
            endpoints.getOrCreate(key, request.local_context, endpoint);
        if (!status.ok() || endpoint->ready()) return status;
        UbBootstrapDesc peer;
        peer.local_eid = fakeDevice().eid;
        peer.endpoint_generation = 100;
        peer.jetty_ids = {777};
        return endpoint->bind(peer);
    };
    UbWorkers workers(adapter, contexts, topology, &manager, &buffers, &rails,
                      &quota, params, std::move(resolver),
                      [&](const std::shared_ptr<UbEndpoint>& endpoint) {
                          (void)endpoints.retire(endpoint);
                      });
    ASSERT_TRUE(workers.start().ok());
    adapter->holdNextCompletion();
    adapter->dropNextQuiescedCompletion();

    Request request{};
    request.opcode = Request::WRITE;
    request.source = source.data();
    request.target_id = LOCAL_SEGMENT_ID;
    request.target_offset = reinterpret_cast<uint64_t>(target.data());
    request.length = source.size();
    auto task = UbTask::create(request);
    for (size_t offset = 0; offset < request.length; offset += 16) {
        ASSERT_NE(task->addSlice(UbSliceSpec{
                      source.data() + offset,
                      reinterpret_cast<uint64_t>(target.data() + offset), 16,
                      offset, 1}),
                  nullptr);
    }
    ASSERT_TRUE(task->seal());
    ASSERT_TRUE(workers.submit(task).ok());

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (task->transferStatus().s == PENDING &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    // T-12 / G-05: the lost-completion WRITE is fenced (resource safety is
    // proven), but its execution outcome stays Unknown, so the slice must
    // take the terminal TIMEOUT sidecar instead of a transparent replay
    // (C07: a WRITE that may already have taken effect is never replayed).
    EXPECT_EQ(task->transferStatus().s, TIMEOUT);
    EXPECT_GT(rails.aggregateBandwidth(), 0.0);
    EXPECT_GE(rails.stats(UbPostPath{0, LOCAL_SEGMENT_ID, 0, 1}).timeouts, 1U);
    EXPECT_EQ(adapter->pendingCount(), 0U);
    EXPECT_GE(adapter->quiesceCalls(), 1U);

    EXPECT_TRUE(workers.stop().ok());
    EXPECT_TRUE(endpoints.clear().ok());
    EXPECT_TRUE(buffers.clear().ok());
    EXPECT_TRUE(context->shutdown().ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbNativeDataPathTest,
     ProviderLostCompletionIsReclaimedAfterSuccessfulFence) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    auto context = std::make_shared<UbContext>(0, fakeDevice(), adapter);
    ASSERT_TRUE(context->initialize(1, JfcOptions{}).ok());
    std::vector<UbContextPtr> contexts{context};
    auto topology = fakeTopology();
    UbBufferManager buffers(adapter, contexts);

    std::array<char, 64> source{};
    std::array<char, 64> target{};
    std::array<char, 64> recovered{};
    for (size_t i = 0; i < source.size(); ++i) {
        source[i] = static_cast<char>(i + 1);
    }
    BufferDesc source_desc{};
    source_desc.addr = reinterpret_cast<uint64_t>(source.data());
    source_desc.length = source.size();
    source_desc.location = kWildcardLocation;
    BufferDesc target_desc{};
    target_desc.addr = reinterpret_cast<uint64_t>(target.data());
    target_desc.length = target.size();
    target_desc.location = kWildcardLocation;
    BufferDesc recovered_desc{};
    recovered_desc.addr = reinterpret_cast<uint64_t>(recovered.data());
    recovered_desc.length = recovered.size();
    recovered_desc.location = kWildcardLocation;
    MemoryOptions options;
    options.perm = kGlobalReadWrite;
    ASSERT_TRUE(buffers.addBuffer(source_desc, options).ok());
    ASSERT_TRUE(buffers.addBuffer(target_desc, options).ok());
    ASSERT_TRUE(buffers.addBuffer(recovered_desc, options).ok());

    SegmentManager manager(std::make_unique<NullRegistry>());
    ASSERT_TRUE(
        manager
            .updateLocal([&](SegmentDesc& segment) {
                segment.name = "local";
                segment.type = SegmentType::Memory;
                segment.detail = MemorySegmentDesc{};
                auto& memory = std::get<MemorySegmentDesc>(segment.detail);
                memory.topology = *topology;
                memory.buffers = {source_desc, target_desc, recovered_desc};
                return Status::OK();
            })
            .ok());

    EndpointStore endpoints(adapter, 16, 1);
    RailMonitor rails;
    QuotaManager quota;
    UbParams params;
    params.worker_count = 1;
    params.poller_count = 1;
    params.slice_size = 16;
    params.max_retries = 1;
    params.slice_timeout_ms = 20;

    std::shared_ptr<UbEndpoint> fenced_endpoint;
    EndpointResolver resolver = [&](const EndpointResolveRequest& request,
                                    std::shared_ptr<UbEndpoint>& endpoint) {
        UbEndpointKey key{request.local_context->topologyId(),
                          request.remote_segment_id, request.remote_topology_id,
                          "local@ub:fake0:eid0"};
        auto status =
            endpoints.getOrCreate(key, request.local_context, endpoint);
        if (!status.ok()) return status;
        if (!fenced_endpoint) fenced_endpoint = endpoint;
        if (endpoint->ready()) return status;
        UbBootstrapDesc peer;
        peer.local_eid = fakeDevice().eid;
        peer.endpoint_generation = 100;
        peer.jetty_ids = {777};
        return endpoint->bind(peer);
    };
    UbWorkers workers(adapter, contexts, topology, &manager, &buffers, &rails,
                      &quota, params, std::move(resolver),
                      [&](const std::shared_ptr<UbEndpoint>& endpoint) {
                          (void)endpoints.retire(endpoint);
                      });
    ASSERT_TRUE(workers.start().ok());
    adapter->holdNextCompletion();
    adapter->dropQuiesceCompletions();

    Request request{};
    request.opcode = Request::WRITE;
    request.source = source.data();
    request.target_id = LOCAL_SEGMENT_ID;
    request.target_offset = reinterpret_cast<uint64_t>(target.data());
    request.length = source.size();
    auto task = UbTask::create(request);
    for (size_t offset = 0; offset < request.length; offset += 16) {
        ASSERT_NE(task->addSlice(UbSliceSpec{
                      source.data() + offset,
                      reinterpret_cast<uint64_t>(target.data() + offset), 16,
                      offset, 1}),
                  nullptr);
    }
    ASSERT_TRUE(task->seal());
    ASSERT_TRUE(workers.submit(task).ok());

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (task->transferStatus().s == PENDING &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    // T-12 / G-05: the fence proves resource safety, never that the WRITE
    // did not take effect, so the first task fails closed with the terminal
    // TIMEOUT sidecar instead of replaying the Unknown WRITE.
    EXPECT_EQ(task->transferStatus().s, TIMEOUT);
    EXPECT_GE(rails.stats(UbPostPath{0, LOCAL_SEGMENT_ID, 0, 1}).timeouts, 1U);
    EXPECT_GE(adapter->quiesceCalls(), 1U);

    // The fence still reclaimed every owner exactly once: the timeout never
    // releases early, and after responsibility converged the endpoint is
    // healthy again for fresh work (C11: no early release, safe reuse).
    EXPECT_EQ(workers.inflightCount(), 0U);
    EXPECT_EQ(quota.activeReservationCount(), 0U);
    ASSERT_NE(fenced_endpoint, nullptr);
    EXPECT_EQ(fenced_endpoint->outstandingWrs(), 0U);
    EXPECT_EQ(fenced_endpoint->outstandingBytes(), 0U);

    Request followup{};
    followup.opcode = Request::WRITE;
    followup.source = source.data();
    followup.target_id = LOCAL_SEGMENT_ID;
    followup.target_offset = reinterpret_cast<uint64_t>(recovered.data());
    followup.length = source.size();
    auto followup_task = UbTask::create(followup);
    for (size_t offset = 0; offset < followup.length; offset += 16) {
        ASSERT_NE(followup_task->addSlice(UbSliceSpec{
                      source.data() + offset,
                      reinterpret_cast<uint64_t>(recovered.data() + offset), 16,
                      offset, 1}),
                  nullptr);
    }
    ASSERT_TRUE(followup_task->seal());
    ASSERT_TRUE(workers.submit(followup_task).ok());

    const auto followup_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (followup_task->transferStatus().s == PENDING &&
           std::chrono::steady_clock::now() < followup_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(followup_task->transferStatus().s, COMPLETED);
    EXPECT_EQ(followup_task->transferStatus().transferred_bytes, source.size());
    EXPECT_EQ(recovered, source);

    EXPECT_TRUE(workers.stop().ok());
    EXPECT_TRUE(endpoints.clear().ok());
    EXPECT_TRUE(buffers.clear().ok());
    EXPECT_TRUE(context->shutdown().ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbNativeDataPathTest, SingleWrRejectedPostRollsBackInflightExactlyOnce) {
    SingleSliceHarness harness;
    ASSERT_TRUE(harness.start(/*slice_timeout_ms=*/5000));

    // Not one byte of the single WR is accepted, so the worker must unwind the
    // inflight pair it recorded before crossing the adapter boundary.
    harness.adapter->failNextPostWithoutAccept();
    auto task = harness.submit(0);
    ASSERT_NE(task, nullptr)
        << "failed to submit the single-WR task; " << harness.diagnostics();
    ASSERT_TRUE(
        SingleSliceHarness::waitForTerminal(task, std::chrono::seconds(2)))
        << "rejected post never reached a terminal state; "
        << SingleSliceHarness::describeTask(task) << " "
        << harness.diagnostics();
    const auto snapshot = [&] {
        return SingleSliceHarness::describeTask(task) + " " +
               harness.diagnostics();
    };
    EXPECT_EQ(task->transferStatus().s, FAILED) << snapshot();
    EXPECT_EQ(task->transferStatus().transferred_bytes, 0U) << snapshot();

    EXPECT_EQ(harness.workers->inflightCount(), 0U) << snapshot();
    EXPECT_EQ(harness.context->outstandingWrs(), 0U) << snapshot();
    EXPECT_EQ(harness.context->inflightBytes(), 0U) << snapshot();
    EXPECT_EQ(harness.quota.activeReservationCount(), 0U) << snapshot();
    ASSERT_NE(harness.endpoint, nullptr) << snapshot();
    EXPECT_EQ(harness.endpoint->outstandingWrs(), 0U) << snapshot();
    EXPECT_EQ(harness.endpoint->outstandingBytes(), 0U) << snapshot();
}

TEST(UbNativeDataPathTest, SingleWrFlushCompletionReleasesInflightOnce) {
    SingleSliceHarness harness;
    // A long timeout keeps the scan from fencing the WR before stop() does.
    ASSERT_TRUE(harness.start(/*slice_timeout_ms=*/5000));

    harness.adapter->holdNextCompletion();
    auto task = harness.submit(0);
    ASSERT_NE(task, nullptr)
        << "failed to submit the single-WR task; " << harness.diagnostics();
    ASSERT_TRUE(harness.waitForPending(1, std::chrono::seconds(2)))
        << "held WR was never observed at the adapter; "
        << SingleSliceHarness::describeTask(task) << " "
        << harness.diagnostics();
    const auto snapshot = [&] {
        return SingleSliceHarness::describeTask(task) + " " +
               harness.diagnostics();
    };
    EXPECT_EQ(harness.workers->inflightCount(), 1U) << snapshot();
    EXPECT_EQ(harness.context->outstandingWrs(), 1U) << snapshot();
    EXPECT_EQ(harness.context->inflightBytes(), SingleSliceHarness::kSliceBytes)
        << snapshot();

    // stop() fences the endpoint. The flush completion surfaces the held WR
    // exactly once, so shutdown must observe the matching removeInflight.
    ASSERT_TRUE(harness.workers->stop().ok())
        << "stop failed after the flush fence; " << snapshot();
    EXPECT_EQ(harness.workers->inflightCount(), 0U) << snapshot();
    EXPECT_EQ(harness.context->outstandingWrs(), 0U) << snapshot();
    EXPECT_EQ(harness.context->inflightBytes(), 0U) << snapshot();
    EXPECT_EQ(harness.quota.activeReservationCount(), 0U) << snapshot();
    EXPECT_EQ(task->transferStatus().s, CANCELED) << snapshot();

    // A second stop must not double-release the already drained pair.
    EXPECT_TRUE(harness.workers->stop().ok())
        << "second stop failed; " << snapshot();
    EXPECT_EQ(harness.context->outstandingWrs(), 0U) << snapshot();
    EXPECT_EQ(harness.context->inflightBytes(), 0U) << snapshot();
}

TEST(UbNativeDataPathTest, SingleWrTimeoutFenceReclaimsInflightOnce) {
    SingleSliceHarness harness;
    ASSERT_TRUE(harness.start(/*slice_timeout_ms=*/10));
    for (size_t i = 0; i < harness.source.size(); ++i) {
        harness.source[i] = static_cast<char>(i + 1);
    }

    // The provider drops the WR and the flush cannot return it. The successful
    // fence is the only proof that the old DMA is gone. T-12 / G-05: a WRITE
    // whose execution outcome stays Unknown must never be transparently
    // replayed: the fence proves resource safety only (INV13), so the slice
    // takes the terminal TIMEOUT sidecar instead of a retry, and the token is
    // reclaimed exactly once.
    harness.adapter->holdNextCompletion();
    harness.adapter->dropQuiesceCompletions();
    auto task = harness.submit(1);
    ASSERT_NE(task, nullptr)
        << "failed to submit the single-WR task; " << harness.diagnostics();
    ASSERT_TRUE(
        SingleSliceHarness::waitForTerminal(task, std::chrono::seconds(2)))
        << "timeout fence never reached a terminal state; "
        << SingleSliceHarness::describeTask(task) << " "
        << harness.diagnostics();
    const auto snapshot = [&] {
        return SingleSliceHarness::describeTask(task) + " " +
               harness.diagnostics();
    };
    EXPECT_EQ(task->transferStatus().s, TIMEOUT)
        << "an Unknown WRITE must not be replayed after a proven fence"
        << snapshot();
    EXPECT_EQ(task->transferStatus().transferred_bytes, 0U) << snapshot();
    const auto timeout_stats =
        harness.rails.stats(UbPostPath{0, LOCAL_SEGMENT_ID, 0, 1});
    EXPECT_GE(timeout_stats.timeouts, 1U)
        << "timeout was never recorded on the rail; rail_timeouts="
        << timeout_stats.timeouts << " rail_pauses=" << timeout_stats.pauses
        << " " << snapshot();
    EXPECT_GE(harness.adapter->quiesceCalls(), 1U)
        << "provider-lost WR was never fenced; " << snapshot();

    EXPECT_EQ(harness.workers->inflightCount(), 0U) << snapshot();
    EXPECT_EQ(harness.context->outstandingWrs(), 0U) << snapshot();
    EXPECT_EQ(harness.context->inflightBytes(), 0U) << snapshot();
    EXPECT_EQ(harness.quota.activeReservationCount(), 0U) << snapshot();
}

// T-12 / C07: the READ side of the timeout rule. A fence that proves the old
// DMA stopped touching the destination makes a fresh READ attempt safe under
// the unified retry budget, so the same injected loss must end COMPLETED.
TEST(UbNativeDataPathTest, SingleWrReadTimeoutFenceRetriesUnderBudget) {
    SingleSliceHarness harness;
    ASSERT_TRUE(harness.start(/*slice_timeout_ms=*/10));
    for (size_t i = 0; i < harness.target.size(); ++i) {
        harness.target[i] = static_cast<char>(i + 1);
    }

    harness.adapter->holdNextCompletion();
    harness.adapter->dropQuiesceCompletions();
    auto task = harness.submit(1, Request::READ);
    ASSERT_NE(task, nullptr)
        << "failed to submit the single-READ task; " << harness.diagnostics();
    ASSERT_TRUE(
        SingleSliceHarness::waitForTerminal(task, std::chrono::seconds(2)))
        << "timeout retry never reached a terminal state; "
        << SingleSliceHarness::describeTask(task) << " "
        << harness.diagnostics();
    const auto snapshot = [&] {
        return SingleSliceHarness::describeTask(task) + " " +
               harness.diagnostics();
    };
    EXPECT_EQ(task->transferStatus().s, COMPLETED)
        << "a fenced READ must retry under the unified budget" << snapshot();
    EXPECT_EQ(task->transferStatus().transferred_bytes,
              SingleSliceHarness::kSliceBytes)
        << snapshot();
    EXPECT_EQ(harness.source, harness.target)
        << "payload mismatch after the READ timeout retry; " << snapshot();
    EXPECT_GE(harness.adapter->quiesceCalls(), 1U)
        << "provider-lost WR was never fenced; " << snapshot();

    EXPECT_EQ(harness.workers->inflightCount(), 0U) << snapshot();
    EXPECT_EQ(harness.context->outstandingWrs(), 0U) << snapshot();
    EXPECT_EQ(harness.context->inflightBytes(), 0U) << snapshot();
    EXPECT_EQ(harness.quota.activeReservationCount(), 0U) << snapshot();
}

TEST(UbNativeDataPathTest, EndpointStoreNeverReusesRetiredGeneration) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    auto context = std::make_shared<UbContext>(0, fakeDevice(), adapter);
    ASSERT_TRUE(context->initialize(1, JfcOptions{}).ok());
    EndpointStore store(adapter, 2, 1);
    UbEndpointKey key{0, 9, 0, "peer@ub:fake0:eid0"};
    std::array<std::shared_ptr<UbEndpoint>, 8> concurrent;
    std::array<Status, 8> statuses;
    std::vector<std::thread> threads;
    for (size_t i = 0; i < concurrent.size(); ++i) {
        threads.emplace_back([&, i] {
            statuses[i] = store.getOrCreate(key, context, concurrent[i]);
        });
    }
    for (auto& thread : threads) thread.join();
    for (size_t i = 0; i < concurrent.size(); ++i) {
        ASSERT_TRUE(statuses[i].ok());
        EXPECT_EQ(concurrent[i], concurrent[0]);
    }
    auto first = concurrent[0];
    const uint64_t old_generation = first->generation();
    EXPECT_TRUE(store.retire(key, old_generation));
    std::shared_ptr<UbEndpoint> replacement;
    ASSERT_TRUE(store.getOrCreate(key, context, replacement).ok());
    EXPECT_GT(replacement->generation(), old_generation);
    EXPECT_FALSE(store.retire(key, old_generation));
    EXPECT_EQ(store.get(key), replacement);
    EXPECT_TRUE(store.clear().ok());
    EXPECT_TRUE(context->shutdown().ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbNativeDataPathTest,
     EndpointStoreDoesNotReplaceEndpointWhenCleanupCannotComplete) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    auto context = std::make_shared<UbContext>(0, fakeDevice(), adapter);
    ASSERT_TRUE(context->initialize(1, JfcOptions{}).ok());
    EndpointStore store(adapter, 1, 1);
    UbEndpointKey key{0, 9, 0, "peer@ub:fake0:eid0"};
    std::shared_ptr<UbEndpoint> endpoint;
    ASSERT_TRUE(store.getOrCreate(key, context, endpoint).ok());
    UbBootstrapDesc peer;
    peer.local_eid = fakeDevice().eid;
    peer.endpoint_generation = 100;
    peer.jetty_ids = {777};
    ASSERT_TRUE(endpoint->bind(peer).ok());

    adapter->failResets();
    std::vector<Completion> completions;
    EXPECT_FALSE(endpoint->quiesce(20, completions).ok());

    std::shared_ptr<UbEndpoint> replacement;
    const auto status = store.getOrCreate(key, context, replacement);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(replacement, nullptr);
    EXPECT_EQ(store.size(), 0U);

    adapter->allowResets();
    EXPECT_TRUE(store.clear().ok());
    EXPECT_TRUE(context->shutdown().ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbNativeDataPathTest, UbTransportRejectsPooledRmBeforeNativeAllocation) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    auto topology = fakeTopology();
    auto control = std::make_shared<ControlService>("p2p", "", nullptr);
    uint16_t port = 0;
    ASSERT_TRUE(control->start(port).ok());
    const std::string segment_name =
        "127.0.0.1:" + std::to_string(static_cast<unsigned>(port));

    // The pooled profile is structurally incomplete here (no physical_only
    // identities): the config gate must fail closed before any native
    // resource is allocated.
    auto config = std::make_shared<Config>();
    config->set("transports/ub/enable", true);
    config->set("transports/ub/runtime_profile", "rm_pool");
    config->set("transports/ub/trans_mode", "rm");
    config->set("transports/ub/jetty_pool/enable", true);
    config->set("transports/ub/probe/enabled", true);

    UbTransport transport(adapter);
    std::string mutable_segment_name = segment_name;
    const auto status =
        transport.install(mutable_segment_name, control, topology, config);
    EXPECT_TRUE(status.IsInvalidArgument()) << status.ToString();
    EXPECT_EQ(adapter->openContextCalls(), 0U)
        << "pooled RM gate ran after opening a native context";
    EXPECT_EQ(adapter->createJettyCalls(), 0U)
        << "pooled RM gate ran after creating a native Jetty";
}

TEST(UbNativeDataPathTest, UbTransportInstallsTrustedPhysicalDevice) {
    auto device = fakeDevice();
    device.capabilities.transport_modes = 6;
    auto unmapped = fakeSecondaryDevice();
    unmapped.capabilities.transport_modes = 6;
    auto adapter = std::make_shared<FakeUrmaAdapter>(
        std::vector<DeviceInfo>{device, unmapped});
    auto topology = fakeTwoDeviceTopology();
    auto control = std::make_shared<ControlService>("p2p", "", nullptr);
    uint16_t port = 0;
    ASSERT_TRUE(control->start(port).ok());
    const std::string segment_name =
        "127.0.0.1:" + std::to_string(static_cast<unsigned>(port));
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        segment.name = segment_name;
                        segment.rpc_server_addr = segment_name;
                        segment.machine_id = "fake-machine";
                        segment.type = SegmentType::Memory;
                        segment.detail = MemorySegmentDesc{};
                        std::get<MemorySegmentDesc>(segment.detail).topology =
                            *topology;
                        return Status::OK();
                    })
                    .ok());

    auto config = std::make_shared<Config>();
    config->set("transports/ub/enable", true);
    config->set("transports/ub/worker_count", 1);
    config->set("transports/ub/poller_count", 1);
    config->set("transports/ub/jfc_per_context", 1);
    setPhysicalOnlyIdentity(*config, device);

    UbTransport transport(adapter);
    std::string mutable_segment_name = segment_name;
    ASSERT_TRUE(
        transport.install(mutable_segment_name, control, topology, config)
            .ok());
    EXPECT_EQ(adapter->openContextCalls(), 1U);
    EXPECT_TRUE(transport.uninstall().ok());
}

TEST(UbNativeDataPathTest, UbTransportRunsSelfReadWriteOverNativeControlPlane) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    // A serialized discovery snapshot is informational. Current local
    // discovery and runtime path health remain the scheduling authorities.
    auto topology = fakeTopology(false);
    auto control = std::make_shared<ControlService>("p2p", "", nullptr);
    uint16_t port = 0;
    ASSERT_TRUE(control->start(port).ok());
    const std::string segment_name =
        "127.0.0.1:" + std::to_string(static_cast<unsigned>(port));
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        segment.name = segment_name;
                        segment.rpc_server_addr = segment_name;
                        segment.machine_id = "fake-machine";
                        segment.type = SegmentType::Memory;
                        segment.detail = MemorySegmentDesc{};
                        std::get<MemorySegmentDesc>(segment.detail).topology =
                            *topology;
                        return Status::OK();
                    })
                    .ok());

    auto config = std::make_shared<Config>();
    config->set("transports/ub/enable", true);
    config->set("transports/ub/worker_count", 1);
    config->set("transports/ub/poller_count", 1);
    config->set("transports/ub/jfc_per_context", 1);
    config->set("transports/ub/jetty_per_endpoint", 1);
    config->set("transports/ub/max_endpoints", 16);
    config->set("transports/ub/slice_size", 16);
    config->set("transports/ub/max_slices_per_task", 8);
    config->set("transports/ub/max_retries", 1);
    config->set("transports/ub/slice_timeout_ms", 1000);
    config->set("transports/ub/endpoint_cooldown_ms", 100);

    UbTransport transport(adapter);
    std::string mutable_segment_name = segment_name;
    ASSERT_TRUE(
        transport.install(mutable_segment_name, control, topology, config)
            .ok());
    EXPECT_STREQ(transport.getName(), "ub");
    EXPECT_TRUE(transport.supportsCancellation());
    EXPECT_FALSE(transport.supportNotification());
    EXPECT_TRUE(transport.capabilities().dram_to_dram);

    // A structurally valid RM v2 request is accepted by the wire codec but
    // rejected before endpoint/Jetty allocation because this runtime is RC.
    const auto create_before_v2 = adapter->createJettyCalls();
    const auto bind_before_v2 = adapter->bindJettyCalls();
    UbBootstrapDesc v2_response;
    ASSERT_TRUE(ControlClient::bootstrapUb(
                    segment_name, makeRmBootstrapV2Request(), v2_response)
                    .ok());
    EXPECT_EQ(v2_response.protocol_version, 2U);
    EXPECT_NE(v2_response.reply_msg.find("RM bootstrap v2"), std::string::npos);
    EXPECT_EQ(adapter->createJettyCalls(), create_before_v2);
    EXPECT_EQ(adapter->bindJettyCalls(), bind_before_v2);

    std::array<char, 64> source{};
    std::array<char, 64> target{};
    std::array<char, 144> oversized_source{};
    std::array<char, 144> oversized_target{};
    for (size_t i = 0; i < source.size(); ++i) {
        source[i] = static_cast<char>(100 - i);
    }
    const auto expected = source;
    BufferDesc source_desc{};
    source_desc.addr = reinterpret_cast<uint64_t>(source.data());
    source_desc.length = source.size();
    source_desc.location = kWildcardLocation;
    BufferDesc target_desc{};
    target_desc.addr = reinterpret_cast<uint64_t>(target.data());
    target_desc.length = target.size();
    target_desc.location = kWildcardLocation;
    BufferDesc oversized_source_desc{};
    oversized_source_desc.addr =
        reinterpret_cast<uint64_t>(oversized_source.data());
    oversized_source_desc.length = oversized_source.size();
    oversized_source_desc.location = kWildcardLocation;
    BufferDesc oversized_target_desc{};
    oversized_target_desc.addr =
        reinterpret_cast<uint64_t>(oversized_target.data());
    oversized_target_desc.length = oversized_target.size();
    oversized_target_desc.location = kWildcardLocation;
    MemoryOptions options;
    options.perm = kGlobalReadWrite;
    std::vector<BufferDesc> descriptors{
        source_desc, target_desc, oversized_source_desc, oversized_target_desc};
    ASSERT_TRUE(transport.addMemoryBuffer(descriptors, options).ok());
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        std::get<MemorySegmentDesc>(segment.detail).buffers =
                            descriptors;
                        return Status::OK();
                    })
                    .ok());

    Transport::SubBatchRef rejected_batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(rejected_batch, 1).ok());
    Request oversized_request{};
    oversized_request.opcode = Request::WRITE;
    oversized_request.source = oversized_source.data();
    oversized_request.target_id = LOCAL_SEGMENT_ID;
    oversized_request.target_offset =
        reinterpret_cast<uint64_t>(oversized_target.data());
    oversized_request.length = oversized_source.size();
    EXPECT_TRUE(
        transport.submitTransferTasks(rejected_batch, {oversized_request})
            .IsInvalidArgument());
    EXPECT_TRUE(transport.freeSubBatch(rejected_batch).ok());

    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(batch, 2).ok());
    Request request{};
    request.opcode = Request::WRITE;
    request.source = source.data();
    request.target_id = LOCAL_SEGMENT_ID;
    request.target_offset = reinterpret_cast<uint64_t>(target.data());
    request.length = source.size();
    ASSERT_TRUE(transport.submitTransferTasks(batch, {request}).ok());

    TransferStatus transfer{PENDING, 0};
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (transfer.s == PENDING &&
           std::chrono::steady_clock::now() < deadline) {
        ASSERT_TRUE(transport.getTransferStatus(batch, 0, transfer).ok());
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(transfer.s, COMPLETED);
    EXPECT_EQ(transfer.transferred_bytes, source.size());
    EXPECT_EQ(source, target);
    EXPECT_GT(transport.getEstimatedBandwidth(), 0.0);

    source.fill(0);
    Request read_request = request;
    read_request.opcode = Request::READ;
    ASSERT_TRUE(transport.submitTransferTasks(batch, {read_request}).ok());
    transfer = TransferStatus{PENDING, 0};
    const auto read_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (transfer.s == PENDING &&
           std::chrono::steady_clock::now() < read_deadline) {
        ASSERT_TRUE(transport.getTransferStatus(batch, 1, transfer).ok());
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(transfer.s, COMPLETED);
    EXPECT_EQ(transfer.transferred_bytes, source.size());
    EXPECT_EQ(source, expected);

    std::vector<NicLoadStats> nic_stats;
    ASSERT_TRUE(transport.getNicLoadStats(nic_stats).ok());
    ASSERT_EQ(nic_stats.size(), 1U);
    EXPECT_EQ(nic_stats.front().device_name, "ub:fake0:eid0");
    EXPECT_EQ(nic_stats.front().inflight_bytes, 0U);
    EXPECT_GT(nic_stats.front().ewma_bandwidth_bps, 0.0);

    ASSERT_TRUE(transport.freeSubBatch(batch).ok());
    EXPECT_EQ(batch, nullptr);

    // A failed device fence must make uninstall retryable without destroying
    // pollers, registered memory, or the held WR token.
    Transport::SubBatchRef draining_batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(draining_batch, 1).ok());
    adapter->holdNextCompletion();
    ASSERT_TRUE(transport.submitTransferTasks(draining_batch, {request}).ok());
    const auto posted_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(1);
    while (adapter->pendingCount() == 0 &&
           std::chrono::steady_clock::now() < posted_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_EQ(adapter->pendingCount(), 1U);
    adapter->failNextQuiesce();
    EXPECT_FALSE(transport.uninstall().ok());
    EXPECT_EQ(adapter->pendingCount(), 1U);
    adapter->failNextShutdown();
    EXPECT_FALSE(transport.uninstall().ok());
    EXPECT_EQ(adapter->pendingCount(), 0U);
    EXPECT_TRUE(transport.freeSubBatch(draining_batch).ok());
    EXPECT_TRUE(transport.uninstall().ok());
}

// ---------------------------------------------------------------------------
// RM Legacy: mode admission, bootstrap v2 handshake, imported targets and the
// single-WR data plane.
// ---------------------------------------------------------------------------

JettyOptions rmJettyOptions() {
    JettyOptions options;
    options.trans_mode = kUrmaTransportModeRm;
    options.bind_tp_type_mask = kUrmaTpTypeCtpMask;
    return options;
}

DeviceInfo rmCapableDevice() {
    auto device = fakeDevice();
    device.capabilities.transport_modes = 3;  // RC|RM
    device.kind = DeviceKind::PHYSICAL;
    device.capabilities.rm_tp_cap = kUrmaTpTypeCtpMask;
    device.capabilities.priority_tp_type_masks[kUrmaMaxPriority] =
        kUrmaTpTypeCtpMask;
    return device;
}

DeviceInfo rmCapableSecondaryDevice() {
    auto device = fakeSecondaryDevice();
    device.capabilities.transport_modes = 3;  // RC|RM
    device.kind = DeviceKind::PHYSICAL;
    device.capabilities.rm_tp_cap = kUrmaTpTypeCtpMask;
    device.capabilities.priority_tp_type_masks[kUrmaMaxPriority] =
        kUrmaTpTypeCtpMask;
    return device;
}

UbExpectedPeerIdentity expectedForRmPeer(const UbBootstrapDesc& desc) {
    UbExpectedPeerIdentity expected;
    expected.segment_name = desc.segment_name;
    expected.local_nic_path = desc.local_nic_path;
    expected.native_device_name = desc.local_device_name;
    expected.device_id = desc.local_device_id;
    expected.eid_index = desc.local_eid_index;
    expected.eid = desc.local_eid;
    expected.physical_device_identity = desc.rm->physical_device_identity;
    expected.transport_modes = desc.rm->transport_modes;
    return expected;
}

TEST(UbRmLegacyTest, EndpointProducesV2AndImportsPeerTargets) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(rmCapableDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    auto context = std::make_shared<UbContext>(0, rmCapableDevice(), adapter);
    ASSERT_TRUE(context->initialize(1, JfcOptions{}).ok());
    auto endpoint = std::make_shared<UbEndpoint>(
        UbEndpointKey{0, 123, 9, "peer/ub:fake0:eid0"}, context, adapter, 1,
        rmJettyOptions());
    ASSERT_TRUE(endpoint->prepare().ok());
    ASSERT_TRUE(endpoint->rmMode());

    // Producer side: a v2 descriptor with one RM target per local Jetty.
    UbBootstrapDesc local;
    ASSERT_TRUE(endpoint
                    ->makeBootstrapDescV2("seg-a", "seg-a/ub:fake0:eid0",
                                          "seg-b/ub:fake0:eid0", 5, 1, 1, local)
                    .ok());
    ASSERT_TRUE(endpoint
                    ->makeBootstrapDesc("seg-a", "seg-a/ub:fake0:eid0",
                                        "seg-b/ub:fake0:eid0", 5, local)
                    .IsInvalidArgument());
    EXPECT_EQ(local.protocol_version, 2U);
    ASSERT_TRUE(local.rm.has_value());
    EXPECT_TRUE(validateUbBootstrap(local).ok());
    EXPECT_TRUE(gateUbBootstrapForMode(UbBootstrapMode::RM,
                                       UbBootstrapProtocol::kAuto, local)
                    .ok());
    EXPECT_EQ(local.rm->targets.size(), 1U);
    EXPECT_EQ(local.rm->targets[0].id, endpoint->jetty(0)->id());
    EXPECT_EQ(local.rm->targets[0].target_epoch, local.endpoint_generation);
    EXPECT_EQ(local.rm->physical_device_identity, "/fake/fake0#0001");
    EXPECT_EQ(local.rm->context_incarnation, context->incarnation());

    // Consumer side: bindRm imports the peer targets and never binds.
    auto peer = local;
    peer.segment_name = "seg-b";
    peer.local_nic_path = "seg-b/ub:fake0:eid0";
    peer.peer_nic_path = "seg-a/ub:fake0:eid0";
    const auto expected = expectedForRmPeer(peer);
    ASSERT_TRUE(endpoint->bindRm(peer, expected).ok());
    EXPECT_EQ(endpoint->importedTargetCount(), 1U);
    EXPECT_EQ(adapter->bindJettyCalls(), 0U)
        << "RM Legacy must never call bindJetty";
    EXPECT_TRUE(endpoint->rmTarget(0) != nullptr);

    // Idempotent re-bind of the same peer descriptor is accepted and must
    // not import the peer targets a second time.
    EXPECT_TRUE(endpoint->bindRm(peer, expected).ok());
    EXPECT_EQ(adapter->importCalls(), 1U);
    // A different peer generation is not merged into the established
    // incarnation; it is reported as AlreadyExists so the target handler can
    // retire the incumbent and retry once.
    auto other_generation = peer;
    other_generation.endpoint_generation = peer.endpoint_generation + 1;
    EXPECT_TRUE(
        endpoint->bindRm(other_generation, expectedForRmPeer(other_generation))
            .IsAlreadyExists());
    EXPECT_EQ(endpoint->importedTargetCount(), 1U);

    // RC bind is rejected on an RM endpoint.
    UbBootstrapDesc v1_peer;
    v1_peer.protocol_version = 1;
    v1_peer.local_eid = fakeDevice().eid;
    v1_peer.endpoint_generation = 7;
    v1_peer.jetty_ids = {11};
    EXPECT_TRUE(endpoint->bind(v1_peer).IsInvalidArgument());

    EXPECT_TRUE(endpoint->retire().ok());
    EXPECT_EQ(endpoint->state(), UbEndpoint::State::kDestroyed);
    EXPECT_EQ(adapter->unimportCalls(), 1U);
    EXPECT_TRUE(context->shutdown().ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbRmLegacyTest, BindRmRejectsBadDescriptorsAndRollsBackImports) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(rmCapableDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    auto context = std::make_shared<UbContext>(0, rmCapableDevice(), adapter);
    ASSERT_TRUE(context->initialize(1, JfcOptions{}).ok());

    auto make_endpoint = [&] {
        return std::make_shared<UbEndpoint>(
            UbEndpointKey{0, 123, 9, "peer/ub:fake0:eid0"}, context, adapter, 2,
            rmJettyOptions());
    };

    // A v1 descriptor can never be consumed by bindRm.
    auto v1_endpoint = make_endpoint();
    ASSERT_TRUE(v1_endpoint->prepare().ok());
    UbBootstrapDesc v1;
    v1.protocol_version = 1;
    v1.local_eid = fakeDevice().eid;
    v1.endpoint_generation = 7;
    v1.jetty_ids = {1, 2};
    EXPECT_TRUE(
        v1_endpoint->bindRm(v1, UbExpectedPeerIdentity{}).IsInvalidArgument());
    EXPECT_EQ(v1_endpoint->state(), UbEndpoint::State::kFailed);
    EXPECT_EQ(v1_endpoint->importedTargetCount(), 0U);

    // Identity mismatch fails closed before any import.
    auto identity_endpoint = make_endpoint();
    ASSERT_TRUE(identity_endpoint->prepare().ok());
    UbBootstrapDesc local;
    ASSERT_TRUE(identity_endpoint
                    ->makeBootstrapDescV2("seg-a", "seg-a/ub:fake0:eid0",
                                          "seg-b/ub:fake0:eid0", 5, 1, 1, local)
                    .ok());
    auto peer = local;
    peer.segment_name = "seg-b";
    peer.local_nic_path = "seg-b/ub:fake0:eid0";
    peer.peer_nic_path = "seg-a/ub:fake0:eid0";
    auto mismatched = expectedForRmPeer(peer);
    mismatched.eid = "0002:0000:0000:0000:0000:0000:0000:0000";
    EXPECT_TRUE(
        identity_endpoint->bindRm(peer, mismatched).IsInvalidArgument());
    EXPECT_EQ(identity_endpoint->state(), UbEndpoint::State::kFailed);
    EXPECT_EQ(adapter->importCalls(), 0U);

    // Target count must match the local Jetty set.
    auto count_endpoint = make_endpoint();
    ASSERT_TRUE(count_endpoint->prepare().ok());
    auto short_peer = peer;
    short_peer.rm->targets.pop_back();
    EXPECT_TRUE(
        count_endpoint->bindRm(short_peer, expectedForRmPeer(short_peer))
            .IsInvalidArgument());
    EXPECT_EQ(count_endpoint->state(), UbEndpoint::State::kFailed);

    // A failing import rolls back every target imported by this attempt in
    // reverse order and destroys the incarnation.
    auto rollback_endpoint = make_endpoint();
    ASSERT_TRUE(rollback_endpoint->prepare().ok());
    adapter->failNextImports(1);
    EXPECT_TRUE(
        rollback_endpoint->bindRm(peer, expectedForRmPeer(peer)).IsRdmaError());
    EXPECT_EQ(rollback_endpoint->state(), UbEndpoint::State::kFailed);
    EXPECT_EQ(rollback_endpoint->importedTargetCount(), 0U);
    EXPECT_EQ(adapter->importCalls(), 2U);
    EXPECT_EQ(adapter->unimportCalls(), 1U)
        << "the first imported target must be rolled back";

    EXPECT_TRUE(context->shutdown().ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

// A remote RM target is a node-wide resource shared by every endpoint on one
// Context: the registry imports it once and reference-counts each user, so a
// second endpoint incarnation must not create a second native import (design
// §5.1).
TEST(UbRmLegacyTest, SharedPeerTargetIsImportedOncePerContext) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(rmCapableDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    auto context = std::make_shared<UbContext>(0, rmCapableDevice(), adapter);
    ASSERT_TRUE(context->initialize(1, JfcOptions{}).ok());

    auto make_endpoint = [&](uint64_t peer_segment_id) {
        return std::make_shared<UbEndpoint>(
            UbEndpointKey{0, peer_segment_id, 9, "peer/ub:fake0:eid0"}, context,
            adapter, 2, rmJettyOptions());
    };
    auto make_peer = [](const std::shared_ptr<UbEndpoint>& endpoint,
                        UbBootstrapDesc& peer) {
        UbBootstrapDesc local;
        EXPECT_TRUE(endpoint
                        ->makeBootstrapDescV2("seg-a", "seg-a/ub:fake0:eid0",
                                              "seg-b/ub:fake0:eid0", 5, 1, 1,
                                              local)
                        .ok());
        peer = local;
        peer.segment_name = "seg-b";
        peer.local_nic_path = "seg-b/ub:fake0:eid0";
        peer.peer_nic_path = "seg-a/ub:fake0:eid0";
    };

    auto first = make_endpoint(123);
    auto second = make_endpoint(456);
    ASSERT_TRUE(first->prepare().ok());
    ASSERT_TRUE(second->prepare().ok());
    UbBootstrapDesc peer;
    make_peer(first, peer);
    ASSERT_TRUE(first->bindRm(peer, expectedForRmPeer(peer)).ok());
    // Both local endpoints reference the same peer descriptor, so the target
    // identities (and epochs) are identical.
    ASSERT_TRUE(second->bindRm(peer, expectedForRmPeer(peer)).ok());
    // One peer device, two targets: the second endpoint only adds references.
    EXPECT_EQ(adapter->importCalls(), 2U)
        << "a shared peer target must be imported once per Context";
    EXPECT_EQ(adapter->unimportCalls(), 0U);

    // Releasing one endpoint keeps the target alive for the other.
    ASSERT_TRUE(first->retire().ok());
    EXPECT_EQ(adapter->unimportCalls(), 0U);
    ASSERT_TRUE(second->retire().ok());
    EXPECT_EQ(adapter->unimportCalls(), 2U);

    EXPECT_TRUE(context->shutdown().ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbRmLegacyTest, RetireRetainsTargetWhenUnimportFailsAndRetries) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(rmCapableDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    auto context = std::make_shared<UbContext>(0, rmCapableDevice(), adapter);
    ASSERT_TRUE(context->initialize(1, JfcOptions{}).ok());
    auto endpoint = std::make_shared<UbEndpoint>(
        UbEndpointKey{0, 123, 9, "peer/ub:fake0:eid0"}, context, adapter, 2,
        rmJettyOptions());
    ASSERT_TRUE(endpoint->prepare().ok());

    UbBootstrapDesc local;
    ASSERT_TRUE(endpoint
                    ->makeBootstrapDescV2("seg-a", "seg-a/ub:fake0:eid0",
                                          "seg-b/ub:fake0:eid0", 5, 1, 1, local)
                    .ok());
    auto peer = local;
    peer.segment_name = "seg-b";
    peer.local_nic_path = "seg-b/ub:fake0:eid0";
    peer.peer_nic_path = "seg-a/ub:fake0:eid0";
    ASSERT_TRUE(endpoint->bindRm(peer, expectedForRmPeer(peer)).ok());
    EXPECT_EQ(endpoint->importedTargetCount(), 2U);
    EXPECT_EQ(adapter->importCalls(), 2U);

    // The first unimport fails: the handle stays owned by this incarnation
    // and the endpoint never reaches kDestroyed with a live target.
    adapter->failNextUnimports(1);
    EXPECT_TRUE(endpoint->retire().IsRdmaError());
    EXPECT_NE(endpoint->state(), UbEndpoint::State::kDestroyed);
    EXPECT_EQ(endpoint->importedTargetCount(), 0U);
    ASSERT_NE(context->targetRegistry(), nullptr);
    EXPECT_EQ(context->targetRegistry()->retainedCount(), 1U)
        << "the failed unimport must retain its target handle";
    EXPECT_EQ(adapter->unimportCalls(), 2U)
        << "the successful sibling unimport still completes";

    // A retry completes the release: no target survives kDestroyed.
    EXPECT_TRUE(endpoint->retire().ok());
    EXPECT_EQ(endpoint->state(), UbEndpoint::State::kDestroyed);
    EXPECT_EQ(endpoint->importedTargetCount(), 0U);
    EXPECT_EQ(adapter->unimportCalls(), 3U);

    EXPECT_TRUE(context->shutdown().ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

// P1-01: a failed import may still return a partially allocated handle. The
// endpoint must adopt it and roll it back through the audited unimport path
// instead of dropping it via the local shared_ptr destructor.
TEST(UbRmLegacyTest, BindRmAdoptsPartialHandleFromFailedImport) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(rmCapableDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    auto context = std::make_shared<UbContext>(0, rmCapableDevice(), adapter);
    ASSERT_TRUE(context->initialize(1, JfcOptions{}).ok());
    auto endpoint = std::make_shared<UbEndpoint>(
        UbEndpointKey{0, 123, 9, "peer/ub:fake0:eid0"}, context, adapter, 2,
        rmJettyOptions());
    ASSERT_TRUE(endpoint->prepare().ok());

    UbBootstrapDesc local;
    ASSERT_TRUE(endpoint
                    ->makeBootstrapDescV2("seg-a", "seg-a/ub:fake0:eid0",
                                          "seg-b/ub:fake0:eid0", 5, 1, 1, local)
                    .ok());
    auto peer = local;
    peer.segment_name = "seg-b";
    peer.local_nic_path = "seg-b/ub:fake0:eid0";
    peer.peer_nic_path = "seg-a/ub:fake0:eid0";

    // The second import fails but still returns a handle: both the first
    // target and the adopted partial handle must be rolled back.
    adapter->failNextImports(1);
    adapter->setPartialImportOnFailure(true);
    EXPECT_TRUE(endpoint->bindRm(peer, expectedForRmPeer(peer)).IsRdmaError());
    EXPECT_EQ(endpoint->state(), UbEndpoint::State::kFailed);
    EXPECT_EQ(adapter->importCalls(), 2U);
    EXPECT_EQ(adapter->unimportCalls(), 2U)
        << "the adopted partial handle is rolled back, never dropped";
    EXPECT_EQ(endpoint->importedTargetCount(), 0U);

    EXPECT_TRUE(context->shutdown().ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

// P1-01: a rollback unimport failure must leave the handle owned by the
// failed incarnation (retained for retry) and surface the original import
// error, not the cleanup error.
TEST(UbRmLegacyTest, BindRmRollbackRetainsTargetWhenUnimportFails) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(rmCapableDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    auto context = std::make_shared<UbContext>(0, rmCapableDevice(), adapter);
    ASSERT_TRUE(context->initialize(1, JfcOptions{}).ok());
    auto endpoint = std::make_shared<UbEndpoint>(
        UbEndpointKey{0, 123, 9, "peer/ub:fake0:eid0"}, context, adapter, 2,
        rmJettyOptions());
    ASSERT_TRUE(endpoint->prepare().ok());

    UbBootstrapDesc local;
    ASSERT_TRUE(endpoint
                    ->makeBootstrapDescV2("seg-a", "seg-a/ub:fake0:eid0",
                                          "seg-b/ub:fake0:eid0", 5, 1, 1, local)
                    .ok());
    auto peer = local;
    peer.segment_name = "seg-b";
    peer.local_nic_path = "seg-b/ub:fake0:eid0";
    peer.peer_nic_path = "seg-a/ub:fake0:eid0";

    // The second import fails and the rollback unimport of the first target
    // fails too: the retained handle stays owned by this incarnation and the
    // caller still sees the original import error.
    adapter->failNextImports(1);
    adapter->failNextUnimports(1);
    EXPECT_TRUE(endpoint->bindRm(peer, expectedForRmPeer(peer)).IsRdmaError());
    EXPECT_EQ(endpoint->state(), UbEndpoint::State::kFailed);
    EXPECT_EQ(adapter->importCalls(), 2U);
    EXPECT_EQ(adapter->unimportCalls(), 1U);
    EXPECT_EQ(endpoint->importedTargetCount(), 0U);
    ASSERT_NE(context->targetRegistry(), nullptr);
    EXPECT_EQ(context->targetRegistry()->retainedCount(), 1U)
        << "the failed rollback unimport retains its target handle";

    // A retry completes the release: no target survives kDestroyed.
    EXPECT_TRUE(endpoint->retire().ok());
    EXPECT_EQ(endpoint->state(), UbEndpoint::State::kDestroyed);
    EXPECT_EQ(endpoint->importedTargetCount(), 0U);
    EXPECT_EQ(adapter->unimportCalls(), 2U);

    EXPECT_TRUE(context->shutdown().ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

// P1-02: a repeated bootstrap against a READY endpoint is classified with
// the full v2 comparison. Conflict, stale and newer outcomes all preserve
// the incumbent; only an exact duplicate is idempotent.
TEST(UbRmLegacyTest, BindRmClassifiesRepeatedBootstrapAgainstIncumbent) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(rmCapableDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    auto context = std::make_shared<UbContext>(0, rmCapableDevice(), adapter);
    ASSERT_TRUE(context->initialize(1, JfcOptions{}).ok());
    auto endpoint = std::make_shared<UbEndpoint>(
        UbEndpointKey{0, 123, 9, "peer/ub:fake0:eid0"}, context, adapter, 1,
        rmJettyOptions());
    ASSERT_TRUE(endpoint->prepare().ok());

    UbBootstrapDesc local;
    ASSERT_TRUE(endpoint
                    ->makeBootstrapDescV2("seg-a", "seg-a/ub:fake0:eid0",
                                          "seg-b/ub:fake0:eid0", 5, 1, 1, local)
                    .ok());
    auto peer = local;
    peer.segment_name = "seg-b";
    peer.local_nic_path = "seg-b/ub:fake0:eid0";
    peer.peer_nic_path = "seg-a/ub:fake0:eid0";
    ASSERT_TRUE(endpoint->bindRm(peer, expectedForRmPeer(peer)).ok());
    EXPECT_TRUE(endpoint->ready());
    EXPECT_EQ(adapter->importCalls(), 1U);

    // Same generations but different RM content is a conflict.
    auto conflict = peer;
    conflict.rm->targets[0].id = peer.rm->targets[0].id + 100;
    EXPECT_TRUE(endpoint->bindRm(conflict, expectedForRmPeer(conflict))
                    .IsAlreadyExists());
    EXPECT_TRUE(endpoint->ready());
    EXPECT_EQ(endpoint->importedTargetCount(), 1U);

    // An older peer generation is stale and must not overwrite the incumbent.
    auto stale = peer;
    stale.endpoint_generation = peer.endpoint_generation - 1;
    stale.rm->targets[0].target_epoch = stale.endpoint_generation;
    EXPECT_TRUE(
        endpoint->bindRm(stale, expectedForRmPeer(stale)).IsInvalidArgument());
    EXPECT_TRUE(endpoint->ready());
    EXPECT_EQ(endpoint->importedTargetCount(), 1U);

    // A newer peer generation turns over through retire + a fresh
    // incarnation, never an in-place rebind of the READY incumbent.
    auto newer = peer;
    newer.endpoint_generation = peer.endpoint_generation + 1;
    newer.rm->targets[0].target_epoch = newer.endpoint_generation;
    EXPECT_TRUE(
        endpoint->bindRm(newer, expectedForRmPeer(newer)).IsAlreadyExists());
    EXPECT_TRUE(endpoint->ready());
    EXPECT_EQ(endpoint->importedTargetCount(), 1U);

    // Every rejection left the incumbent untouched: still the exact
    // duplicate, still exactly one import, no unimport churn.
    EXPECT_TRUE(endpoint->bindRm(peer, expectedForRmPeer(peer)).ok());
    EXPECT_EQ(adapter->importCalls(), 1U);
    EXPECT_EQ(adapter->unimportCalls(), 0U);

    EXPECT_TRUE(endpoint->retire().ok());
    EXPECT_EQ(adapter->unimportCalls(), 1U);
    EXPECT_TRUE(context->shutdown().ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

// Simple send-Jetty pool, fail-closed after use (C02 / G-01): a retired
// incarnation's drained RESET members have no rearm proof, so release
// deletes them instead of idling them for reuse; the next incarnation
// always creates fresh members. A provider-supported rearm would be the
// only way to reintroduce reuse.
TEST(UbRmLegacyTest, SendJettyPoolRetireDeletesMembersFailClosed) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(rmCapableDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    auto context = std::make_shared<UbContext>(0, rmCapableDevice(), adapter);
    ASSERT_TRUE(context->initialize(1, JfcOptions{}).ok());
    auto pool = std::make_shared<UbSendJettyPool>(
        adapter, context->handle(), context->jfcs(), rmJettyOptions());
    context->attachJettyPool(pool);

    auto make_endpoint = [&] {
        return std::make_shared<UbEndpoint>(
            UbEndpointKey{0, 123, 9, "peer/ub:fake0:eid0"}, context, adapter, 2,
            rmJettyOptions());
    };
    auto make_peer = [](const std::shared_ptr<UbEndpoint>& endpoint,
                        UbBootstrapDesc& peer) {
        UbBootstrapDesc local;
        EXPECT_TRUE(endpoint
                        ->makeBootstrapDescV2("seg-a", "seg-a/ub:fake0:eid0",
                                              "seg-b/ub:fake0:eid0", 5, 1, 1,
                                              local)
                        .ok());
        peer = local;
        peer.segment_name = "seg-b";
        peer.local_nic_path = "seg-b/ub:fake0:eid0";
        peer.peer_nic_path = "seg-a/ub:fake0:eid0";
    };

    // First incarnation leases (creates) two Jettys.
    auto first = make_endpoint();
    ASSERT_TRUE(first->prepare().ok());
    EXPECT_EQ(adapter->createJettyCalls(), 2U);
    UbBootstrapDesc peer;
    make_peer(first, peer);
    ASSERT_TRUE(first->bindRm(peer, expectedForRmPeer(peer)).ok());
    EXPECT_EQ(first->importedTargetCount(), 2U);

    // A clean retire releases the members back to the pool, which deletes
    // them fail-closed (no rearm proof for a RESET member); the imported
    // targets are still unimported.
    ASSERT_TRUE(first->retire().ok());
    EXPECT_EQ(first->state(), UbEndpoint::State::kDestroyed);
    EXPECT_EQ(adapter->deleteJettyCalls(), 2U)
        << "used members are deleted, never idled for reuse (G-01)";
    EXPECT_EQ(pool->idleCount(), 0U);
    EXPECT_EQ(first->importedTargetCount(), 0U);
    EXPECT_EQ(adapter->unimportCalls(), 2U);

    // Second incarnation creates fresh members: no reuse without a
    // provider-supported rearm; the data-plane pairing (2 targets, 2
    // Jettys) still validates.
    auto second = make_endpoint();
    ASSERT_TRUE(second->prepare().ok());
    EXPECT_EQ(adapter->createJettyCalls(), 4U)
        << "the second incarnation must create fresh members";
    EXPECT_EQ(pool->idleCount(), 0U);
    UbBootstrapDesc second_peer;
    make_peer(second, second_peer);
    ASSERT_TRUE(
        second->bindRm(second_peer, expectedForRmPeer(second_peer)).ok());
    EXPECT_EQ(second->importedTargetCount(), 2U);
    ASSERT_TRUE(second->retire().ok());
    EXPECT_EQ(adapter->createJettyCalls(), 4U);
    EXPECT_EQ(adapter->deleteJettyCalls(), 4U);
    EXPECT_EQ(pool->idleCount(), 0U);

    // Context shutdown drains the pool: nothing is left pending.
    EXPECT_TRUE(context->shutdown().ok());
    EXPECT_EQ(adapter->deleteJettyCalls(), 4U);
    EXPECT_TRUE(adapter->shutdown().ok());
}

// Without an attached pool the legacy behavior is unchanged: prepare creates
// the Jetty set and retire deletes it.
TEST(UbRmLegacyTest, SendJettyPoolDisabledCreatesAndDeletes) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(rmCapableDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    auto context = std::make_shared<UbContext>(0, rmCapableDevice(), adapter);
    ASSERT_TRUE(context->initialize(1, JfcOptions{}).ok());
    EXPECT_EQ(context->jettyPool(), nullptr);
    auto endpoint = std::make_shared<UbEndpoint>(
        UbEndpointKey{0, 123, 9, "peer/ub:fake0:eid0"}, context, adapter, 2,
        rmJettyOptions());
    ASSERT_TRUE(endpoint->prepare().ok());
    EXPECT_EQ(adapter->createJettyCalls(), 2U);

    UbBootstrapDesc local;
    ASSERT_TRUE(endpoint
                    ->makeBootstrapDescV2("seg-a", "seg-a/ub:fake0:eid0",
                                          "seg-b/ub:fake0:eid0", 5, 1, 1, local)
                    .ok());
    auto peer = local;
    peer.segment_name = "seg-b";
    peer.local_nic_path = "seg-b/ub:fake0:eid0";
    peer.peer_nic_path = "seg-a/ub:fake0:eid0";
    ASSERT_TRUE(endpoint->bindRm(peer, expectedForRmPeer(peer)).ok());

    ASSERT_TRUE(endpoint->retire().ok());
    EXPECT_EQ(adapter->deleteJettyCalls(), 2U)
        << "without a pool the retire path deletes every Jetty";

    EXPECT_TRUE(context->shutdown().ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

// A Jetty whose RESET failed never enters the pool: the failure path
// deletes it through the audited deleteJetty path instead.
TEST(UbRmLegacyTest, SendJettyPoolExcludesJettysWhoseResetFailed) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(rmCapableDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    auto context = std::make_shared<UbContext>(0, rmCapableDevice(), adapter);
    ASSERT_TRUE(context->initialize(1, JfcOptions{}).ok());
    auto pool = std::make_shared<UbSendJettyPool>(
        adapter, context->handle(), context->jfcs(), rmJettyOptions());
    context->attachJettyPool(pool);

    auto endpoint = std::make_shared<UbEndpoint>(
        UbEndpointKey{0, 123, 9, "peer/ub:fake0:eid0"}, context, adapter, 2,
        rmJettyOptions());
    ASSERT_TRUE(endpoint->prepare().ok());
    EXPECT_EQ(adapter->createJettyCalls(), 2U);

    UbBootstrapDesc local;
    ASSERT_TRUE(endpoint
                    ->makeBootstrapDescV2("seg-a", "seg-a/ub:fake0:eid0",
                                          "seg-b/ub:fake0:eid0", 5, 1, 1, local)
                    .ok());
    auto peer = local;
    peer.segment_name = "seg-b";
    peer.local_nic_path = "seg-b/ub:fake0:eid0";
    peer.peer_nic_path = "seg-a/ub:fake0:eid0";

    // The import fails while resets are injected to fail: failLocked still
    // reaches the delete step, and the unproven Jettys must be deleted, not
    // pooled.
    adapter->failNextImports(1);
    adapter->failResets();
    EXPECT_TRUE(endpoint->bindRm(peer, expectedForRmPeer(peer)).IsRdmaError());
    EXPECT_EQ(endpoint->state(), UbEndpoint::State::kFailed);
    EXPECT_EQ(endpoint->importedTargetCount(), 0U);
    EXPECT_EQ(adapter->deleteJettyCalls(), 2U)
        << "Jettys whose RESET failed must be deleted, never pooled";
    EXPECT_EQ(pool->idleCount(), 0U);
    adapter->allowResets();

    EXPECT_TRUE(context->shutdown().ok());
    EXPECT_EQ(adapter->deleteJettyCalls(), 2U)
        << "an empty pool drains nothing at shutdown";
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbRmLegacyTest, RmJettyBindIsRejectedByTheAdapter) {
    auto adapter = std::make_shared<FakeUrmaAdapter>(fakeDevice());
    ASSERT_TRUE(adapter->initialize().ok());
    auto context = std::make_shared<UbContext>(0, fakeDevice(), adapter);
    ASSERT_TRUE(context->initialize(1, JfcOptions{}).ok());
    JettyPtr jetty;
    JfcPtr jfc;
    ASSERT_TRUE(adapter->createJfc(context->handle(), JfcOptions{}, jfc).ok());
    ASSERT_TRUE(
        adapter->createJetty(context->handle(), jfc, rmJettyOptions(), jetty)
            .ok());
    EXPECT_TRUE(
        adapter->bindJetty(jetty, RemoteJettyInfo{}).IsInvalidArgument());
    EXPECT_TRUE(context->shutdown().ok());
    EXPECT_TRUE(adapter->shutdown().ok());
}

TEST(UbRmLegacyTest, RmInstallGateFailsClosedWithoutRmCapabilities) {
    auto device = fakeDevice();
    // transport_modes without RM or no CTP priority mask must reject install
    // with NotImplemented before any Jetty is created.
    device.capabilities.transport_modes = 1;
    auto adapter = std::make_shared<FakeUrmaAdapter>(device);
    auto topology = fakeTopology();
    auto control = std::make_shared<ControlService>("p2p", "", nullptr);
    uint16_t port = 0;
    ASSERT_TRUE(control->start(port).ok());
    const std::string segment_name =
        "127.0.0.1:" + std::to_string(static_cast<unsigned>(port));

    auto config = std::make_shared<Config>();
    config->set("transports/ub/enable", true);
    config->set("transports/ub/runtime_profile", "rm_legacy");
    config->set("transports/ub/trans_mode", "rm");
    setPhysicalOnlyIdentity(*config, device);

    UbTransport transport(adapter);
    std::string mutable_segment_name = segment_name;
    const auto status =
        transport.install(mutable_segment_name, control, topology, config);
    EXPECT_TRUE(status.IsNotImplemented()) << status.ToString();
    EXPECT_EQ(adapter->createJettyCalls(), 0U)
        << "the capability gate must run before Jetty creation";
}

TEST(UbRmLegacyTest, RmInstallWithOnlyBondingDeviceNamesItInFailure) {
    // An honestly labeled bonding device is excluded by physical-only
    // selection. With no physical device left, install must fail closed and
    // the failure must name the excluded device instead of a bare
    // "no context" message, and no Jetty may be created.
    auto device = rmCapableDevice();
    device.kind = DeviceKind::BONDING;
    auto adapter = std::make_shared<FakeUrmaAdapter>(device);
    auto topology = fakeTopology();
    auto control = std::make_shared<ControlService>("p2p", "", nullptr);
    uint16_t port = 0;
    ASSERT_TRUE(control->start(port).ok());
    const std::string segment_name =
        "127.0.0.1:" + std::to_string(static_cast<unsigned>(port));

    auto config = std::make_shared<Config>();
    config->set("transports/ub/enable", true);
    config->set("transports/ub/runtime_profile", "rm_legacy");
    config->set("transports/ub/trans_mode", "rm");
    setDeviceIdentity(*config, device, "bonding");

    UbTransport transport(adapter);
    std::string mutable_segment_name = segment_name;
    const auto status =
        transport.install(mutable_segment_name, control, topology, config);
    EXPECT_TRUE(status.IsDeviceNotFound()) << status.ToString();
    EXPECT_NE(status.ToString().find("skipped by device selection"),
              std::string::npos)
        << "the failure must name the exclusion reason";
    EXPECT_NE(status.ToString().find(device.native_device_name),
              std::string::npos)
        << "the failure must name the excluded device";
    EXPECT_EQ(adapter->createJettyCalls(), 0U)
        << "no Jetty may be created for a skipped device";
}

TEST(UbRmLegacyTest, TransportRunsSelfReadWriteWithExplicitRmTargets) {
    auto device = rmCapableDevice();
    auto adapter = std::make_shared<FakeUrmaAdapter>(device);
    auto topology = fakeTopology(false);
    auto control = std::make_shared<ControlService>("p2p", "", nullptr);
    uint16_t port = 0;
    ASSERT_TRUE(control->start(port).ok());
    const std::string segment_name =
        "127.0.0.1:" + std::to_string(static_cast<unsigned>(port));
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        segment.name = segment_name;
                        segment.rpc_server_addr = segment_name;
                        segment.machine_id = "fake-machine";
                        segment.type = SegmentType::Memory;
                        segment.detail = MemorySegmentDesc{};
                        std::get<MemorySegmentDesc>(segment.detail).topology =
                            *topology;
                        return Status::OK();
                    })
                    .ok());

    auto config = std::make_shared<Config>();
    config->set("transports/ub/enable", true);
    config->set("transports/ub/runtime_profile", "rm_legacy");
    config->set("transports/ub/trans_mode", "rm");
    config->set("transports/ub/worker_count", 1);
    config->set("transports/ub/poller_count", 1);
    config->set("transports/ub/jfc_per_context", 1);
    config->set("transports/ub/jetty_per_endpoint", 1);
    config->set("transports/ub/max_endpoints", 16);
    config->set("transports/ub/slice_size", 16);
    config->set("transports/ub/max_slices_per_task", 8);
    config->set("transports/ub/max_retries", 1);
    config->set("transports/ub/slice_timeout_ms", 1000);
    config->set("transports/ub/endpoint_cooldown_ms", 100);
    setPhysicalOnlyIdentity(*config, device);

    UbTransport transport(adapter);
    std::string mutable_segment_name = segment_name;
    ASSERT_TRUE(
        transport.install(mutable_segment_name, control, topology, config)
            .ok());
    EXPECT_EQ(adapter->openContextCalls(), 1U);

    // A v1 request must be rejected by the RM runtime gate.
    UbBootstrapDesc v1_request;
    v1_request.protocol_version = 1;
    v1_request.local_eid = device.eid;
    v1_request.endpoint_generation = 9;
    v1_request.jetty_ids = {1};
    UbBootstrapDesc v1_response;
    ASSERT_TRUE(
        ControlClient::bootstrapUb(segment_name, v1_request, v1_response).ok());
    EXPECT_NE(v1_response.reply_msg, "");

    std::array<char, 64> source{};
    std::array<char, 64> target{};
    for (size_t i = 0; i < source.size(); ++i) {
        source[i] = static_cast<char>(100 - i);
    }
    const auto expected = source;
    BufferDesc source_desc{};
    source_desc.addr = reinterpret_cast<uint64_t>(source.data());
    source_desc.length = source.size();
    source_desc.location = kWildcardLocation;
    BufferDesc target_desc{};
    target_desc.addr = reinterpret_cast<uint64_t>(target.data());
    target_desc.length = target.size();
    target_desc.location = kWildcardLocation;
    MemoryOptions options;
    options.perm = kGlobalReadWrite;
    std::vector<BufferDesc> descriptors{source_desc, target_desc};
    ASSERT_TRUE(transport.addMemoryBuffer(descriptors, options).ok());
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        std::get<MemorySegmentDesc>(segment.detail).buffers =
                            descriptors;
                        return Status::OK();
                    })
                    .ok());

    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(batch, 2).ok());
    Request request{};
    request.opcode = Request::WRITE;
    request.source = source.data();
    request.target_id = LOCAL_SEGMENT_ID;
    request.target_offset = reinterpret_cast<uint64_t>(target.data());
    request.length = source.size();
    ASSERT_TRUE(transport.submitTransferTasks(batch, {request}).ok());

    TransferStatus transfer{PENDING, 0};
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (transfer.s == PENDING &&
           std::chrono::steady_clock::now() < deadline) {
        ASSERT_TRUE(transport.getTransferStatus(batch, 0, transfer).ok());
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(transfer.s, COMPLETED);
    EXPECT_EQ(transfer.transferred_bytes, source.size());
    EXPECT_EQ(source, target);
    EXPECT_GE(adapter->rmPostCalls(), 1U)
        << "the RM data plane must post with an explicit target";
    EXPECT_EQ(adapter->bindJettyCalls(), 0U)
        << "RM Legacy posts must not rely on bound remote Jettys";
    ASSERT_FALSE(adapter->importedIds().empty());
    EXPECT_EQ(adapter->lastPostedTargetId(), adapter->importedIds().front());

    source.fill(0);
    Request read_request = request;
    read_request.opcode = Request::READ;
    ASSERT_TRUE(transport.submitTransferTasks(batch, {read_request}).ok());
    transfer = TransferStatus{PENDING, 0};
    const auto read_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (transfer.s == PENDING &&
           std::chrono::steady_clock::now() < read_deadline) {
        ASSERT_TRUE(transport.getTransferStatus(batch, 1, transfer).ok());
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(transfer.s, COMPLETED);
    EXPECT_EQ(transfer.transferred_bytes, source.size());
    EXPECT_EQ(source, expected);

    ASSERT_TRUE(transport.freeSubBatch(batch).ok());
    EXPECT_TRUE(transport.uninstall().ok());
    EXPECT_GE(adapter->unimportCalls(), adapter->importCalls())
        << "every imported target must be unimported before uninstall ends";
}

TEST(UbRmLegacyTest, TransportRunsSelfReadWriteWithPooledJettys) {
    auto device = rmCapableDevice();
    auto adapter = std::make_shared<FakeUrmaAdapter>(device);
    auto topology = fakeTopology(false);
    auto control = std::make_shared<ControlService>("p2p", "", nullptr);
    uint16_t port = 0;
    ASSERT_TRUE(control->start(port).ok());
    const std::string segment_name =
        "127.0.0.1:" + std::to_string(static_cast<unsigned>(port));
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        segment.name = segment_name;
                        segment.rpc_server_addr = segment_name;
                        segment.machine_id = "fake-machine";
                        segment.type = SegmentType::Memory;
                        segment.detail = MemorySegmentDesc{};
                        std::get<MemorySegmentDesc>(segment.detail).topology =
                            *topology;
                        return Status::OK();
                    })
                    .ok());

    auto config = std::make_shared<Config>();
    config->set("transports/ub/enable", true);
    config->set("transports/ub/runtime_profile", "rm_legacy");
    config->set("transports/ub/trans_mode", "rm");
    config->set("transports/ub/worker_count", 1);
    config->set("transports/ub/poller_count", 1);
    config->set("transports/ub/jfc_per_context", 1);
    config->set("transports/ub/jetty_per_endpoint", 2);
    config->set("transports/ub/max_endpoints", 16);
    config->set("transports/ub/slice_size", 16);
    config->set("transports/ub/max_slices_per_task", 8);
    config->set("transports/ub/max_retries", 1);
    config->set("transports/ub/slice_timeout_ms", 1000);
    config->set("transports/ub/endpoint_cooldown_ms", 100);
    config->set("transports/ub/jetty_reuse/enable", true);
    setPhysicalOnlyIdentity(*config, device);

    UbTransport transport(adapter);
    std::string mutable_segment_name = segment_name;
    ASSERT_TRUE(
        transport.install(mutable_segment_name, control, topology, config)
            .ok());

    std::array<char, 64> source{};
    std::array<char, 64> target{};
    for (size_t i = 0; i < source.size(); ++i) {
        source[i] = static_cast<char>(100 - i);
    }
    BufferDesc source_desc{};
    source_desc.addr = reinterpret_cast<uint64_t>(source.data());
    source_desc.length = source.size();
    source_desc.location = kWildcardLocation;
    BufferDesc target_desc{};
    target_desc.addr = reinterpret_cast<uint64_t>(target.data());
    target_desc.length = target.size();
    target_desc.location = kWildcardLocation;
    MemoryOptions options;
    options.perm = kGlobalReadWrite;
    std::vector<BufferDesc> descriptors{source_desc, target_desc};
    ASSERT_TRUE(transport.addMemoryBuffer(descriptors, options).ok());
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        std::get<MemorySegmentDesc>(segment.detail).buffers =
                            descriptors;
                        return Status::OK();
                    })
                    .ok());

    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(batch, 2).ok());
    Request request{};
    request.opcode = Request::WRITE;
    request.source = source.data();
    request.target_id = LOCAL_SEGMENT_ID;
    request.target_offset = reinterpret_cast<uint64_t>(target.data());
    request.length = source.size();
    ASSERT_TRUE(transport.submitTransferTasks(batch, {request}).ok());

    TransferStatus transfer{PENDING, 0};
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (transfer.s == PENDING &&
           std::chrono::steady_clock::now() < deadline) {
        ASSERT_TRUE(transport.getTransferStatus(batch, 0, transfer).ok());
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(transfer.s, COMPLETED);
    EXPECT_EQ(transfer.transferred_bytes, source.size());
    EXPECT_EQ(source, target);
    EXPECT_GE(adapter->rmPostCalls(), 1U)
        << "the RM data plane must post with an explicit target";
    EXPECT_EQ(adapter->bindJettyCalls(), 0U)
        << "RM Legacy posts must not rely on bound remote Jettys";

    // The pool is attached by install: the two Jettys of the single endpoint
    // are created exactly once and never deleted while the endpoint lives.
    EXPECT_EQ(adapter->createJettyCalls(), 2U);
    EXPECT_EQ(adapter->deleteJettyCalls(), 0U);

    ASSERT_TRUE(transport.freeSubBatch(batch).ok());
    EXPECT_TRUE(transport.uninstall().ok());
    // Retire returns the reset Jettys to the pool and context shutdown drains
    // them, so every created Jetty is finally deleted exactly once.
    EXPECT_EQ(adapter->createJettyCalls(), 2U);
    EXPECT_EQ(adapter->deleteJettyCalls(), 2U);
    EXPECT_GE(adapter->unimportCalls(), adapter->importCalls())
        << "every imported target must be unimported before uninstall ends";
}

TEST(UbRmLegacyTest, NotAcceptedPostRollsBackAndRetriesAsNewAttempt) {
    auto device = rmCapableDevice();
    auto adapter = std::make_shared<FakeUrmaAdapter>(device);
    auto topology = fakeTopology(false);
    auto control = std::make_shared<ControlService>("p2p", "", nullptr);
    uint16_t port = 0;
    ASSERT_TRUE(control->start(port).ok());
    const std::string segment_name =
        "127.0.0.1:" + std::to_string(static_cast<unsigned>(port));
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        segment.name = segment_name;
                        segment.rpc_server_addr = segment_name;
                        segment.machine_id = "fake-machine";
                        segment.type = SegmentType::Memory;
                        segment.detail = MemorySegmentDesc{};
                        std::get<MemorySegmentDesc>(segment.detail).topology =
                            *topology;
                        return Status::OK();
                    })
                    .ok());

    auto config = std::make_shared<Config>();
    config->set("transports/ub/enable", true);
    config->set("transports/ub/runtime_profile", "rm_legacy");
    config->set("transports/ub/trans_mode", "rm");
    config->set("transports/ub/worker_count", 1);
    config->set("transports/ub/poller_count", 1);
    config->set("transports/ub/jfc_per_context", 1);
    config->set("transports/ub/jetty_per_endpoint", 1);
    config->set("transports/ub/max_endpoints", 16);
    config->set("transports/ub/slice_size", 16);
    config->set("transports/ub/max_slices_per_task", 8);
    config->set("transports/ub/max_retries", 1);
    config->set("transports/ub/slice_timeout_ms", 1000);
    config->set("transports/ub/endpoint_cooldown_ms", 100);
    setPhysicalOnlyIdentity(*config, device);

    UbTransport transport(adapter);
    std::string mutable_segment_name = segment_name;
    ASSERT_TRUE(
        transport.install(mutable_segment_name, control, topology, config)
            .ok());

    std::array<char, 32> source{};
    std::array<char, 32> target{};
    for (size_t i = 0; i < source.size(); ++i) {
        source[i] = static_cast<char>(i);
    }
    BufferDesc source_desc{};
    source_desc.addr = reinterpret_cast<uint64_t>(source.data());
    source_desc.length = source.size();
    source_desc.location = kWildcardLocation;
    BufferDesc target_desc{};
    target_desc.addr = reinterpret_cast<uint64_t>(target.data());
    target_desc.length = target.size();
    target_desc.location = kWildcardLocation;
    MemoryOptions options;
    options.perm = kGlobalReadWrite;
    std::vector<BufferDesc> descriptors{source_desc, target_desc};
    ASSERT_TRUE(transport.addMemoryBuffer(descriptors, options).ok());
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        std::get<MemorySegmentDesc>(segment.detail).buffers =
                            descriptors;
                        return Status::OK();
                    })
                    .ok());

    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(batch, 1).ok());
    Request request{};
    request.opcode = Request::WRITE;
    request.source = source.data();
    request.target_id = LOCAL_SEGMENT_ID;
    request.target_offset = reinterpret_cast<uint64_t>(target.data());
    request.length = source.size();

    // The first post is deterministically rejected before any WR crosses the
    // native boundary: the pre-registered responsibility is rolled back and a
    // new attempt may be posted.
    adapter->failNextPostWithoutAccept();
    ASSERT_TRUE(transport.submitTransferTasks(batch, {request}).ok());
    TransferStatus transfer{PENDING, 0};
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (transfer.s == PENDING &&
           std::chrono::steady_clock::now() < deadline) {
        ASSERT_TRUE(transport.getTransferStatus(batch, 0, transfer).ok());
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(transfer.s, COMPLETED);
    EXPECT_EQ(source, target);
    EXPECT_GE(adapter->rmPostCalls(), 2U)
        << "the rejected attempt must be retried as a new post";

    ASSERT_TRUE(transport.freeSubBatch(batch).ok());
    EXPECT_TRUE(transport.uninstall().ok());
}

TEST(UbRmLegacyTest, UnknownPostAcceptanceNeverReplaysAndFenceSettles) {
    auto device = rmCapableDevice();
    auto adapter = std::make_shared<FakeUrmaAdapter>(device);
    auto topology = fakeTopology(false);
    auto control = std::make_shared<ControlService>("p2p", "", nullptr);
    uint16_t port = 0;
    ASSERT_TRUE(control->start(port).ok());
    const std::string segment_name =
        "127.0.0.1:" + std::to_string(static_cast<unsigned>(port));
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        segment.name = segment_name;
                        segment.rpc_server_addr = segment_name;
                        segment.machine_id = "fake-machine";
                        segment.type = SegmentType::Memory;
                        segment.detail = MemorySegmentDesc{};
                        std::get<MemorySegmentDesc>(segment.detail).topology =
                            *topology;
                        return Status::OK();
                    })
                    .ok());

    auto config = std::make_shared<Config>();
    config->set("transports/ub/enable", true);
    config->set("transports/ub/runtime_profile", "rm_legacy");
    config->set("transports/ub/trans_mode", "rm");
    config->set("transports/ub/worker_count", 1);
    config->set("transports/ub/poller_count", 1);
    config->set("transports/ub/jfc_per_context", 1);
    config->set("transports/ub/jetty_per_endpoint", 1);
    config->set("transports/ub/max_endpoints", 16);
    config->set("transports/ub/slice_size", 16);
    config->set("transports/ub/max_slices_per_task", 8);
    config->set("transports/ub/max_retries", 0);
    config->set("transports/ub/slice_timeout_ms", 200);
    config->set("transports/ub/endpoint_cooldown_ms", 100);
    setPhysicalOnlyIdentity(*config, device);

    UbTransport transport(adapter);
    std::string mutable_segment_name = segment_name;
    ASSERT_TRUE(
        transport.install(mutable_segment_name, control, topology, config)
            .ok());

    std::array<char, 32> source{};
    std::array<char, 32> target{};
    BufferDesc source_desc{};
    source_desc.addr = reinterpret_cast<uint64_t>(source.data());
    source_desc.length = source.size();
    source_desc.location = kWildcardLocation;
    BufferDesc target_desc{};
    target_desc.addr = reinterpret_cast<uint64_t>(target.data());
    target_desc.length = target.size();
    target_desc.location = kWildcardLocation;
    MemoryOptions options;
    options.perm = kGlobalReadWrite;
    std::vector<BufferDesc> descriptors{source_desc, target_desc};
    ASSERT_TRUE(transport.addMemoryBuffer(descriptors, options).ok());
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        std::get<MemorySegmentDesc>(segment.detail).buffers =
                            descriptors;
                        return Status::OK();
                    })
                    .ok());

    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(batch, 1).ok());
    Request request{};
    request.opcode = Request::WRITE;
    request.source = source.data();
    request.target_id = LOCAL_SEGMENT_ID;
    request.target_offset = reinterpret_cast<uint64_t>(target.data());
    request.length = source.size();

    // Contradictory acceptance: the WR crosses the boundary while the post
    // reports an error. The worker must not roll back or replay; the timeout
    // fence settles the pre-registered owners.
    adapter->failNextPostAfterAccept();
    ASSERT_TRUE(transport.submitTransferTasks(batch, {request}).ok());
    TransferStatus transfer{PENDING, 0};
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (transfer.s == PENDING &&
           std::chrono::steady_clock::now() < deadline) {
        ASSERT_TRUE(transport.getTransferStatus(batch, 0, transfer).ok());
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    EXPECT_NE(transfer.s, PENDING);
    EXPECT_EQ(adapter->rmPostCalls(), 1U)
        << "a WR with unknown acceptance must never be replayed transparently";
    EXPECT_EQ(adapter->pendingCount(), 0U)
        << "the fence must have settled the outstanding WR";

    ASSERT_TRUE(transport.freeSubBatch(batch).ok());
    EXPECT_TRUE(transport.uninstall().ok());
}

// rm_pool full-link wiring: the config gate opens for a well-formed pooled
// profile, install attaches one SendJettyPool per context plus the admission
// ledgers, and the data path leases/returns pooled members end to end
// (C01/C05/C06, INV16 opened for this install generation).
TEST(UbRmPoolTest, RuntimeGateOpensAndStaysFailClosedOnMissingGuards) {
    auto device = rmCapableDevice();
    auto config = std::make_shared<Config>();
    config->set("transports/ub/enable", true);
    config->set("transports/ub/runtime_profile", "rm_pool");
    config->set("transports/ub/trans_mode", "rm");
    config->set("transports/ub/jetty_pool/enable", true);
    config->set("transports/ub/probe/enabled", true);
    setPhysicalOnlyIdentity(*config, device);
    UbParams params;
    ASSERT_TRUE(UbParams::FromConfig(*config, params).ok());
    UbRuntimeMode mode = UbRuntimeMode::kRcLegacy;
    ASSERT_TRUE(resolveUbRuntimeMode(params, mode).ok());
    EXPECT_EQ(mode, UbRuntimeMode::kPooledRm);

    // Without physical_only the pooled profile fails closed: it never falls
    // back to name-based device classification.
    auto loose_config = std::make_shared<Config>();
    loose_config->set("transports/ub/enable", true);
    loose_config->set("transports/ub/runtime_profile", "rm_pool");
    loose_config->set("transports/ub/trans_mode", "rm");
    loose_config->set("transports/ub/jetty_pool/enable", true);
    loose_config->set("transports/ub/probe/enabled", true);
    UbParams loose_params;
    ASSERT_TRUE(UbParams::FromConfig(*loose_config, loose_params).ok());
    EXPECT_TRUE(resolveUbRuntimeMode(loose_params, mode).IsInvalidArgument());

    // force_disable is the runtime brake: it overrides an otherwise
    // well-formed pooled request.
    params.jetty_pool.force_disable = true;
    EXPECT_TRUE(resolveUbRuntimeMode(params, mode).IsInvalidArgument());
}

TEST(UbRmPoolTest, TransportRunsSelfReadWriteThroughPooledSendPool) {
    auto device = rmCapableDevice();
    auto adapter = std::make_shared<FakeUrmaAdapter>(device);
    auto topology = fakeTopology(false);
    auto control = std::make_shared<ControlService>("p2p", "", nullptr);
    uint16_t port = 0;
    ASSERT_TRUE(control->start(port).ok());
    const std::string segment_name =
        "127.0.0.1:" + std::to_string(static_cast<unsigned>(port));
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        segment.name = segment_name;
                        segment.rpc_server_addr = segment_name;
                        segment.machine_id = "fake-machine";
                        segment.type = SegmentType::Memory;
                        segment.detail = MemorySegmentDesc{};
                        std::get<MemorySegmentDesc>(segment.detail).topology =
                            *topology;
                        return Status::OK();
                    })
                    .ok());

    auto config = std::make_shared<Config>();
    config->set("transports/ub/enable", true);
    config->set("transports/ub/runtime_profile", "rm_pool");
    config->set("transports/ub/trans_mode", "rm");
    config->set("transports/ub/worker_count", 1);
    config->set("transports/ub/poller_count", 1);
    config->set("transports/ub/jfc_per_context", 1);
    config->set("transports/ub/jetty_per_endpoint", 1);
    config->set("transports/ub/max_endpoints", 16);
    // One slice per transfer: the transfer below carries 64 bytes. With a
    // 16-byte slice four slices would be posted and "return the member" is
    // asynchronous to "lease the next slice", so two members can legally be in
    // flight at once (desired_serving = 2). That made the strict createJetty
    // reuse assertion depend on poller timing. A single slice keeps the
    // sequence serial, so reuse is deterministic.
    config->set("transports/ub/slice_size", 64);
    config->set("transports/ub/max_slices_per_task", 8);
    config->set("transports/ub/max_retries", 1);
    config->set("transports/ub/slice_timeout_ms", 1000);
    config->set("transports/ub/endpoint_cooldown_ms", 100);
    config->set("transports/ub/jetty_pool/enable", true);
    // Pin the pool ceiling so the test observes strict reuse: one endpoint
    // Jetty plus one pooled member is enough for a serial 3-transfer sequence.
    // desired_serving must be 1: any higher ceiling lets a lease that races
    // the previous return's settlement legally create a deficit member, which
    // would make the createJettyCalls assertions depend on return timing.
    config->set("transports/ub/jetty_pool/business_desired_serving", 1);
    // This test pins exact createJettyCalls counters; keep the pool
    // demand-driven so the background refill cannot top up asynchronously.
    config->set("transports/ub/send_pool/background_refill", false);
    setPhysicalOnlyIdentity(*config, device);

    UbTransport transport(adapter);
    std::string mutable_segment_name = segment_name;
    const auto install_status =
        transport.install(mutable_segment_name, control, topology, config);
    ASSERT_TRUE(install_status.ok()) << install_status.ToString();
    EXPECT_EQ(adapter->openContextCalls(), 1U);

    // Install creates no Jetty: the endpoint set and pooled members are both
    // created lazily on first use.
    EXPECT_EQ(adapter->createJettyCalls(), 0U);

    std::array<char, 64> source{};
    std::array<char, 64> target{};
    for (size_t i = 0; i < source.size(); ++i) {
        source[i] = static_cast<char>(100 - i);
    }
    const auto expected = source;
    BufferDesc source_desc{};
    source_desc.addr = reinterpret_cast<uint64_t>(source.data());
    source_desc.length = source.size();
    source_desc.location = kWildcardLocation;
    BufferDesc target_desc{};
    target_desc.addr = reinterpret_cast<uint64_t>(target.data());
    target_desc.length = target.size();
    target_desc.location = kWildcardLocation;
    MemoryOptions options;
    options.perm = kGlobalReadWrite;
    std::vector<BufferDesc> descriptors{source_desc, target_desc};
    ASSERT_TRUE(transport.addMemoryBuffer(descriptors, options).ok());
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        std::get<MemorySegmentDesc>(segment.detail).buffers =
                            descriptors;
                        return Status::OK();
                    })
                    .ok());

    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(batch, 3).ok());
    Request request{};
    request.opcode = Request::WRITE;
    request.source = source.data();
    request.target_id = LOCAL_SEGMENT_ID;
    request.target_offset = reinterpret_cast<uint64_t>(target.data());
    request.length = source.size();
    ASSERT_TRUE(transport.submitTransferTasks(batch, {request}).ok());

    TransferStatus transfer{PENDING, 0};
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (transfer.s == PENDING &&
           std::chrono::steady_clock::now() < deadline) {
        ASSERT_TRUE(transport.getTransferStatus(batch, 0, transfer).ok());
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(transfer.s, COMPLETED);
    EXPECT_EQ(transfer.transferred_bytes, source.size());
    EXPECT_EQ(source, target);
    EXPECT_GE(adapter->rmPostCalls(), 1U)
        << "the pooled data plane must post with an explicit target";
    EXPECT_EQ(adapter->bindJettyCalls(), 0U)
        << "pooled posts must not rely on bound remote Jettys";
    ASSERT_FALSE(adapter->importedIds().empty());
    EXPECT_EQ(adapter->lastPostedTargetId(), adapter->importedIds().front());
    const uint64_t after_first_write = adapter->createJettyCalls();
    EXPECT_GE(after_first_write, 2U)
        << "the first pooled attempt creates the endpoint Jetty and a pool "
           "member";

    // Second WRITE: the returned member is leased again (LIFO reuse); no new
    // Jetty may be created.
    target.fill(0);
    ASSERT_TRUE(transport.submitTransferTasks(batch, {request}).ok());
    transfer = TransferStatus{PENDING, 0};
    const auto second_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (transfer.s == PENDING &&
           std::chrono::steady_clock::now() < second_deadline) {
        ASSERT_TRUE(transport.getTransferStatus(batch, 1, transfer).ok());
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(transfer.s, COMPLETED);
    EXPECT_EQ(source, target);
    EXPECT_EQ(adapter->createJettyCalls(), after_first_write)
        << "the second attempt must reuse the returned pool member";

    // READ back through the pooled path.
    source.fill(0);
    Request read_request = request;
    read_request.opcode = Request::READ;
    ASSERT_TRUE(transport.submitTransferTasks(batch, {read_request}).ok());
    transfer = TransferStatus{PENDING, 0};
    const auto read_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (transfer.s == PENDING &&
           std::chrono::steady_clock::now() < read_deadline) {
        ASSERT_TRUE(transport.getTransferStatus(batch, 2, transfer).ok());
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(transfer.s, COMPLETED);
    EXPECT_EQ(transfer.transferred_bytes, source.size());
    EXPECT_EQ(source, expected);
    EXPECT_EQ(adapter->createJettyCalls(), after_first_write)
        << "the read must reuse the pooled member too";

    ASSERT_TRUE(transport.freeSubBatch(batch).ok());
    EXPECT_TRUE(transport.uninstall().ok());
    EXPECT_EQ(adapter->deleteJettyCalls(), adapter->createJettyCalls())
        << "every pooled member and endpoint Jetty is deleted on uninstall";
    EXPECT_GE(adapter->unimportCalls(), adapter->importCalls())
        << "every imported target must be unimported before uninstall ends";
}

// T-07 / C04: the pooled send path publishes one path version and charges it
// for the posted WR. The charge is released at the same point as the quota
// release, so after the transfer completes the published path must be back to
// zero in-flight load while the cumulative charge counter has advanced.
TEST(UbRmPoolTest, PooledSendPublishesAndChargesPathVersion) {
    auto device = rmCapableDevice();
    auto adapter = std::make_shared<FakeUrmaAdapter>(device);
    auto topology = fakeTopology(false);
    auto control = std::make_shared<ControlService>("p2p", "", nullptr);
    uint16_t port = 0;
    ASSERT_TRUE(control->start(port).ok());
    const std::string segment_name =
        "127.0.0.1:" + std::to_string(static_cast<unsigned>(port));
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        segment.name = segment_name;
                        segment.rpc_server_addr = segment_name;
                        segment.machine_id = "fake-machine";
                        segment.type = SegmentType::Memory;
                        segment.detail = MemorySegmentDesc{};
                        std::get<MemorySegmentDesc>(segment.detail).topology =
                            *topology;
                        return Status::OK();
                    })
                    .ok());

    auto config = std::make_shared<Config>();
    config->set("transports/ub/enable", true);
    config->set("transports/ub/runtime_profile", "rm_pool");
    config->set("transports/ub/trans_mode", "rm");
    config->set("transports/ub/worker_count", 1);
    config->set("transports/ub/poller_count", 1);
    config->set("transports/ub/jfc_per_context", 1);
    config->set("transports/ub/jetty_per_endpoint", 1);
    config->set("transports/ub/max_endpoints", 16);
    config->set("transports/ub/slice_size", 64);
    config->set("transports/ub/max_slices_per_task", 8);
    config->set("transports/ub/max_retries", 1);
    config->set("transports/ub/slice_timeout_ms", 1000);
    config->set("transports/ub/endpoint_cooldown_ms", 100);
    config->set("transports/ub/jetty_pool/enable", true);
    config->set("transports/ub/jetty_pool/business_desired_serving", 1);
    config->set("transports/ub/send_pool/background_refill", false);
    setPhysicalOnlyIdentity(*config, device);

    UbTransport transport(adapter);
    std::string mutable_segment_name = segment_name;
    const auto install_status =
        transport.install(mutable_segment_name, control, topology, config);
    ASSERT_TRUE(install_status.ok()) << install_status.ToString();

    std::array<char, 64> source{};
    std::array<char, 64> target{};
    for (size_t i = 0; i < source.size(); ++i) {
        source[i] = static_cast<char>(100 - i);
    }
    BufferDesc source_desc{};
    source_desc.addr = reinterpret_cast<uint64_t>(source.data());
    source_desc.length = source.size();
    source_desc.location = kWildcardLocation;
    BufferDesc target_desc{};
    target_desc.addr = reinterpret_cast<uint64_t>(target.data());
    target_desc.length = target.size();
    target_desc.location = kWildcardLocation;
    MemoryOptions options;
    options.perm = kGlobalReadWrite;
    std::vector<BufferDesc> descriptors{source_desc, target_desc};
    ASSERT_TRUE(transport.addMemoryBuffer(descriptors, options).ok());
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        std::get<MemorySegmentDesc>(segment.detail).buffers =
                            descriptors;
                        return Status::OK();
                    })
                    .ok());

    // A fresh install has a registry but nothing published or charged yet.
    const auto before_any_send = transport.ubPathRegistryStats();
    EXPECT_TRUE(before_any_send.entries.empty())
        << "no path may be published before the first resolution";
    EXPECT_EQ(before_any_send.total_charges, 0U);

    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(batch, 1).ok());
    Request request{};
    request.opcode = Request::WRITE;
    request.source = source.data();
    request.target_id = LOCAL_SEGMENT_ID;
    request.target_offset = reinterpret_cast<uint64_t>(target.data());
    request.length = source.size();
    ASSERT_TRUE(transport.submitTransferTasks(batch, {request}).ok());

    TransferStatus transfer{PENDING, 0};
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (transfer.s == PENDING &&
           std::chrono::steady_clock::now() < deadline) {
        ASSERT_TRUE(transport.getTransferStatus(batch, 0, transfer).ok());
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_EQ(transfer.s, COMPLETED);
    ASSERT_EQ(source, target);

    // C03: the first submit missed the endpoint cache, so the handshake must
    // have gone through the async seam instead of blocking the posting thread.
    const auto async = transport.ubAsyncBootstrapStats();
    EXPECT_GE(async.requested, 1U);
    EXPECT_GE(async.enqueued, 1U);
    EXPECT_GE(async.completed, 1U);
    EXPECT_EQ(async.failed, 0U);
    EXPECT_EQ(async.dropped_full, 0U);

    // The settlement and the owner release are asynchronous to COMPLETED, so
    // poll until the charge has drained rather than assuming one ordering.
    ub::UbPathRegistrySnapshot snapshot;
    const auto drain_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    for (;;) {
        snapshot = transport.ubPathRegistryStats();
        bool drained = !snapshot.entries.empty() &&
                       snapshot.total_charges >= 1;
        for (const auto& entry : snapshot.entries) {
            drained = drained && entry.path_inflight_wrs == 0 &&
                      entry.path_inflight_bytes == 0 &&
                      entry.target_inflight_wrs == 0 &&
                      entry.target_inflight_bytes == 0;
        }
        if (drained) break;
        if (std::chrono::steady_clock::now() >= drain_deadline) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_FALSE(snapshot.entries.empty())
        << "the pooled send path must publish a path version";
    EXPECT_GE(snapshot.total_charges, 1U)
        << "every posted pooled WR must take one registry charge";
    EXPECT_EQ(snapshot.total_cold_prepares_enqueued, 0U)
        << "a resolved endpoint is published ready, never cold";
    for (const auto& entry : snapshot.entries) {
        EXPECT_TRUE(entry.target_ready);
        EXPECT_FALSE(entry.retired);
        EXPECT_FALSE(entry.paused);
        EXPECT_NE(entry.version.key.pool.context_incarnation, 0U);
        EXPECT_NE(entry.version.key.peer_incarnation, 0U);
        EXPECT_FALSE(entry.version.key.remote_port_id.empty());
        EXPECT_NE(entry.version.topology_epoch, 0U);
        EXPECT_NE(entry.version.target_epoch, 0U);
        EXPECT_EQ(entry.path_inflight_wrs, 0U)
            << "the charge must be released with the quota release";
        EXPECT_EQ(entry.path_inflight_bytes, 0U);
        EXPECT_EQ(entry.target_inflight_wrs, 0U);
        EXPECT_EQ(entry.target_inflight_bytes, 0U);
    }

    ASSERT_TRUE(transport.freeSubBatch(batch).ok());
    EXPECT_TRUE(transport.uninstall().ok());
}

// T-10 / C02: one pooled attempt is exactly one single-WR post. Every post
// call crossing the adapter boundary carries exactly one WorkRequest (the
// postOne contract), so the call count and the WR count stay equal however
// many attempts (slices, retries, probes) run.
TEST(UbRmPoolTest, PooledAttemptPostsExactlyOneWrPerAttempt) {
    auto device = rmCapableDevice();
    auto adapter = std::make_shared<FakeUrmaAdapter>(device);
    auto topology = fakeTopology(false);
    auto control = std::make_shared<ControlService>("p2p", "", nullptr);
    uint16_t port = 0;
    ASSERT_TRUE(control->start(port).ok());
    const std::string segment_name =
        "127.0.0.1:" + std::to_string(static_cast<unsigned>(port));
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        segment.name = segment_name;
                        segment.rpc_server_addr = segment_name;
                        segment.machine_id = "fake-machine";
                        segment.type = SegmentType::Memory;
                        segment.detail = MemorySegmentDesc{};
                        std::get<MemorySegmentDesc>(segment.detail).topology =
                            *topology;
                        return Status::OK();
                    })
                    .ok());

    auto config = std::make_shared<Config>();
    config->set("transports/ub/enable", true);
    config->set("transports/ub/runtime_profile", "rm_pool");
    config->set("transports/ub/trans_mode", "rm");
    config->set("transports/ub/worker_count", 1);
    config->set("transports/ub/poller_count", 1);
    config->set("transports/ub/jfc_per_context", 1);
    config->set("transports/ub/jetty_per_endpoint", 1);
    config->set("transports/ub/max_endpoints", 16);
    config->set("transports/ub/slice_size", 16);
    config->set("transports/ub/max_slices_per_task", 8);
    config->set("transports/ub/max_retries", 1);
    config->set("transports/ub/slice_timeout_ms", 1000);
    config->set("transports/ub/endpoint_cooldown_ms", 100);
    config->set("transports/ub/jetty_pool/enable", true);
    config->set("transports/ub/send_pool/desired_per_context", 2);
    config->set("transports/ub/send_pool/background_refill", false);
    setPhysicalOnlyIdentity(*config, device);

    UbTransport transport(adapter);
    std::string mutable_segment_name = segment_name;
    ASSERT_TRUE(
        transport.install(mutable_segment_name, control, topology, config)
            .ok());

    std::array<char, 64> source{};
    std::array<char, 64> target{};
    for (size_t i = 0; i < source.size(); ++i) {
        source[i] = static_cast<char>(i);
    }
    BufferDesc source_desc{};
    source_desc.addr = reinterpret_cast<uint64_t>(source.data());
    source_desc.length = source.size();
    source_desc.location = kWildcardLocation;
    BufferDesc target_desc{};
    target_desc.addr = reinterpret_cast<uint64_t>(target.data());
    target_desc.length = target.size();
    target_desc.location = kWildcardLocation;
    MemoryOptions options;
    options.perm = kGlobalReadWrite;
    std::vector<BufferDesc> descriptors{source_desc, target_desc};
    ASSERT_TRUE(transport.addMemoryBuffer(descriptors, options).ok());
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        std::get<MemorySegmentDesc>(segment.detail).buffers =
                            descriptors;
                        return Status::OK();
                    })
                    .ok());

    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(batch, 3).ok());
    Request request{};
    request.opcode = Request::WRITE;
    request.source = source.data();
    request.target_id = LOCAL_SEGMENT_ID;
    request.target_offset = reinterpret_cast<uint64_t>(target.data());
    request.length = source.size();
    ASSERT_TRUE(transport.submitTransferTasks(batch, {request}).ok());

    TransferStatus transfer{PENDING, 0};
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (transfer.s == PENDING &&
           std::chrono::steady_clock::now() < deadline) {
        ASSERT_TRUE(transport.getTransferStatus(batch, 0, transfer).ok());
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_EQ(transfer.s, COMPLETED);
    EXPECT_EQ(source, target);
    // One WR per post call: the 64-byte request is four 16-byte slices, each
    // posted through its own single-WR postOne call.
    EXPECT_GE(adapter->postCalls(), 4U);
    EXPECT_EQ(adapter->postCalls(), adapter->postedWrs())
        << "every post call must carry exactly one WorkRequest";
    EXPECT_GE(adapter->rmPostCalls(), 4U)
        << "each pooled slice posts with an explicit imported target";

    ASSERT_TRUE(transport.freeSubBatch(batch).ok());
    EXPECT_TRUE(transport.uninstall().ok());
}

// T-10 / A01: a definitive zero-acceptance post (the WR never crossed the
// native boundary) unwinds the whole admission chain in reverse order —
// gate token, JFC credit, lease, Task K permit — so the member returns to
// the pool, the permit is free again before the retry requeues, and the
// retry attempt reuses the same member instead of creating a new one.
TEST(UbRmPoolTest, PooledZeroAcceptRollsBackAdmissionAndRetriesSameMember) {
    auto device = rmCapableDevice();
    auto adapter = std::make_shared<FakeUrmaAdapter>(device);
    auto topology = fakeTopology(false);
    auto control = std::make_shared<ControlService>("p2p", "", nullptr);
    uint16_t port = 0;
    ASSERT_TRUE(control->start(port).ok());
    const std::string segment_name =
        "127.0.0.1:" + std::to_string(static_cast<unsigned>(port));
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        segment.name = segment_name;
                        segment.rpc_server_addr = segment_name;
                        segment.machine_id = "fake-machine";
                        segment.type = SegmentType::Memory;
                        segment.detail = MemorySegmentDesc{};
                        std::get<MemorySegmentDesc>(segment.detail).topology =
                            *topology;
                        return Status::OK();
                    })
                    .ok());

    auto config = std::make_shared<Config>();
    config->set("transports/ub/enable", true);
    config->set("transports/ub/runtime_profile", "rm_pool");
    config->set("transports/ub/trans_mode", "rm");
    config->set("transports/ub/worker_count", 1);
    config->set("transports/ub/poller_count", 1);
    config->set("transports/ub/jfc_per_context", 1);
    config->set("transports/ub/jetty_per_endpoint", 1);
    config->set("transports/ub/max_endpoints", 16);
    config->set("transports/ub/slice_size", 16);
    config->set("transports/ub/max_slices_per_task", 8);
    config->set("transports/ub/max_retries", 1);
    config->set("transports/ub/slice_timeout_ms", 1000);
    config->set("transports/ub/endpoint_cooldown_ms", 100);
    config->set("transports/ub/jetty_pool/enable", true);
    config->set("transports/ub/send_pool/desired_per_context", 1);
    config->set("transports/ub/send_pool/background_refill", false);
    setPhysicalOnlyIdentity(*config, device);

    UbTransport transport(adapter);
    std::string mutable_segment_name = segment_name;
    ASSERT_TRUE(
        transport.install(mutable_segment_name, control, topology, config)
            .ok());

    std::array<char, 64> source{};
    std::array<char, 64> target{};
    for (size_t i = 0; i < source.size(); ++i) {
        source[i] = static_cast<char>(200 - i);
    }
    BufferDesc source_desc{};
    source_desc.addr = reinterpret_cast<uint64_t>(source.data());
    source_desc.length = source.size();
    source_desc.location = kWildcardLocation;
    BufferDesc target_desc{};
    target_desc.addr = reinterpret_cast<uint64_t>(target.data());
    target_desc.length = target.size();
    target_desc.location = kWildcardLocation;
    MemoryOptions options;
    options.perm = kGlobalReadWrite;
    std::vector<BufferDesc> descriptors{source_desc, target_desc};
    ASSERT_TRUE(transport.addMemoryBuffer(descriptors, options).ok());
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        std::get<MemorySegmentDesc>(segment.detail).buffers =
                            descriptors;
                        return Status::OK();
                    })
                    .ok());

    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(batch, 3).ok());
    Request request{};
    request.opcode = Request::WRITE;
    request.source = source.data();
    request.target_id = LOCAL_SEGMENT_ID;
    request.target_offset = reinterpret_cast<uint64_t>(target.data());
    request.length = source.size();
    // The first slice attempt is definitively rejected before the WR crosses
    // the native boundary (posted_count stays zero).
    adapter->failNextPostWithoutAccept();
    ASSERT_TRUE(transport.submitTransferTasks(batch, {request}).ok());

    TransferStatus transfer{PENDING, 0};
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (transfer.s == PENDING &&
           std::chrono::steady_clock::now() < deadline) {
        ASSERT_TRUE(transport.getTransferStatus(batch, 0, transfer).ok());
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_EQ(transfer.s, COMPLETED);
    EXPECT_EQ(transfer.transferred_bytes, source.size());
    EXPECT_EQ(source, target);
    // One shared receive target (C04 / C05.1: the endpoint incarnation no
    // longer owns a native Jetty) plus one pool member. The zero-accept
    // rollback returned the member, so the retry (and every later slice)
    // reuses the same pool member instead of creating a new one.
    EXPECT_EQ(adapter->createJettyCalls(), 2U);
    // Four slices: one rejected post, its retry, and three further slices.
    EXPECT_EQ(adapter->postCalls(), 5U);
    EXPECT_EQ(adapter->postCalls(), adapter->postedWrs());
    EXPECT_EQ(adapter->deleteJettyCalls(), 0U)
        << "the shared receive target outlives the endpoint incarnation and "
           "the rolled-back member must not be retired before uninstall";

    ASSERT_TRUE(transport.freeSubBatch(batch).ok());
    EXPECT_TRUE(transport.uninstall().ok());
    EXPECT_EQ(adapter->deleteJettyCalls(), adapter->createJettyCalls());
}

// T-10 / A01: a Task K miss is capacity pressure, never a retry or fault.
// With K=1 and a zero retry budget, the later slices of one task must defer
// while the first slice is in flight (its completion parked so the permit
// stays held) and still complete afterwards; a single deferred attempt that
// consumed retry budget would fail the task permanently.
TEST(UbRmPoolTest, PooledTaskPermitMissDefersWithoutRetryCharge) {
    auto device = rmCapableDevice();
    auto adapter = std::make_shared<FakeUrmaAdapter>(device);
    auto topology = fakeTopology(false);
    auto control = std::make_shared<ControlService>("p2p", "", nullptr);
    uint16_t port = 0;
    ASSERT_TRUE(control->start(port).ok());
    const std::string segment_name =
        "127.0.0.1:" + std::to_string(static_cast<unsigned>(port));
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        segment.name = segment_name;
                        segment.rpc_server_addr = segment_name;
                        segment.machine_id = "fake-machine";
                        segment.type = SegmentType::Memory;
                        segment.detail = MemorySegmentDesc{};
                        std::get<MemorySegmentDesc>(segment.detail).topology =
                            *topology;
                        return Status::OK();
                    })
                    .ok());

    auto config = std::make_shared<Config>();
    config->set("transports/ub/enable", true);
    config->set("transports/ub/runtime_profile", "rm_pool");
    config->set("transports/ub/trans_mode", "rm");
    config->set("transports/ub/worker_count", 1);
    config->set("transports/ub/poller_count", 1);
    config->set("transports/ub/jfc_per_context", 1);
    config->set("transports/ub/jetty_per_endpoint", 1);
    config->set("transports/ub/max_endpoints", 16);
    config->set("transports/ub/slice_size", 16);
    config->set("transports/ub/max_slices_per_task", 8);
    // Zero retry budget: any attempt that charges retry (instead of
    // deferring) fails its slice permanently.
    config->set("transports/ub/max_retries", 0);
    config->set("transports/ub/slice_timeout_ms", 1000);
    config->set("transports/ub/endpoint_cooldown_ms", 100);
    config->set("transports/ub/jetty_pool/enable", true);
    config->set("transports/ub/send_pool/desired_per_context", 2);
    config->set("transports/ub/send_pool/background_refill", false);
    // K=1: one task may hold exactly one inflight attempt at a time.
    config->set("transports/ub/task_max_inflight_attempts", 1);
    setPhysicalOnlyIdentity(*config, device);

    UbTransport transport(adapter);
    std::string mutable_segment_name = segment_name;
    ASSERT_TRUE(
        transport.install(mutable_segment_name, control, topology, config)
            .ok());

    std::array<char, 64> source{};
    std::array<char, 64> target{};
    for (size_t i = 0; i < source.size(); ++i) {
        source[i] = static_cast<char>(64 + i);
    }
    BufferDesc source_desc{};
    source_desc.addr = reinterpret_cast<uint64_t>(source.data());
    source_desc.length = source.size();
    source_desc.location = kWildcardLocation;
    BufferDesc target_desc{};
    target_desc.addr = reinterpret_cast<uint64_t>(target.data());
    target_desc.length = target.size();
    target_desc.location = kWildcardLocation;
    MemoryOptions options;
    options.perm = kGlobalReadWrite;
    std::vector<BufferDesc> descriptors{source_desc, target_desc};
    ASSERT_TRUE(transport.addMemoryBuffer(descriptors, options).ok());
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        std::get<MemorySegmentDesc>(segment.detail).buffers =
                            descriptors;
                        return Status::OK();
                    })
                    .ok());

    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(batch, 3).ok());
    Request request{};
    request.opcode = Request::WRITE;
    request.source = source.data();
    request.target_id = LOCAL_SEGMENT_ID;
    request.target_offset = reinterpret_cast<uint64_t>(target.data());
    request.length = source.size();
    // Park every completion: the first slice stays in flight holding the
    // task's only permit while the worker reaches the later slices.
    adapter->holdAllCompletions();
    ASSERT_TRUE(transport.submitTransferTasks(batch, {request}).ok());

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    TransferStatus transfer{PENDING, 0};
    ASSERT_TRUE(transport.getTransferStatus(batch, 0, transfer).ok());
    ASSERT_EQ(transfer.s, PENDING)
        << "the parked completion must keep the task open";
    // Exactly one slice posted: the K miss deferred every other slice of
    // this task without posting or failing anything.
    EXPECT_EQ(adapter->postCalls(), 1U);
    EXPECT_EQ(adapter->postCalls(), adapter->postedWrs());

    adapter->releaseHeldCompletions();
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (transfer.s == PENDING &&
           std::chrono::steady_clock::now() < deadline) {
        ASSERT_TRUE(transport.getTransferStatus(batch, 0, transfer).ok());
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    // K=1 with a zero retry budget: completion proves every deferred slice
    // was re-admitted after the permit freed instead of consuming retries.
    ASSERT_EQ(transfer.s, COMPLETED);
    EXPECT_EQ(transfer.transferred_bytes, source.size());
    EXPECT_EQ(source, target);
    EXPECT_GE(adapter->postCalls(), 4U);
    EXPECT_EQ(adapter->postCalls(), adapter->postedWrs());

    ASSERT_TRUE(transport.freeSubBatch(batch).ok());
    EXPECT_TRUE(transport.uninstall().ok());
}

// C08 error-retire path: a real error completion (the class native status 9
// belongs to) must move the leased member's Jetty to ERROR, consume its
// flush-done fence and drain the remaining WRs before the member is reset and
// deleted. The member never re-enters circulation: the retry leases a freshly
// created member.
TEST(UbRmPoolTest, PooledMemberRetiresAfterCqeError) {
    auto device = rmCapableDevice();
    auto adapter = std::make_shared<FakeUrmaAdapter>(device);
    auto topology = fakeTopology(false);
    auto control = std::make_shared<ControlService>("p2p", "", nullptr);
    uint16_t port = 0;
    ASSERT_TRUE(control->start(port).ok());
    const std::string segment_name =
        "127.0.0.1:" + std::to_string(static_cast<unsigned>(port));
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        segment.name = segment_name;
                        segment.rpc_server_addr = segment_name;
                        segment.machine_id = "fake-machine";
                        segment.type = SegmentType::Memory;
                        segment.detail = MemorySegmentDesc{};
                        std::get<MemorySegmentDesc>(segment.detail).topology =
                            *topology;
                        return Status::OK();
                    })
                    .ok());

    auto config = std::make_shared<Config>();
    config->set("transports/ub/enable", true);
    config->set("transports/ub/runtime_profile", "rm_pool");
    config->set("transports/ub/trans_mode", "rm");
    config->set("transports/ub/worker_count", 1);
    config->set("transports/ub/poller_count", 1);
    config->set("transports/ub/jfc_per_context", 1);
    config->set("transports/ub/jetty_per_endpoint", 1);
    config->set("transports/ub/max_endpoints", 16);
    config->set("transports/ub/slice_size", 16);
    config->set("transports/ub/max_slices_per_task", 4);
    config->set("transports/ub/max_retries", 1);
    config->set("transports/ub/slice_timeout_ms", 1000);
    config->set("transports/ub/endpoint_cooldown_ms", 100);
    config->set("transports/ub/jetty_pool/enable", true);
    config->set("transports/ub/jetty_pool/business_desired_serving", 1);
    setPhysicalOnlyIdentity(*config, device);

    UbTransport transport(adapter);
    std::string mutable_segment_name = segment_name;
    const auto install_status =
        transport.install(mutable_segment_name, control, topology, config);
    ASSERT_TRUE(install_status.ok()) << install_status.ToString();

    std::array<char, 64> source{};
    std::array<char, 64> target{};
    for (size_t i = 0; i < source.size(); ++i) {
        source[i] = static_cast<char>(100 - i);
    }
    BufferDesc source_desc{};
    source_desc.addr = reinterpret_cast<uint64_t>(source.data());
    source_desc.length = source.size();
    source_desc.location = kWildcardLocation;
    BufferDesc target_desc{};
    target_desc.addr = reinterpret_cast<uint64_t>(target.data());
    target_desc.length = target.size();
    target_desc.location = kWildcardLocation;
    MemoryOptions options;
    options.perm = kGlobalReadWrite;
    std::vector<BufferDesc> descriptors{source_desc, target_desc};
    ASSERT_TRUE(transport.addMemoryBuffer(descriptors, options).ok());
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        std::get<MemorySegmentDesc>(segment.detail).buffers =
                            descriptors;
                        return Status::OK();
                    })
                    .ok());

    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(batch, 1).ok());
    Request request{};
    request.opcode = Request::WRITE;
    request.source = source.data();
    request.target_id = LOCAL_SEGMENT_ID;
    request.target_offset = reinterpret_cast<uint64_t>(target.data());
    request.length = source.size();
    // The first attempt completes with a real error CQE; the retry (one
    // allowed) must run on a replacement member and succeed.
    adapter->failNextCompletion(CompletionCategory::REMOTE_PATH_ERROR);
    ASSERT_TRUE(transport.submitTransferTasks(batch, {request}).ok());

    TransferStatus transfer{PENDING, 0};
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (transfer.s == PENDING &&
           std::chrono::steady_clock::now() < deadline) {
        ASSERT_TRUE(transport.getTransferStatus(batch, 0, transfer).ok());
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(transfer.s, COMPLETED);
    EXPECT_EQ(source, target);

    // Asynchronous drain chain (design §4.4/§5.2): the error completion closes
    // the post gate and hands the member to the control executor, which runs
    // modify ERROR and the software flush. The blocking quiesce must not run on
    // this path any more.
    EXPECT_GE(adapter->modifyErrorCalls(), 1U)
        << "an errored pooled member must be moved to ERROR before it is "
           "retired";
    EXPECT_GE(adapter->flushJettyCalls(), 1U)
        << "the drain chain must flush the software SQ before DELETE_READY";
    EXPECT_GE(adapter->finishDrainCalls(), 1U)
        << "the drain chain must finish (clear marker / release segments)";
    EXPECT_EQ(adapter->quiesceCalls(), 0U)
        << "the pooled error hot path must never block on quiesceJetty";
    // Retirement hands the native reset+delete to the background delete
    // executor, so wait (bounded) instead of sampling synchronously.
    const auto delete_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (adapter->deleteJettyCalls() == 0 &&
           std::chrono::steady_clock::now() < delete_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_GE(adapter->deleteJettyCalls(), 1U)
        << "the errored member must be reset and deleted, never reused";
    EXPECT_GE(adapter->createJettyCalls(), 3U)
        << "the retry must lease a freshly created member after the errored "
           "one was retired";

    ASSERT_TRUE(transport.freeSubBatch(batch).ok());
    EXPECT_TRUE(transport.uninstall().ok());
    EXPECT_EQ(adapter->deleteJettyCalls(), adapter->createJettyCalls());
}

// T-11 / G-04: a success CQE whose byte count contradicts the submitted WR
// length must fail the attempt closed (never settle as a full success) and
// retire the pool member permanently. `delta` is applied to the reported
// length of the next success completion: negative models a short completion,
// positive an overlong one. The fake still performs the data copy, so the
// bytes may have landed: that is exactly why the result must be FAILED
// instead of a fabricated success (no length is ever "normalized" up to the
// requested length without a versioned provider contract).
static void runPooledLengthMismatchCase(int32_t delta) {
    auto device = rmCapableDevice();
    auto adapter = std::make_shared<FakeUrmaAdapter>(device);
    auto topology = fakeTopology(false);
    auto control = std::make_shared<ControlService>("p2p", "", nullptr);
    uint16_t port = 0;
    ASSERT_TRUE(control->start(port).ok());
    const std::string segment_name =
        "127.0.0.1:" + std::to_string(static_cast<unsigned>(port));
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        segment.name = segment_name;
                        segment.rpc_server_addr = segment_name;
                        segment.machine_id = "fake-machine";
                        segment.type = SegmentType::Memory;
                        segment.detail = MemorySegmentDesc{};
                        std::get<MemorySegmentDesc>(segment.detail).topology =
                            *topology;
                        return Status::OK();
                    })
                    .ok());

    auto config = std::make_shared<Config>();
    config->set("transports/ub/enable", true);
    config->set("transports/ub/runtime_profile", "rm_pool");
    config->set("transports/ub/trans_mode", "rm");
    config->set("transports/ub/worker_count", 1);
    config->set("transports/ub/poller_count", 1);
    config->set("transports/ub/jfc_per_context", 1);
    config->set("transports/ub/jetty_per_endpoint", 1);
    config->set("transports/ub/max_endpoints", 16);
    config->set("transports/ub/slice_size", 64);
    config->set("transports/ub/max_slices_per_task", 4);
    config->set("transports/ub/max_retries", 0);
    config->set("transports/ub/slice_timeout_ms", 1000);
    config->set("transports/ub/endpoint_cooldown_ms", 100);
    config->set("transports/ub/jetty_pool/enable", true);
    config->set("transports/ub/jetty_pool/business_desired_serving", 1);
    // The retirement wakes the background refill, which legitimately restores
    // the pool's serving capacity; that is not a retry of the failed attempt.
    // Keep refill off so createJettyCalls measures only the endpoint Jetty and
    // the leased member, as the hot-path invariant below states.
    config->set("transports/ub/send_pool/background_refill", false);
    setPhysicalOnlyIdentity(*config, device);

    UbTransport transport(adapter);
    std::string mutable_segment_name = segment_name;
    const auto install_status =
        transport.install(mutable_segment_name, control, topology, config);
    ASSERT_TRUE(install_status.ok()) << install_status.ToString();

    std::array<char, 64> source{};
    std::array<char, 64> target{};
    for (size_t i = 0; i < source.size(); ++i) {
        source[i] = static_cast<char>(77 - i);
    }
    BufferDesc source_desc{};
    source_desc.addr = reinterpret_cast<uint64_t>(source.data());
    source_desc.length = source.size();
    source_desc.location = kWildcardLocation;
    BufferDesc target_desc{};
    target_desc.addr = reinterpret_cast<uint64_t>(target.data());
    target_desc.length = target.size();
    target_desc.location = kWildcardLocation;
    MemoryOptions options;
    options.perm = kGlobalReadWrite;
    std::vector<BufferDesc> descriptors{source_desc, target_desc};
    ASSERT_TRUE(transport.addMemoryBuffer(descriptors, options).ok());
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        std::get<MemorySegmentDesc>(segment.detail).buffers =
                            descriptors;
                        return Status::OK();
                    })
                    .ok());

    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(batch, 1).ok());
    Request request{};
    request.opcode = Request::WRITE;
    request.source = source.data();
    request.target_id = LOCAL_SEGMENT_ID;
    request.target_offset = reinterpret_cast<uint64_t>(target.data());
    request.length = source.size();
    adapter->reportNextCompletionLengthDelta(delta);
    ASSERT_TRUE(transport.submitTransferTasks(batch, {request}).ok());

    TransferStatus transfer{PENDING, 0};
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (transfer.s == PENDING &&
           std::chrono::steady_clock::now() < deadline) {
        ASSERT_TRUE(transport.getTransferStatus(batch, 0, transfer).ok());
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(transfer.s, FAILED)
        << "a contradicting byte count must never settle as a success";
    EXPECT_EQ(transfer.transferred_bytes, 0U);
    // Exactly one WR for the single slice of the single attempt.
    EXPECT_EQ(adapter->postCalls(), 1U);
    EXPECT_EQ(adapter->postCalls(), adapter->postedWrs());
    // The member is retired (INV07) and eventually reset+deleted; it never
    // returns to the LIFO stack for reuse.
    const auto delete_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (adapter->deleteJettyCalls() == 0 &&
           std::chrono::steady_clock::now() < delete_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_GE(adapter->deleteJettyCalls(), 1U)
        << "the length-mismatch member must be retired and deleted, never "
           "reused";
    // One endpoint Jetty plus one pool member, and no replacement member:
    // the length mismatch is permanent and not retryable, so nothing new is
    // created on this hot path.
    EXPECT_EQ(adapter->createJettyCalls(), 2U);

    ASSERT_TRUE(transport.freeSubBatch(batch).ok());
    EXPECT_TRUE(transport.uninstall().ok());
    EXPECT_EQ(adapter->deleteJettyCalls(), adapter->createJettyCalls());
}

TEST(UbRmPoolTest, PooledShortCompletionFailsClosedAndRetiresMember) {
    runPooledLengthMismatchCase(-8);
}

TEST(UbRmPoolTest, PooledOverlongCompletionFailsClosedAndRetiresMember) {
    runPooledLengthMismatchCase(+8);
}

// T-13 / B-03: a zero reported length carries no versioned meaning under the
// default contract (kZeroIsUnreported): it is never padded up to the request
// and never judged as a mismatch, so the attempt settles on the success
// category alone.
static void runPooledZeroLengthReportCase(bool strict_contract) {
    auto device = rmCapableDevice();
    auto adapter = std::make_shared<FakeUrmaAdapter>(device);
    if (strict_contract) {
        ProviderContract strict;
        strict.provider_id = "fake-urma-strict";
        strict.native_api_version = 1;
        strict.software_flush = true;
        strict.marker_flush = true;
        strict.delete_tristate = true;
        strict.async_drain = true;
        strict.event_barrier = true;
        strict.length_semantics = LengthSemantics::kStrictMatch;
        adapter->injectProviderContract(strict);
    }
    auto topology = fakeTopology(false);
    auto control = std::make_shared<ControlService>("p2p", "", nullptr);
    uint16_t port = 0;
    ASSERT_TRUE(control->start(port).ok());
    const std::string segment_name =
        "127.0.0.1:" + std::to_string(static_cast<unsigned>(port));
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        segment.name = segment_name;
                        segment.rpc_server_addr = segment_name;
                        segment.machine_id = "fake-machine";
                        segment.type = SegmentType::Memory;
                        segment.detail = MemorySegmentDesc{};
                        std::get<MemorySegmentDesc>(segment.detail).topology =
                            *topology;
                        return Status::OK();
                    })
                    .ok());

    auto config = std::make_shared<Config>();
    config->set("transports/ub/enable", true);
    config->set("transports/ub/runtime_profile", "rm_pool");
    config->set("transports/ub/trans_mode", "rm");
    config->set("transports/ub/worker_count", 1);
    config->set("transports/ub/poller_count", 1);
    config->set("transports/ub/jfc_per_context", 1);
    config->set("transports/ub/jetty_per_endpoint", 1);
    config->set("transports/ub/max_endpoints", 16);
    config->set("transports/ub/slice_size", 64);
    config->set("transports/ub/max_slices_per_task", 4);
    config->set("transports/ub/max_retries", 0);
    config->set("transports/ub/slice_timeout_ms", 1000);
    config->set("transports/ub/endpoint_cooldown_ms", 100);
    config->set("transports/ub/jetty_pool/enable", true);
    config->set("transports/ub/jetty_pool/business_desired_serving", 1);
    setPhysicalOnlyIdentity(*config, device);

    UbTransport transport(adapter);
    std::string mutable_segment_name = segment_name;
    const auto install_status =
        transport.install(mutable_segment_name, control, topology, config);
    ASSERT_TRUE(install_status.ok()) << install_status.ToString();

    std::array<char, 64> source{};
    std::array<char, 64> target{};
    for (size_t i = 0; i < source.size(); ++i) {
        source[i] = static_cast<char>(55 - i);
    }
    BufferDesc source_desc{};
    source_desc.addr = reinterpret_cast<uint64_t>(source.data());
    source_desc.length = source.size();
    source_desc.location = kWildcardLocation;
    BufferDesc target_desc{};
    target_desc.addr = reinterpret_cast<uint64_t>(target.data());
    target_desc.length = target.size();
    target_desc.location = kWildcardLocation;
    MemoryOptions options;
    options.perm = kGlobalReadWrite;
    std::vector<BufferDesc> descriptors{source_desc, target_desc};
    ASSERT_TRUE(transport.addMemoryBuffer(descriptors, options).ok());
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        std::get<MemorySegmentDesc>(segment.detail).buffers =
                            descriptors;
                        return Status::OK();
                    })
                    .ok());

    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(batch, 1).ok());
    Request request{};
    request.opcode = Request::WRITE;
    request.source = source.data();
    request.target_id = LOCAL_SEGMENT_ID;
    request.target_offset = reinterpret_cast<uint64_t>(target.data());
    request.length = source.size();
    // A success CQE that reports zero bytes for a 64-byte request.
    adapter->reportNextCompletionLengthDelta(
        -static_cast<int32_t>(source.size()));
    ASSERT_TRUE(transport.submitTransferTasks(batch, {request}).ok());

    TransferStatus transfer{PENDING, 0};
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (transfer.s == PENDING &&
           std::chrono::steady_clock::now() < deadline) {
        ASSERT_TRUE(transport.getTransferStatus(batch, 0, transfer).ok());
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    if (strict_contract) {
        // The versioned contract declares every SUCCESS length exact, so a
        // zero report on a non-empty request is a contradiction: fail closed.
        EXPECT_EQ(transfer.s, FAILED)
            << "kStrictMatch must judge a zero report as a mismatch";
        EXPECT_EQ(transfer.transferred_bytes, 0U);
    } else {
        // B-03: zero stays unreported - no padding, no mismatch verdict.
        EXPECT_EQ(transfer.s, COMPLETED)
            << "kZeroIsUnreported must not judge a zero report";
        EXPECT_EQ(transfer.transferred_bytes, source.size());
        EXPECT_EQ(source, target);
    }
    EXPECT_EQ(adapter->postCalls(), 1U);

    ASSERT_TRUE(transport.freeSubBatch(batch).ok());
    EXPECT_TRUE(transport.uninstall().ok());
    EXPECT_EQ(adapter->deleteJettyCalls(), adapter->createJettyCalls());
}

TEST(UbRmPoolTest, PooledZeroReportedLengthStaysUnjudgedByDefault) {
    runPooledZeroLengthReportCase(/*strict_contract=*/false);
}

TEST(UbRmPoolTest, PooledStrictContractJudgesZeroReportedLength) {
    runPooledZeroLengthReportCase(/*strict_contract=*/true);
}

// T-13 / C08: the production pool gate. The asynchronous drain chain may only
// be armed when the adapter verifies the capabilities the chain depends on;
// an injected contract that strips one capability must fail the installation
// instead of silently falling back to the synchronous quiesce.
TEST(UbRmPoolTest, PooledInstallFailsClosedWithoutVerifiableAsyncDrain) {
    auto device = rmCapableDevice();
    auto adapter = std::make_shared<FakeUrmaAdapter>(device);
    ProviderContract degraded;
    degraded.provider_id = "fake-urma-degraded";
    degraded.native_api_version = 1;
    degraded.software_flush = true;
    degraded.marker_flush = true;
    degraded.delete_tristate = true;
    degraded.async_drain = false;  // the capability the gate requires
    degraded.event_barrier = true;
    adapter->injectProviderContract(degraded);
    // The injector itself must round-trip the versioned statement.
    EXPECT_EQ(adapter->providerContract().provider_id,
              degraded.provider_id);
    EXPECT_FALSE(adapter->providerContract().async_drain);

    auto topology = fakeTopology(false);
    auto control = std::make_shared<ControlService>("p2p", "", nullptr);
    uint16_t port = 0;
    ASSERT_TRUE(control->start(port).ok());
    const std::string segment_name =
        "127.0.0.1:" + std::to_string(static_cast<unsigned>(port));
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        segment.name = segment_name;
                        segment.rpc_server_addr = segment_name;
                        segment.machine_id = "fake-machine";
                        segment.type = SegmentType::Memory;
                        segment.detail = MemorySegmentDesc{};
                        std::get<MemorySegmentDesc>(segment.detail).topology =
                            *topology;
                        return Status::OK();
                    })
                    .ok());

    auto config = std::make_shared<Config>();
    config->set("transports/ub/enable", true);
    config->set("transports/ub/runtime_profile", "rm_pool");
    config->set("transports/ub/trans_mode", "rm");
    config->set("transports/ub/worker_count", 1);
    config->set("transports/ub/poller_count", 1);
    config->set("transports/ub/jfc_per_context", 1);
    config->set("transports/ub/jetty_per_endpoint", 1);
    config->set("transports/ub/max_endpoints", 16);
    config->set("transports/ub/slice_size", 64);
    config->set("transports/ub/max_slices_per_task", 4);
    config->set("transports/ub/max_retries", 0);
    config->set("transports/ub/slice_timeout_ms", 1000);
    config->set("transports/ub/endpoint_cooldown_ms", 100);
    config->set("transports/ub/jetty_pool/enable", true);
    config->set("transports/ub/jetty_pool/business_desired_serving", 1);
    setPhysicalOnlyIdentity(*config, device);

    UbTransport transport(adapter);
    std::string mutable_segment_name = segment_name;
    const auto install_status =
        transport.install(mutable_segment_name, control, topology, config);
    ASSERT_FALSE(install_status.ok())
        << "an unverifiable async drain contract must fail the install";
    EXPECT_NE(install_status.ToString().find(
                  "verifiable asynchronous drain"),
              std::string::npos)
        << install_status.ToString();
}

// T-11 / C07: a post that reports definite zero acceptance contradicted by a
// real success CQE is an isolation case. The WR provably crossed the native
// boundary (the data may even have landed), so neither the clean rollback
// (member recycled as "never posted") nor a fabricated success applies: the
// attempt fails closed and the member is retired permanently.
TEST(UbRmPoolTest, PooledZeroAcceptContradictedByRealCqeRetiresMember) {
    auto device = rmCapableDevice();
    auto adapter = std::make_shared<FakeUrmaAdapter>(device);
    auto topology = fakeTopology(false);
    auto control = std::make_shared<ControlService>("p2p", "", nullptr);
    uint16_t port = 0;
    ASSERT_TRUE(control->start(port).ok());
    const std::string segment_name =
        "127.0.0.1:" + std::to_string(static_cast<unsigned>(port));
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        segment.name = segment_name;
                        segment.rpc_server_addr = segment_name;
                        segment.machine_id = "fake-machine";
                        segment.type = SegmentType::Memory;
                        segment.detail = MemorySegmentDesc{};
                        std::get<MemorySegmentDesc>(segment.detail).topology =
                            *topology;
                        return Status::OK();
                    })
                    .ok());

    auto config = std::make_shared<Config>();
    config->set("transports/ub/enable", true);
    config->set("transports/ub/runtime_profile", "rm_pool");
    config->set("transports/ub/trans_mode", "rm");
    config->set("transports/ub/worker_count", 1);
    config->set("transports/ub/poller_count", 1);
    config->set("transports/ub/jfc_per_context", 1);
    config->set("transports/ub/jetty_per_endpoint", 1);
    config->set("transports/ub/max_endpoints", 16);
    config->set("transports/ub/slice_size", 64);
    config->set("transports/ub/max_slices_per_task", 4);
    config->set("transports/ub/max_retries", 0);
    config->set("transports/ub/slice_timeout_ms", 1000);
    config->set("transports/ub/endpoint_cooldown_ms", 100);
    config->set("transports/ub/jetty_pool/enable", true);
    config->set("transports/ub/jetty_pool/business_desired_serving", 1);
    // The contradicted member's retirement wakes the background refill, which
    // restores serving capacity; that is not a hot-path replacement for the
    // failed attempt. Keep refill off so createJettyCalls stays at endpoint
    // Jetty plus leased member.
    config->set("transports/ub/send_pool/background_refill", false);
    setPhysicalOnlyIdentity(*config, device);

    UbTransport transport(adapter);
    std::string mutable_segment_name = segment_name;
    const auto install_status =
        transport.install(mutable_segment_name, control, topology, config);
    ASSERT_TRUE(install_status.ok()) << install_status.ToString();

    std::array<char, 64> source{};
    std::array<char, 64> target{};
    for (size_t i = 0; i < source.size(); ++i) {
        source[i] = static_cast<char>(33 - i);
    }
    BufferDesc source_desc{};
    source_desc.addr = reinterpret_cast<uint64_t>(source.data());
    source_desc.length = source.size();
    source_desc.location = kWildcardLocation;
    BufferDesc target_desc{};
    target_desc.addr = reinterpret_cast<uint64_t>(target.data());
    target_desc.length = target.size();
    target_desc.location = kWildcardLocation;
    MemoryOptions options;
    options.perm = kGlobalReadWrite;
    std::vector<BufferDesc> descriptors{source_desc, target_desc};
    ASSERT_TRUE(transport.addMemoryBuffer(descriptors, options).ok());
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        std::get<MemorySegmentDesc>(segment.detail).buffers =
                            descriptors;
                        return Status::OK();
                    })
                    .ok());

    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(batch, 1).ok());
    Request request{};
    request.opcode = Request::WRITE;
    request.source = source.data();
    request.target_id = LOCAL_SEGMENT_ID;
    request.target_offset = reinterpret_cast<uint64_t>(target.data());
    request.length = source.size();
    // The post returns a definite zero acceptance (posted_count stays zero),
    // yet the fake executes the WR and delivers a real success CQE.
    adapter->failNextPostWithoutAcceptButComplete();
    ASSERT_TRUE(transport.submitTransferTasks(batch, {request}).ok());

    TransferStatus transfer{PENDING, 0};
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (transfer.s == PENDING &&
           std::chrono::steady_clock::now() < deadline) {
        ASSERT_TRUE(transport.getTransferStatus(batch, 0, transfer).ok());
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    // The CQE proves the WR crossed the native boundary, so the attempt can
    // be neither "definitely not executed" nor completed: fail closed.
    EXPECT_EQ(transfer.s, FAILED);
    EXPECT_EQ(transfer.transferred_bytes, 0U);
    // The fake executed the WRITE: the contradiction is real, which is why
    // transparent replay or a fabricated success are both forbidden.
    EXPECT_EQ(source, target);
    EXPECT_EQ(adapter->postCalls(), 1U);
    EXPECT_EQ(adapter->postCalls(), adapter->postedWrs());
    // Isolation: the member served a real completion, so it is retired and
    // deleted instead of returning to the LIFO stack.
    const auto delete_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (adapter->deleteJettyCalls() == 0 &&
           std::chrono::steady_clock().now() < delete_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_GE(adapter->deleteJettyCalls(), 1U)
        << "the contradicted member must be isolated and deleted, never "
           "recycled";
    EXPECT_EQ(adapter->createJettyCalls(), 2U)
        << "no replacement member: the contradiction is not a retryable "
           "capacity miss";

    ASSERT_TRUE(transport.freeSubBatch(batch).ok());
    EXPECT_TRUE(transport.uninstall().ok());
    EXPECT_EQ(adapter->deleteJettyCalls(), adapter->createJettyCalls());
}

// P2 refill executor: a known PoolKey is topped back up to desired_serving
// while idle, so the next attempt leases a pre-warmed member and the hot path
// performs no provider creation.
TEST(UbRmPoolTest, BackgroundRefillTopsUpKnownPoolKey) {
    auto device = rmCapableDevice();
    auto adapter = std::make_shared<FakeUrmaAdapter>(device);
    auto topology = fakeTopology(false);
    auto control = std::make_shared<ControlService>("p2p", "", nullptr);
    uint16_t port = 0;
    ASSERT_TRUE(control->start(port).ok());
    const std::string segment_name =
        "127.0.0.1:" + std::to_string(static_cast<unsigned>(port));
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        segment.name = segment_name;
                        segment.rpc_server_addr = segment_name;
                        segment.machine_id = "fake-machine";
                        segment.type = SegmentType::Memory;
                        segment.detail = MemorySegmentDesc{};
                        std::get<MemorySegmentDesc>(segment.detail).topology =
                            *topology;
                        return Status::OK();
                    })
                    .ok());

    auto config = std::make_shared<Config>();
    config->set("transports/ub/enable", true);
    config->set("transports/ub/runtime_profile", "rm_pool");
    config->set("transports/ub/trans_mode", "rm");
    config->set("transports/ub/worker_count", 1);
    config->set("transports/ub/poller_count", 1);
    config->set("transports/ub/jfc_per_context", 1);
    config->set("transports/ub/jetty_per_endpoint", 1);
    config->set("transports/ub/max_endpoints", 16);
    config->set("transports/ub/slice_size", 16);
    config->set("transports/ub/max_slices_per_task", 4);
    config->set("transports/ub/max_retries", 1);
    config->set("transports/ub/slice_timeout_ms", 1000);
    config->set("transports/ub/endpoint_cooldown_ms", 100);
    config->set("transports/ub/jetty_pool/enable", true);
    config->set("transports/ub/jetty_pool/business_desired_serving", 2);
    config->set("transports/ub/send_pool/background_refill", true);
    config->set("transports/ub/send_pool/background_delete", true);
    config->set("transports/ub/jetty_pool/refill_interval_ms", 5);
    setPhysicalOnlyIdentity(*config, device);

    UbTransport transport(adapter);
    std::string mutable_segment_name = segment_name;
    const auto install_status =
        transport.install(mutable_segment_name, control, topology, config);
    ASSERT_TRUE(install_status.ok()) << install_status.ToString();

    std::array<char, 64> source{};
    std::array<char, 64> target{};
    for (size_t i = 0; i < source.size(); ++i) {
        source[i] = static_cast<char>(100 - i);
    }
    BufferDesc source_desc{};
    source_desc.addr = reinterpret_cast<uint64_t>(source.data());
    source_desc.length = source.size();
    source_desc.location = kWildcardLocation;
    BufferDesc target_desc{};
    target_desc.addr = reinterpret_cast<uint64_t>(target.data());
    target_desc.length = target.size();
    target_desc.location = kWildcardLocation;
    MemoryOptions options;
    options.perm = kGlobalReadWrite;
    std::vector<BufferDesc> descriptors{source_desc, target_desc};
    ASSERT_TRUE(transport.addMemoryBuffer(descriptors, options).ok());
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        std::get<MemorySegmentDesc>(segment.detail).buffers =
                            descriptors;
                        return Status::OK();
                    })
                    .ok());

    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(batch, 2).ok());
    Request request{};
    request.opcode = Request::WRITE;
    request.source = source.data();
    request.target_id = LOCAL_SEGMENT_ID;
    request.target_offset = reinterpret_cast<uint64_t>(target.data());
    request.length = source.size();
    ASSERT_TRUE(transport.submitTransferTasks(batch, {request}).ok());

    TransferStatus transfer{PENDING, 0};
    const auto first_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (transfer.s == PENDING &&
           std::chrono::steady_clock::now() < first_deadline) {
        ASSERT_TRUE(transport.getTransferStatus(batch, 0, transfer).ok());
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(transfer.s, COMPLETED);
    EXPECT_EQ(source, target);

    // One endpoint Jetty plus desired_serving pooled members. The refill
    // executor reaches the ceiling without a second business attempt.
    const auto refill_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (adapter->createJettyCalls() < 3 &&
           std::chrono::steady_clock::now() < refill_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(adapter->createJettyCalls(), 3U)
        << "refill must top the known PoolKey up to desired_serving while "
           "idle";

    // The next attempt consumes a pre-warmed member: no provider creation.
    target.fill(0);
    ASSERT_TRUE(transport.submitTransferTasks(batch, {request}).ok());
    transfer = TransferStatus{PENDING, 0};
    const auto second_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (transfer.s == PENDING &&
           std::chrono::steady_clock::now() < second_deadline) {
        ASSERT_TRUE(transport.getTransferStatus(batch, 1, transfer).ok());
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(transfer.s, COMPLETED);
    EXPECT_EQ(source, target);
    EXPECT_EQ(adapter->createJettyCalls(), 3U)
        << "the second attempt must lease a pre-warmed member";

    ASSERT_TRUE(transport.freeSubBatch(batch).ok());
    EXPECT_TRUE(transport.uninstall().ok());
    EXPECT_EQ(adapter->deleteJettyCalls(), adapter->createJettyCalls());
}

// C01 / G-03 heterogeneous install: each Context must create its members with
// its OWN device's LocalSendProfile. device0 supports CTP at priority 15;
// device1 only at priority 3 (CTP|RTP). A last-value scalar collapse would
// give both devices the same priority/mask and misconfigure one of them.
TEST(UbRmPoolTest, PerContextProfilesKeepHeterogeneousPriorityAndMask) {
    auto device0 = rmCapableDevice();  // CTP @ priority 15, mask = CTP
    auto device1 = rmCapableSecondaryDevice();
    device1.capabilities.priority_tp_type_masks[kUrmaMaxPriority] = 0;
    device1.capabilities.priority_tp_type_masks[3] =
        kUrmaTpTypeCtpMask | kUrmaTpTypeRtpMask;
    // Pin the path quota to a single WR (depth x jetty_per_endpoint). Slice
    // routing must exercise both Contexts: with the default depth the four
    // slices can all retire sequentially on device0 before the next post, and
    // device1 would never register its PoolKey.
    device0.capabilities.max_jetty_depth = 1;
    device1.capabilities.max_jetty_depth = 1;
    auto adapter = std::make_shared<FakeUrmaAdapter>(
        std::vector<DeviceInfo>{device0, device1});

    auto topology = fakeTwoDeviceTopology();
    auto control = std::make_shared<ControlService>("p2p", "", nullptr);
    uint16_t port = 0;
    ASSERT_TRUE(control->start(port).ok());
    const std::string segment_name =
        "127.0.0.1:" + std::to_string(static_cast<unsigned>(port));
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        segment.name = segment_name;
                        segment.rpc_server_addr = segment_name;
                        segment.machine_id = "fake-machine";
                        segment.type = SegmentType::Memory;
                        segment.detail = MemorySegmentDesc{};
                        std::get<MemorySegmentDesc>(segment.detail).topology =
                            *topology;
                        return Status::OK();
                    })
                    .ok());

    auto config = std::make_shared<Config>();
    config->set("transports/ub/enable", true);
    config->set("transports/ub/runtime_profile", "rm_pool");
    config->set("transports/ub/trans_mode", "rm");
    config->set("transports/ub/worker_count", 1);
    config->set("transports/ub/poller_count", 1);
    config->set("transports/ub/jfc_per_context", 1);
    config->set("transports/ub/jetty_per_endpoint", 1);
    config->set("transports/ub/max_endpoints", 16);
    config->set("transports/ub/slice_size", 16);
    config->set("transports/ub/max_slices_per_task", 4);
    config->set("transports/ub/max_retries", 1);
    config->set("transports/ub/slice_timeout_ms", 1000);
    config->set("transports/ub/endpoint_cooldown_ms", 100);
    config->set("transports/ub/jetty_pool/enable", true);
    config->set("transports/ub/jetty_pool/business_desired_serving", 2);
    config->set("transports/ub/send_pool/background_refill", true);
    config->set("transports/ub/send_pool/background_delete", true);
    config->set("transports/ub/jetty_pool/refill_interval_ms", 5);
    setPhysicalOnlyIdentities(*config, {device0, device1});

    UbTransport transport(adapter);
    std::string mutable_segment_name = segment_name;
    const auto install_status =
        transport.install(mutable_segment_name, control, topology, config);
    ASSERT_TRUE(install_status.ok()) << install_status.ToString();
    // Install stays lazy: no Jetty exists before the first business attempt.
    EXPECT_EQ(adapter->createJettyCalls(), 0U);

    // A 64-byte WRITE splits into four 16-byte slices across the two local
    // ports, so both Contexts resolve endpoints and register their PoolKeys.
    std::array<char, 64> source{};
    std::array<char, 64> target{};
    for (size_t i = 0; i < source.size(); ++i) {
        source[i] = static_cast<char>(100 - i);
    }
    BufferDesc source_desc{};
    source_desc.addr = reinterpret_cast<uint64_t>(source.data());
    source_desc.length = source.size();
    source_desc.location = kWildcardLocation;
    BufferDesc target_desc{};
    target_desc.addr = reinterpret_cast<uint64_t>(target.data());
    target_desc.length = target.size();
    target_desc.location = kWildcardLocation;
    MemoryOptions options;
    options.perm = kGlobalReadWrite;
    std::vector<BufferDesc> descriptors{source_desc, target_desc};
    ASSERT_TRUE(transport.addMemoryBuffer(descriptors, options).ok());
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        std::get<MemorySegmentDesc>(segment.detail).buffers =
                            descriptors;
                        return Status::OK();
                    })
                    .ok());

    Transport::SubBatchRef batch = nullptr;
    ASSERT_TRUE(transport.allocateSubBatch(batch, 1).ok());
    Request request{};
    request.opcode = Request::WRITE;
    request.source = source.data();
    request.target_id = LOCAL_SEGMENT_ID;
    request.target_offset = reinterpret_cast<uint64_t>(target.data());
    request.length = source.size();
    // Freeze completions: the first slice keeps its path quota consumed, so
    // the next slice must take the other Context's path instead of racing a
    // provider that retires CQEs in a handful of microseconds.
    adapter->holdAllCompletions();
    ASSERT_TRUE(transport.submitTransferTasks(batch, {request}).ok());

    // device0 is ranked first, so device1 only registers its PoolKey through
    // the second slice's path. Wait for that key before letting the held
    // completions drain.
    const auto both_contexts_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (adapter->createdJettyOptions("ub:fake1:eid0").size() < 2 &&
           std::chrono::steady_clock::now() < both_contexts_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    adapter->releaseHeldCompletions();

    TransferStatus transfer{PENDING, 0};
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (transfer.s == PENDING &&
           std::chrono::steady_clock::now() < deadline) {
        ASSERT_TRUE(transport.getTransferStatus(batch, 0, transfer).ok());
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(transfer.s, COMPLETED);
    EXPECT_EQ(source, target);

    // Background refill tops each known PoolKey up to desired_serving.
    const auto refill_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while ((adapter->createdJettyOptions("ub:fake0:eid0").size() < 2 ||
            adapter->createdJettyOptions("ub:fake1:eid0").size() < 2) &&
           std::chrono::steady_clock::now() < refill_deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    const auto options0 = adapter->createdJettyOptions("ub:fake0:eid0");
    const auto options1 = adapter->createdJettyOptions("ub:fake1:eid0");
    ASSERT_GE(options0.size(), 2U);
    ASSERT_GE(options1.size(), 2U);
    for (const auto& options : options0) {
        EXPECT_EQ(options.trans_mode, kUrmaTransportModeRm);
        EXPECT_EQ(options.priority, 15U);
        EXPECT_EQ(options.bind_tp_type_mask, kUrmaTpTypeCtpMask);
    }
    for (const auto& options : options1) {
        EXPECT_EQ(options.trans_mode, kUrmaTransportModeRm);
        EXPECT_EQ(options.priority, 3U);
        EXPECT_EQ(options.bind_tp_type_mask,
                  kUrmaTpTypeCtpMask | kUrmaTpTypeRtpMask);
    }
    EXPECT_NE(options0.front().priority, options1.front().priority)
        << "heterogeneous devices must not collapse into one scalar profile";

    ASSERT_TRUE(transport.freeSubBatch(batch).ok());
    EXPECT_TRUE(transport.uninstall().ok());
    EXPECT_EQ(adapter->deleteJettyCalls(), adapter->createJettyCalls());
}

// B-11 / C05.1 node-level receive-target quota: frozen at rm_pool install and
// checked before any native allocation. helper keeps the two quota cases from
// drifting apart.
std::shared_ptr<Config> rmPoolNodeQuotaConfig(
    const std::vector<DeviceInfo>& devices, uint32_t max_target_jetties) {
    auto config = std::make_shared<Config>();
    config->set("transports/ub/enable", true);
    config->set("transports/ub/runtime_profile", "rm_pool");
    config->set("transports/ub/trans_mode", "rm");
    config->set("transports/ub/worker_count", 1);
    config->set("transports/ub/poller_count", 1);
    config->set("transports/ub/jfc_per_context", 1);
    config->set("transports/ub/jetty_per_endpoint", 1);
    config->set("transports/ub/max_endpoints", 16);
    config->set("transports/ub/slice_size", 64);
    config->set("transports/ub/max_slices_per_task", 4);
    config->set("transports/ub/max_retries", 0);
    config->set("transports/ub/slice_timeout_ms", 1000);
    config->set("transports/ub/endpoint_cooldown_ms", 100);
    config->set("transports/ub/jetty_pool/enable", true);
    config->set("transports/ub/jetty_pool/business_desired_serving", 1);
    config->set("transports/ub/max_target_jetties_per_node",
                max_target_jetties);
    setPhysicalOnlyIdentities(*config, devices);
    return config;
}

// The static demand (one receive target per selected port) is rejected before
// a Context or Jetty exists when it exceeds max_target_jetties_per_node.
TEST(UbRmPoolTest, PooledInstallRejectsStaticDemandAboveNodeQuota) {
    NodeTargetQuota::resetRegistryForTest();
    auto device0 = rmCapableDevice();
    auto device1 = rmCapableSecondaryDevice();
    auto adapter = std::make_shared<FakeUrmaAdapter>(
        std::vector<DeviceInfo>{device0, device1});
    auto topology = fakeTwoDeviceTopology();
    auto control = std::make_shared<ControlService>("p2p", "", nullptr);
    uint16_t port = 0;
    ASSERT_TRUE(control->start(port).ok());
    const std::string segment_name =
        "127.0.0.1:" + std::to_string(static_cast<unsigned>(port));
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        segment.name = segment_name;
                        segment.rpc_server_addr = segment_name;
                        segment.machine_id = "fake-machine";
                        segment.type = SegmentType::Memory;
                        segment.detail = MemorySegmentDesc{};
                        std::get<MemorySegmentDesc>(segment.detail).topology =
                            *topology;
                        return Status::OK();
                    })
                    .ok());

    auto config = rmPoolNodeQuotaConfig({device0, device1}, 1);

    UbTransport transport(adapter);
    std::string mutable_segment_name = segment_name;
    const auto install_status =
        transport.install(mutable_segment_name, control, topology, config);
    EXPECT_TRUE(install_status.IsInvalidArgument())
        << "a two-port topology under a one-target quota must fail closed: "
        << install_status.ToString();
    EXPECT_EQ(adapter->openContextCalls(), 0U)
        << "the static quota check must run before native allocation";
    EXPECT_EQ(adapter->createJettyCalls(), 0U);
    NodeTargetQuota::resetRegistryForTest();
}

// The ledger is process-authoritative: a second install that asks for another
// limit fails closed instead of splitting or widening the frozen accounting.
TEST(UbRmPoolTest, PooledInstallRejectsConflictingFrozenNodeQuotaLimit) {
    NodeTargetQuota::resetRegistryForTest();
    std::shared_ptr<NodeTargetQuota> frozen;
    ASSERT_TRUE(NodeTargetQuota::acquire(8, frozen).ok());

    auto device = rmCapableDevice();
    auto adapter = std::make_shared<FakeUrmaAdapter>(device);
    auto topology = fakeTopology(false);
    auto control = std::make_shared<ControlService>("p2p", "", nullptr);
    uint16_t port = 0;
    ASSERT_TRUE(control->start(port).ok());
    const std::string segment_name =
        "127.0.0.1:" + std::to_string(static_cast<unsigned>(port));
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        segment.name = segment_name;
                        segment.rpc_server_addr = segment_name;
                        segment.machine_id = "fake-machine";
                        segment.type = SegmentType::Memory;
                        segment.detail = MemorySegmentDesc{};
                        std::get<MemorySegmentDesc>(segment.detail).topology =
                            *topology;
                        return Status::OK();
                    })
                    .ok());

    auto config = rmPoolNodeQuotaConfig({device}, 4);

    UbTransport transport(adapter);
    std::string mutable_segment_name = segment_name;
    const auto install_status =
        transport.install(mutable_segment_name, control, topology, config);
    EXPECT_TRUE(install_status.IsInvalidArgument())
        << "a different limit must not re-freeze the process ledger: "
        << install_status.ToString();
    EXPECT_EQ(adapter->openContextCalls(), 0U);

    std::shared_ptr<NodeTargetQuota> again;
    ASSERT_TRUE(NodeTargetQuota::acquire(8, again).ok());
    EXPECT_EQ(again->snapshot().limit, 8U)
        << "the first limit stays authoritative";
    NodeTargetQuota::resetRegistryForTest();
}

// C03 async bootstrap seam: request() must be non-blocking, coalesce an
// in-flight key, respect the failure cooldown and refuse a new key once the
// bounded table is full.
TEST(UbColdResolveSchedulerTest, CoalescesInFlightAndHonoursCooldownAndBound) {
    std::shared_ptr<UbThreadedPathControlExecutor> executor;
    ASSERT_TRUE(UbThreadedPathControlExecutor::create(8, executor).ok());

    std::mutex mutex;
    std::condition_variable started_cv;
    std::condition_variable release_cv;
    bool release = false;
    size_t calls = 0;
    // The resolver records the calling thread and blocks until released, which
    // is what proves request() itself never waited.
    std::vector<std::thread::id> resolve_threads;
    EndpointResolveRequest request;
    request.local_context = nullptr;
    request.remote_segment = nullptr;
    request.remote_segment_id = LOCAL_SEGMENT_ID;
    request.remote_topology_id = 0;

    std::shared_ptr<AsyncColdResolveScheduler> scheduler;
    ASSERT_TRUE(AsyncColdResolveScheduler::create(
                    executor,
                    [&](const EndpointResolveRequest&,
                        std::shared_ptr<UbEndpoint>&) {
                        {
                            std::lock_guard<std::mutex> lock(mutex);
                            ++calls;
                            resolve_threads.push_back(
                                std::this_thread::get_id());
                        }
                        started_cv.notify_all();
                        std::unique_lock<std::mutex> lock(mutex);
                        release_cv.wait(lock, [&] { return release; });
                        return Status::TooManyRequests(
                            "injected async resolve failure");
                    },
                    /*max_entries=*/1, /*retry_cooldown_ns=*/50'000'000ULL,
                    scheduler)
                    .ok());

    // Two requests for the same key: the second is coalesced while the first
    // is in flight, and both return before the resolver is released.
    EXPECT_TRUE(scheduler->request(request));
    EXPECT_TRUE(scheduler->request(request));
    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(started_cv.wait_for(lock, std::chrono::seconds(2),
                                        [&] { return calls == 1; }));
        EXPECT_FALSE(resolve_threads.empty());
        EXPECT_NE(resolve_threads.front(), std::this_thread::get_id())
            << "the resolve must run on the control executor thread";
        release = true;
    }
    release_cv.notify_all();

    // Wait for the failed resolve to settle, then verify the cooldown.
    const auto settled = [&] {
        return scheduler->stats().failed == 1;
    };
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::seconds(2);
    while (!settled() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(settled());
    EXPECT_TRUE(scheduler->request(request));
    EXPECT_GE(scheduler->stats().coalesced, 2U)
        << "in-flight and cooling-down duplicates must not re-run the resolve";

    // The entry table is already at its bound (one key), so a different key is
    // refused instead of growing it.
    EndpointResolveRequest other = request;
    other.remote_topology_id = 1;
    EXPECT_FALSE(scheduler->request(other));
    const auto stats = scheduler->stats();
    EXPECT_GE(stats.dropped_full, 1U);
    EXPECT_EQ(stats.enqueued, 1U);
}

}  // namespace
}  // namespace mooncake::tent::ub
