#pragma once

#include <cstdint>
#include <string>

#include "tent/common/status.h"

namespace mooncake::tent::ub {

// T-13 / C02: a version-isolated statement of what the provider build
// actually guarantees. The contract binds the native stack identity
// (UMDK/liburma/ABI) to the capability set the pooled data path depends on.
// Nothing here is inferred: an adapter reports only what it can prove, and
// the production pool gate fails closed when a required capability is not
// verifiable (C08: "缺可验证异步能力生产 pool gate 失败").
//
// Length semantics follow B-03: without a versioned provider statement the
// completion length cannot be interpreted, so zero is *unreported*, never
// padded up to the requested length and never judged as a mismatch.
enum class LengthSemantics : uint8_t {
    // The provider makes no versioned statement about completion lengths.
    // A zero byte count is unreported (not judged); a non-zero count that
    // differs from the request is a hard mismatch (fail closed).
    kZeroIsUnreported = 0,
    // The provider guarantees every SUCCESS completion reports the exact
    // transferred length. Zero on a non-empty request is a mismatch too.
    kStrictMatch = 1,
};

struct ProviderContract {
    // Identity of the native stack this adapter talks to. Version-isolated:
    // a mismatch between the bootstrap-declared capabilities and this
    // contract is a configuration error, not a fallback.
    std::string provider_id;
    uint32_t native_api_version = 0;
    size_t abi_size = 0;

    // ---- capabilities (only what the adapter can prove) -------------------
    // flushJettyOnce: bounded software-SQ extraction that returns real WR
    // completions (WR_UNHANDLED keeps its token) and entity markers.
    bool software_flush = false;
    // classifyEntityMarker: entity markers carry no WR identity (token 0),
    // so they never consume WR credit (URMA_CR_WR_FLUSH_ERR_DONE /
    // URMA_CR_WR_SUSPEND_DONE shapes only).
    bool marker_flush = false;
    // deleteJettyWithOutcome: the three-way delete result (consumed /
    // retained / unknown) required by the deletion accounting (C09).
    bool delete_tristate = false;
    // modifyJettyError + the flush-done marker form the verifiable
    // beginError barrier the asynchronous drain chain relies on.
    bool async_drain = false;
    // The provider signals asynchronous (control) events on the JFC; the
    // dispatcher must route them by entity instead of by WR token.
    bool event_barrier = false;
    // Proven RESET re-arm: a reset Jetty may be reused. Without proof a
    // reset object is retired, never recycled (B-03 / G-01).
    bool reset_rearm_proven = false;

    LengthSemantics length_semantics = LengthSemantics::kZeroIsUnreported;

    [[nodiscard]] bool operator==(const ProviderContract& other) const {
        return provider_id == other.provider_id &&
               native_api_version == other.native_api_version &&
               abi_size == other.abi_size &&
               software_flush == other.software_flush &&
               marker_flush == other.marker_flush &&
               delete_tristate == other.delete_tristate &&
               async_drain == other.async_drain &&
               event_barrier == other.event_barrier &&
               reset_rearm_proven == other.reset_rearm_proven &&
               length_semantics == other.length_semantics;
    }
};

// The default contract for an adapter that reports nothing: every capability
// stays unproven, so the production pool gate refuses the asynchronous mode.
[[nodiscard]] inline ProviderContract legacyUnknownContract() {
    return ProviderContract{};
}

// Production pool gate (C08): the asynchronous drain chain may only be armed
// when the adapter *verifies* the capabilities the chain depends on. A
// missing capability is an installation failure - the pooled profile must
// not fall back to the synchronous quiesce on the hot path silently.
[[nodiscard]] inline Status checkPooledAsyncDrainContract(
    const ProviderContract& contract) {
    if (!contract.async_drain) {
        return Status::InvalidArgument(
            "the adapter does not verify asynchronous drain capability");
    }
    if (!contract.software_flush) {
        return Status::InvalidArgument(
            "the adapter does not verify bounded software flush");
    }
    if (!contract.marker_flush) {
        return Status::InvalidArgument(
            "the adapter does not verify entity marker classification");
    }
    if (!contract.event_barrier) {
        return Status::InvalidArgument(
            "the adapter does not verify the entity event barrier");
    }
    return Status::OK();
}

}  // namespace mooncake::tent::ub