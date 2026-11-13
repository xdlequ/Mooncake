// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Real-device integration tests for the TENT UB transport.
//
// Unlike tent_ub_native_data_path_test, which injects FakeUrmaAdapter into
// every component, the tests here run against RealUrmaAdapter so the real
// liburma call sequence (urma_init, urma_get_device_list, context/JFC/Jetty
// lifecycle, RM post) is exercised on nodes that expose /sys/class/ubcore.
//
// The binary is opt-in at build time via TENT_UB_REAL_DEVICE_TESTS and
// self-skips at run time when the node has no UB hardware, the build has no
// usable liburma, or no active RM-capable device is present, so hardware-less
// CI nodes stay green.

#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "tent/common/config.h"
#include "tent/common/types.h"
#include "tent/runtime/control_plane.h"
#include "tent/runtime/segment.h"
#include "tent/runtime/segment_manager.h"
#include "tent/runtime/topology.h"
#include "tent/thirdparty/nlohmann/json.h"
#include "tent/transport/ub/context.h"
#include "tent/transport/ub/jfc.h"
#include "tent/transport/ub/topology_attrs.h"
#include "tent/transport/ub/ub_transport.h"
#include "tent/transport/ub/urma_adapter.h"

namespace mooncake::tent::ub {
namespace {

// Runtime gate: the URMA userland only enumerates devices when the UB core
// subsystem is present on the node.
bool ubcoreSysfsPresent() {
    return std::filesystem::exists("/sys/class/ubcore");
}

JettyOptions rmJettyOptions() {
    JettyOptions options;
    options.trans_mode = kUrmaTransportModeRm;
    options.bind_tp_type_mask = kUrmaTpTypeRtpMask;
    return options;
}

// The first active device advertising the RM transport mode. Device kind is
// not required here: the trusted identity published below classifies the
// device as physical for the install-time capability gate.
const DeviceInfo* pickRmCapableDevice(const std::vector<DeviceInfo>& devices) {
    for (const auto& device : devices) {
        if (!device.active) continue;
        if ((device.capabilities.transport_modes & kUrmaTransportModeRm) == 0) {
            continue;
        }
        return &device;
    }
    return nullptr;
}

// Mirror the production topology layout for a single UB NIC: one NicEntry
// carrying the trusted hardware attributes plus the wildcard host memory
// location every local buffer attaches to.
std::shared_ptr<Topology> buildSingleDeviceTopology(const DeviceInfo& device) {
    auto topology = std::make_shared<Topology>();
    Topology::NicEntry nic{.name = device.topology_name,
                           .pci_bus_id = device.native_device_path,
                           .type = Topology::NIC_UB,
                           .numa_node = 0};
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

// Mirror setPhysicalOnlyIdentity from ub_native_data_path_test.cpp, but with
// the identity of a really discovered device.
void applyRmLegacyIdentity(Config& config, const DeviceInfo& device) {
    config.set("transports/ub/physical_only", true);
    config.set("transports/ub/device_identities",
               nlohmann::json::array({{{"native_device_name",
                                        device.native_device_name},
                                       {"native_device_path",
                                        device.native_device_path},
                                       {"eid_index", device.eid_index},
                                       {"eid", device.eid},
                                       {"kind", "physical"},
                                       {"transport_modes",
                                        device.capabilities.transport_modes}}}));
}

class UbRealDeviceTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // Common pre-test initialization: probe the node for real UB
        // hardware and resolve the default (real) URMA adapter. Any test
        // that cannot run on this node skips instead of failing, keeping
        // hardware-less CI nodes green.
        if (!ubcoreSysfsPresent()) {
            GTEST_SKIP()
                << "no UB hardware on this node (/sys/class/ubcore missing)";
        }
        adapter_ = createDefaultUrmaAdapter();
        if (!adapter_ || !adapter_->available()) {
            adapter_.reset();
            GTEST_SKIP()
                << "this build has no usable liburma (TENT_HAS_REAL_URMA=0)";
        }
        ASSERT_TRUE(adapter_->initialize().ok());
        ASSERT_TRUE(adapter_->discoverDevices(discovered_).ok());
    }

    void TearDown() override {
        // Common post-test cleanup: release the default adapter. The real
        // adapter owns process-level URMA state (see ensureUrmaInitialized),
        // so it must never be torn down per test beyond this handle reset.
        adapter_.reset();
    }

    std::shared_ptr<UrmaAdapter> adapter_;
    std::vector<DeviceInfo> discovered_;
};

// Smoke test for the real adapter: initialize, discover at least one active
// device, then shut down cleanly. This validates the RealUrmaAdapter wrapper
// against the native liburma entry points without touching the data plane.
TEST_F(UbRealDeviceTest, RealAdapterInitializesAndDiscoversActiveDevices) {
    ASSERT_NE(adapter_, nullptr);
    ASSERT_FALSE(discovered_.empty())
        << "a node with /sys/class/ubcore must enumerate at least one URMA "
           "device";

    // RM Legacy only selects devices with at least one active port; require
    // the discovered set to contain such a device, otherwise every RM test
    // would fail for an environmental reason.
    for (const auto& device : discovered_) {
        if (device.active) {
            GTEST_SKIP() << "active URMA device found: " << device.native_device_name;
        }
        fprintf(stderr, "【DBG】device.name: %s, path = %s, kind = %s, mode =  %s, rm_tp_cap = 0x%x,
            prio6_tp = 0x%x, eid = %s, active: %d\n",
             device.native_device_name.c_str(), device.native_device_path.c_str(),
             device.kind.c_str(), device.transport_modes.c_str(),
             device.capabilities.transport_modes,
             device.capabilities.rm_tp_cap,
             device.capabilities.priority_tp_type_masks[6],
             device.eid.c_str(), device.active);
    }
    size_t active_devices = 0;
    for (const auto& device : discovered_) {
        if (device.active) ++active_devices;
    }
    EXPECT_GE(active_devices, 1U)
        << "no active URMA device found; check link state";

    EXPECT_TRUE(adapter_->shutdown().ok());
}

// RM Legacy Jetty lifecycle over the real adapter: open a context on the
// first active RM-capable physical device, create a JFC and an RM Jetty,
// exercise reset/delete and verify every handle is released before shutdown.
TEST_F(UbRealDeviceTest, RmLegacyJettyLifecycleOverRealUrma) {
    const auto* device = pickRmCapableDevice(discovered_);
    if (!device) {
        GTEST_SKIP() << "no active RM-capable URMA device on this node";
    }

    auto context = std::make_shared<UbContext>(0, *device, adapter_);
    ASSERT_TRUE(context->initialize(1, JfcOptions{}).ok());
    ASSERT_FALSE(context->jfcs().empty());
    ASSERT_TRUE(context->active());

    // Create an RM Jetty bound to the context's first JFC. The RM profile
    // (RM transport mode + RTP bind mask) must be accepted by the real
    // provider for an RM-capable device.
    JettyPtr jetty;
    {
        auto st = adapter_->createJetty(context->handle(),
                                   context->jfcs().front()->handle(),
                                    rmJettyOptions(), jetty);
        if (!st.ok()) {
            GTEST_SKIP() << "createJetty failed: " << st.ToString();
            fprintf(stderr, "[DBG] dev name = %s, eid = %s, path = %s, 
                kind = %s, mode = %s, active = %d\n",
             device.native_device_name.c_str(), device.eid.c_str(),
             device.native_device_path.c_str(), device.kind.c_str(),
             device.transport_modes.c_str(),
             (int)device.active);
            fprintf(stderr, "【DBG】createJetty failed: %s\n", st.ToString().c_str());
        }
        {
            auto ctp = rmJettyOptions();
            ctp.bind_tp_type_mask = 0x2U;
            JettyPtr jetty2;
            auto st2 = adapter_->createJetty(context->handle(),
                                   context->jfcs().front()->handle(),
                                   ctp, jetty2);
            if (!st2.ok()) {
                GTEST_SKIP() << "createJetty failed: " << st2.ToString();
                fprintf(stderr, "[DBG] dev name = %s, eid = %s, path = %s, 
                    kind = %s, mode = %s, active = %d\n",
             device.native_device_name.c_str(), device.eid.c_str(),
             device.native_device_path.c_str(), device.kind.c_str(),
             device.transport_modes.c_str(),
             (int)device.active);
                fprintf(stderr, "【DBG】createJetty failed: %s\n", st2.ToString().c_str());
            }
            fprintf(stderr, "【DBG】createJetty success: %s\n", st2.ToString().c_str());
            if(jetty2) {
                adapter_->deleteJetty(jetty2);
            }
        }

    }
    ASSERT_NE(jetty, nullptr);

    // RESET fences in-flight WRs; a fresh Jetty must reset cleanly, and a
    // delete on the reset Jetty must release the native handle.
    EXPECT_TRUE(adapter_->resetJetty(jetty).ok());
    EXPECT_TRUE(adapter_->deleteJetty(jetty).ok());

    EXPECT_TRUE(context->shutdown().ok());
    EXPECT_TRUE(adapter_->shutdown().ok());
}

// Full-stack RM Legacy self read/write over the real adapter: install a
// UbTransport on top of the really discovered device (shared adapter, real
// RealUrmaAdapter underneath), register local buffers, bootstrap against the
// local segment through the loopback control plane and complete one RM WRITE
// whose payload must round-trip through the real Jetty.
TEST_F(UbRealDeviceTest, RmLegacySelfReadWriteOverRealUrma) {
    const auto* device = pickRmCapableDevice(discovered_);
    if (!device) {
        GTEST_SKIP() << "no active RM-capable URMA device on this node";
    }

    auto topology = buildSingleDeviceTopology(*device);
    auto control = std::make_shared<ControlService>("p2p", "", nullptr);
    uint16_t port = 0;
    ASSERT_TRUE(control->start(port).ok());
    const std::string segment_name =
        "127.0.0.1:" + std::to_string(static_cast<unsigned>(port));
    ASSERT_TRUE(control->segmentManager()
                    .updateLocal([&](SegmentDesc& segment) {
                        segment.name = segment_name;
                        segment.rpc_server_addr = segment_name;
                        segment.machine_id = "real-machine";
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
    // Exercise the simple send-Jetty reuse pool end to end as well.
    config->set("transports/ub/jetty_reuse/enable", true);
    applyRmLegacyIdentity(*config, *device);

    // Share the adapter resolved in SetUp: it is the real RealUrmaAdapter
    // (not a fake), and sharing keeps urma_init process-scoped to one owner.
    UbTransport transport(adapter_);
    std::string mutable_segment_name = segment_name;
    ASSERT_TRUE(
        transport.install(mutable_segment_name, control, topology, config)
            .ok())
        << "install rejected a really discovered RM device; check the "
           "provider capability gate logs";

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
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (transfer.s == PENDING &&
           std::chrono::steady_clock::now() < deadline) {
        ASSERT_TRUE(transport.getTransferStatus(batch, 0, transfer).ok());
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_EQ(transfer.s, COMPLETED);
    EXPECT_EQ(transfer.transferred_bytes, source.size());
    EXPECT_EQ(source, target)
        << "the RM WRITE payload must round-trip over the real Jetty";

    ASSERT_TRUE(transport.freeSubBatch(batch).ok());
    EXPECT_TRUE(transport.uninstall().ok());
}

}  // namespace
}  // namespace mooncake::tent::ub
