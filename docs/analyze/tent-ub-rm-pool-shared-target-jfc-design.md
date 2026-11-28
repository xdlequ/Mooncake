# TENT UB `rm_pool` 共享接收目标与 JFC 方案

## 已确认的问题

- `UbEndpoint::prepare()` 在 `rm_pool` 下从 Context 的 `UbLocalPortTargetRegistry` 获取一个共享接收 Jetty，但旧代码把 `published_target_count_` 设为 `jetty_per_endpoint`。`makeBootstrapDescV2()` 随后将同一 Jetty 写入 `rm.targets` 多次。`validateV2()` 对 `target_identity` 做唯一性校验，因此配置值大于 1 时会报 `duplicate target identity`。
- `bindRm()` 原先要求对端目标数等于本端发布数，两个端点即使都使用共享接收目标，也被迫发送重复条目。`jetty_per_endpoint` 在池化路径只应影响发送池规模的兼容输入，不应决定接收目标数。
- `jfc_per_context` 默认 6，`poller_count` 默认 2。JFC 已由 Context 持有并由多个端点共享，不是每端点创建 6 个。RM 模式下一个 `RealJfc` 的 JFS 与 JFR 已共用同一个 native JFC；业务 poller 在所有 Context 的 JFC 槽之间轮询。
- `UbContext::initialize()` 原先对每个 JFC 槽固定按两个 native JFC 检查设备上限，与 `rm_pool` 的实际资源数不符。

## yuanrong 的对应设计

- `UrmaResource::Init()` 为活动设备创建一个 JFC；`UrmaManager::ServerEventHandleThreadMain()` 由一个 `UrmaPollJfc` 线程轮询这个 JFC。
- `UrmaResource::GetOrCreateSharedRecvJetty()` 在资源层缓存一个接收 Jetty。`UrmaManager::GetOrCreateLocalJetty(..., RECV)` 对不同连接返回同一个 Jetty ID，握手按连接传递这个 ID，而不是为每条发送 lane 复制一组接收目标。
- 发送 Jetty 另由池管理；`UrmaJetty::Create()` 将发送 JFS 和共享 JFR 连接到同一个 JFC。Mooncake 保留多设备 Context 和发送池，但采用相同的资源所有权区分。

## 实施规则

1. `rm_pool` 每个 Context/配置端口只发布一个共享接收目标。Bootstrap v2 的 `rm.targets` 只有一个条目；`bindRm()` 接受一个目标并通过 Context registry 导入一次。发送池可有多个 Jetty，所有发送 WR 均可指向该共享目标。RM Legacy 仍按本地 Jetty 数发布目标。
2. 保留 Bootstrap 的目标身份唯一性校验；不能通过放宽校验接受重复条目。旧节点若仍发布重复目标，混部时会被拒绝；升级两端或临时将旧节点的 `jetty_per_endpoint` 设为 1。
3. `rm_pool` 未显式配置时使用每 Context 一个业务 JFC、全 transport 一个业务 poller。一个 poller 可遍历多个 Context 的业务槽；probe JFC 仍在现有 poller 轮询序中。显式 `jfc_per_context` 和 `poller_count` 保留为容量调优入口，RC/RM Legacy 默认值保持原样。
4. `rm_pool` 的设备 JFC 上限按实际 native 数计，每槽一个；其他运行模式保留原有上限检查。发送池的 JFC credit ledger、成员轮转和完成轮询继续基于实际 Context JFC 槽数工作。

## 多 EID 候选

自动导入按发现顺序保留每个完整的物理设备/EID 身份，不再按 native device path 只保留最小的 EID index。bonding 与其他已知非物理设备仍被过滤；随后身份解析和物理设备选路会看到全部有效 EID。已有配置文件中非空的 `device_identities` 会被视为显式配置并优先使用，旧版自动保存的单 EID 列表需要移除后重新自动导入。

## 验证与上线关注点

- 配置默认测试覆盖 `rm_pool` 的 JFC/poller 为 1，以及显式值可覆盖；设备上限为 1 时 `rm_pool` Context 能初始化，其他模式保留原有检查。
- 使用默认 `jetty_per_endpoint=6` 的 `rm_pool` 自环读写测试覆盖 Bootstrap、导入、发送与完成，回归原先的重复目标故障。保留 Bootstrap 对真正重复身份的拒绝测试。
- 自动导入测试覆盖同一物理设备的多个 EID 均成为选路候选。
- 单 JFC/单 poller 可能成为高吞吐或多设备部署的瓶颈；用显式配置扩容，并在实机压测中检查 JFC 深度、完成延迟和 poller CPU。probe JFC 仍独立创建，不能把“一个业务 JFC”误读为全进程只存在一个 native JFC。
