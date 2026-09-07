# Task 01 Final Production Readiness Audit

审计时间：2026-09-07 09:27 +0800

## Final Status

Task 01 — API Contract & Runtime Reliability

FINAL STATUS: COMPLETE

Blocking findings: 0

## Scope

本次审计只覆盖 `Frontend Production Readiness / Task 01` 已完成范围：API contract、response boundary、request lifecycle、realtime 聚合、可选 WebSocket 安全回退、History/Alarm/Dashboard 部分失败降级、PointEditor P0 性能热点，以及真实 executable Spring Boot JAR smoke。

本次审计不是 Task 02+ 的功能入口，不评估或实现 WebSocket backend、虚拟表格、Electron 安全交付、依赖安全升级、bundle/startup 优化、真实 PLC 长稳、TDengine/Redis/Cloud 生产 SLA。

## Audit Matrix

| Task | Goal | Status | Evidence | Residual Risk | Blocking? |
|---|---|---|---|---|---|
| 01.1 | 建立 API inventory 与 reliability baseline | PASS | `API-CONTRACT-INVENTORY.md` 与 `RELIABILITY-BASELINE.md` 仍能解释 RAW DTO、`ApiResult<T>`、command envelope、legacy compatibility；后续 smoke 与 wrappers 未发现 baseline 失真。 | Inventory 是基线文档，不替代后续协议/存储/云端验收。 | No |
| 01.2 | 明确 API response boundary 与稳定 TypeScript contract | PASS | `collector-desktop/src/api` 搜索 `Promise<unknown>` 为 0；业务 wrappers 使用 `requestApiData<T>()`、`requestRaw<T>()`、`requestEnvelope<T>()`；真实 smoke 验证 `ApiResult` 与 RAW aggregate。 | `unknown`/`Record<string, unknown>` 仍保留在 telemetry value、shadow document、additionalConfig、动态协议字段等合理动态边界。 | No |
| 01.3 | request lifecycle latest-request-wins 与 loading ownership 分离 | PASS | `createLatestRequestOwner`、feature/store-level generation tests 覆盖 Realtime、History、Alarm、DeviceConfigPanel、DeviceOperationShell、stores、Log、Dashboard、PointEditor。 | `ConfigOpsPanel` 初始化 sync status `.catch(() => undefined)` 保留为已知 P2。 | No |
| 01.4 | Realtime HTTP aggregate 与可选 WebSocket 安全回退 | PASS | `loadRealtimeRowsByContext()` all-mode 只调用 aggregate；后端 `getAllRealtimeData()` 一次构造全部 cache keys 并单轮 `cacheManager.getAll(allCacheKeys)`；测试覆盖 N=1/10/100、zero-device、per-device error。 | backend `/ws/realtime` 未实现；生产 realtime baseline 仍是 HTTP aggregate polling。 | No |
| 01.5 | History / Alarm / Dashboard 部分失败与降级 UX | PASS | History main critical、compare/alarms optional；Alarm history critical、ack optional、ack write authoritative；Dashboard 8 sources 独立 state、stale/unavailable、all-failed 不更新 `lastRefresh`。 | Optional datasource 降级只是 contract/UX 保真，不证明被关闭依赖的生产 SLA。 | No |
| 01.6 | PointEditor `runtimeOf()` O(P²) 热点修复 | PASS | `runtimeMergedRows` 搜索为 0；`runtimeLookup` 由 `realtimeRows` computed 构建；`resolvePointRuntime()` 按 `pointId -> pointCode -> address`，空 identity 不入索引；表格仍用 `filteredPoints` 原始引用。 | 标准 `el-table` DOM 渲染、`filteredPoints`、`selectedPoint` 仍是线性成本；CSV preview 仍可能渲染多行。 | No |
| 01.7 | 真实 executable JAR + real HTTP socket smoke | PASS | 当前 HEAD 重新 `build:web`、package executable JAR、检查 BOOT-INF 静态资源；`scripts/run-console-real-smoke.ps1` 启动真实 `java -jar`，HTTP smoke exit 0。 | Minimal smoke 关闭 TDengine/Redis stream/report/MQTT/shadow persistence/alarm state，不覆盖生产外部依赖 SLA。 | No |
| 01.8 | Final audit 与证据闭环 | PASS | 完整 frontend gates、Maven nofork clean verify、P0 nofork regression、package、JAR content、real smoke、secret scan、diff check 完成；发现的 smoke-script actuator 503 断言已最小修复并重跑通过。 | 默认 forked Surefire 在本机 Windows 环境仍触发已知 manifest/JVM goodbye 问题；`-DforkCount=0` 全量验证通过。 | No |

## API Contract Audit

- RAW DTO：`/api/data/**` 与 `/monitor/**` 继续通过 `requestRaw<T>()` 或 RAW DTO 类型保留顶层 payload；`/api/data/realtime` 真实 HTTP 返回顶层 `status/deviceCount/dataCount/devices/timestamp`。
- ApiResult：`/api/config/**`、`/api/device/**`、`/api/ops/**`、control、shadow、edge 继续通过 `requestApiData<T>()` 或 `requestEnvelope<T>()` 表达稳定 wrapper。
- Envelope：command 类接口保留 message/deviceId/running/count 等 outer fields，不把所有接口强行解为 data。
- `ApiResult.statusSuccess(...)`：当前 contract 为 `code=200` + `status=success`，`ApiResultTest` 与 controller regression tests 已覆盖。
- Stable unknown regression：`collector-desktop/src/api` 中 `Promise<unknown>` 搜索结果为 0。
- Dynamic unknown：telemetry value、shadow reported/desired/delta/metadata/history、additionalConfig、protocol dynamic fields 继续允许动态对象。

Result: PASS

## Request Lifecycle Audit

- Realtime：主表与单点查询按 mode/device/point snapshot + generation 提交；timer overlap guard 保持。
- RealtimeDataPanel：HTTP fallback 有 panel-level owner；WS stale rows 不遮挡 disconnected/unavailable 后的 HTTP fallback。
- History：查询 snapshot 统一裁决 main/compare/alarm settled result；fatal main failure 清当前结果，optional failure 标记 warning。
- Alarm：history read 与 ack enrichment 分离；ack WRITE 作为 authoritative side effect，不被 stale bulk read 覆盖。
- Device config：protocol/status/workbench/diff 四个独立 owner，same-protocol device switch 仍重新读取。
- Stores：`device.store`、`point.store`、`protocol.store`、`runtime.store` 使用 store-instance 或 per-key generation；`websocket.store` 使用 store-instance runtime `WeakMap`。
- Log：server query 与本地 filter 分离；changed server query 不被 pending timer 吞掉。
- Dashboard：refresh cycle latest guard；初始化失败不会启动下游 metric requests，不会卡 loading。
- PointEditor：component-instance realtime owner，device switch/unmount invalidate。
- Writes：save、acknowledge、control、config update 不被 latest-discard 请求本身；仅 post-write UI commit 受当前 context 约束。

Result: PASS

## Realtime Audit

- Aggregate HTTP request count：前端 all-mode 只调用 `getAllDeviceRealtimeData()`；N=1/10/100 tests 均断言不调用 per-device API。
- Backend bulk cache lookup：`getAllRealtimeData()` 收集全部设备点位 cache keys 后一次 `cacheManager.getAll(allCacheKeys)`，再内存 group。
- Zero devices：返回 `status=success, deviceCount=0, dataCount=0, devices=[]`。
- Per-device degradation：无 points 的设备作为 inner device error/degraded，不拖垮 outer aggregate。
- WS default：默认 disabled；backend `/ws/realtime` 未实现，不作为 Task 01 blocker。
- WS generation：callback 校验 socket + connectionGeneration + deviceId；store runtime 使用 `WeakMap`，无 module-global singleton owner。
- Reconnect：首次 handshake failure 不重连；成功连接后最多 5 次指数退避 1/2/4/8/16 秒。
- HTTP fallback：WS disabled/unavailable/disconnected/stale rows 时保留 5000ms HTTP polling。

Result: PASS

## Partial Failure Audit

### History

- Main：critical；失败时清主历史、compare、related alarms，并设置 fatal error。
- Compare：optional；单个失败从 series/export 中排除，成功空数组与失败区分。
- Alarms：optional；失败标记 unavailable，不清成功历史。

### Alarm

- History：critical；失败清当前查询结果并显示错误。
- Ack：optional enrichment；失败保留 alarm rows 与最后已知 ack state。
- READ/WRITE race：ack write 会 invalidate bulk ack read，旧 bulk read 不能覆盖已确认结果或产生 stale warning。
- Unknown：ack unavailable 时显示“状态未同步”，不伪装成“待确认”。

### Dashboard

- Source states：8 sources 具备独立 `idle/loading/success/error`、`lastSuccessAt`。
- Stale：失败且有 last success 时保留 last known good 并标记 stale。
- Unavailable：首次失败无旧数据时标记 unavailable，不展示成功空态。
- All failed：不更新 `lastRefresh`；partial success 才允许更新并显示 partial warning。

Result: PASS

## PointEditor Performance Audit

- Runtime lookup：`runtimeLookup = computed(() => buildPointRuntimeLookup(realtimeRows.value))`。
- Lookup complexity：`resolvePointRuntime()` 单点最多 `pointId/pointCode/address` 三次 `Map.get()`，O(1)。
- Table complexity：runtime lookup 热点从 O(P²) 收敛为 O(P + R)；表格仍线性渲染。
- Editable references：`<el-table :data="filteredPoints">` 保留原始 editable row 引用，未切成 merged spread copies。
- Dynamic field：`displayExtraValue()` 使用 `getPointExtraValue()` 直接读取 `additionalConfig` path，不再 per-cell 构建临时 `PointExtraModel`。
- Residual：`el-table` DOM、`filteredPoints`、`selectedPoint.find()`、CSV preview 仍是非阻塞线性/后续优化项。

Result: PASS

## Real Backend Audit

- Executable JAR：`collector-boot/target/data-collection-service-0.0.1-SNAPSHOT.jar` built。
- JAR content：`missing=[]; js=36; css=18`。
- `java -jar`：真实进程启动，context path `/collector`，port `19090`。
- `/health`：HTTP 200，JSON 可解析，`status=DOWN` 属于当前关闭外部依赖后的可达健康状态。
- Actuator：HTTP 503 + JSON `status=DOWN` 被接受为合法 Actuator degraded/down contract。
- Desktop：`/collector/desktop/index.html` HTTP 200 HTML。
- Assets：从 index 解析的 JS 与 CSS asset 均 HTTP 200 且 body 非空。
- Auth：no-token / invalid token 均 401；valid smoke token 进入业务接口。
- ApiResult：`/api/config/devices` HTTP 200，`code=200`，`data` present。
- RAW：`/api/data/realtime` HTTP 200，RAW aggregate 顶层字段存在。
- Dashboard endpoints：config devices、device runtime、report、runtime、system、cache、performance 为 PASS；recent alarms 与 storage 为 expected DEGRADED JSON。
- Temporary device：固定 `smoke-local-http-01` 创建、读取、points、device realtime、single point、aggregate、summary、cleanup 均通过；未执行 start device。
- Cleanup：只删除固定 smoke device；cleanup 后 readback 不再成功，aggregate 不再包含该 device；owned Java PID 已停止。
- Exit code：real backend smoke exit 0。

Result: PASS

## Regression Verification

### Frontend

- `npm run typecheck`: PASS
- `npm run lint`: PASS
- `npm run stylelint`: PASS
- `npm test`: PASS — 64 test files，427 tests
- `npm run build`: PASS
- `npm run build:web`: PASS — 同步文件数 55
- `npm run verify`: PASS — 内含 typecheck/lint/stylelint/test/build/build:web，64 test files，427 tests

### Backend

- `cmd.exe /c mvn -B -ntp clean verify`: ENVIRONMENT ISSUE — Windows forked Surefire boot JVM 退出码 0 但未正常 goodbye，未出现测试 failure；记录为本地 launcher/fork 环境问题。
- `cmd.exe /c mvn -B -ntp -DforkCount=0 clean verify`: PASS
- `cmd.exe /c mvn -B -ntp -pl collector-boot -am -Pp0-regression test`: ENVIRONMENT ISSUE — 同类 forked Surefire 问题。
- `cmd.exe /c mvn -B -ntp -DforkCount=0 -pl collector-boot -am -Pp0-regression test`: PASS
- `cmd.exe /c mvn -B -ntp -pl collector-boot -am clean package -DskipTests`: PASS
- JAR content check: PASS

### Integration

- `scripts/run-console-real-smoke.ps1`: PASS，exit 0，真实 `java -jar` + real HTTP socket。

### Security / Git

- `node scripts/scan-config-secrets.mjs`: PASS
- `git diff --check`: PASS（仅 Git line-ending warning，无 whitespace error）
- `git diff -- collector-boot/src/main/resources/static/desktop`: no diff

## Findings

### BLOCKER

None.

### DEFERRED

- backend `/ws/realtime` 未实现；生产 realtime baseline 仍是 HTTP aggregate polling。
- `ConfigOpsPanel` 初始化 sync status `.catch(() => undefined)` 保留为 P2，不影响核心生产流程成功/失败语义。
- 标准 `el-table` DOM rendering、`filteredPoints`、`selectedPoint`、CSV preview 仍可在 Task 02 或后续专项优化。
- Minimal smoke 不覆盖 TDengine / Redis / Cloud / MQTT / 真实 PLC 生产 SLA。
- Electron delivery/security、dependency security、bundle/startup performance 属于后续任务。
- Windows forked Surefire 环境问题仍存在；本轮通过 `-DforkCount=0` 完成真实 Maven test/verify 兜底。

### INFORMATIONAL

- 01.7 smoke 脚本在本轮审计中修正 Actuator health 503/down 的合法 degraded 判断。
- `scripts/soak/**` 等已提交脚本按用户确认保留，未做 scope cleanup。

## Repairs During Audit

| Finding | Root Cause | Repair | Regression |
|---|---|---|---|
| `ApiResult.statusSuccess(...)` test 仍按旧 legacy shape 断言无 `code` | 01.7 已修复真实 contract 为 `code=200 + status=success`，但通用 `ApiResultTest` 的 legacy 断言未同步 | 更新 `ApiResultTest`，确认 status-style success / device success JSON 均含 `code=200` | `collector-common` nofork test、full Maven nofork verify、controller tests、real smoke 均通过 |
| real smoke 对 Actuator health 硬要求 HTTP 200 | Spring Boot Actuator 在 DOWN 时可返回 HTTP 503，但 body 仍是合法 health JSON；用户 Task 明确允许 UP/DEGRADED/DOWN | 新增 `Assert-StatusCodeIn`，允许 actuator health HTTP 200/503，并继续要求 JSON status field | PowerShell parse 通过；真实 JAR smoke 重跑 exit 0 |

## Residual Risks

- backend `/ws/realtime` not implemented；当前生产实时基线为 HTTP aggregate polling。
- WebSocket 是安全可选前端路径，不是端到端 production-ready transport。
- `ConfigOpsPanel` 初始 sync status 吞错为 P2，未升级 blocker。
- `el-table` DOM 渲染仍线性，未引入 virtualization/pagination。
- Full real TDengine / Redis / Cloud / MQTT 不在 01.7 minimal smoke 覆盖范围内。
- 真实 PLC / OPC / IEC 验收属于独立 industrial protocol validation，不在 Task 01。
- Electron security/delivery、dependency security、bundle/startup performance 属于后续任务。

## Final Acceptance Matrix

- [x] 01.1 inventory remains valid
- [x] API stable wrappers have explicit contracts
- [x] RAW / ApiResult / envelope boundaries verified
- [x] no P0/P1 stable API unknown regression
- [x] latest-request-wins preserved
- [x] loading ownership separated from commit eligibility
- [x] writes remain authoritative side effects
- [x] Pinia request state instance-safe
- [x] aggregate realtime remains 1 HTTP request
- [x] backend aggregate remains single bulk cache lookup
- [x] zero-device aggregate works
- [x] WebSocket default disabled
- [x] WS finite reconnect remains
- [x] WS stale-device messages blocked
- [x] HTTP fallback remains available
- [x] History critical/optional boundary correct
- [x] History failed compare excluded from series/export
- [x] Alarm history survives ack failure
- [x] ack unknown != unacknowledged
- [x] ack READ cannot overwrite completed WRITE
- [x] Dashboard 8 sources independently degrade
- [x] Dashboard all-failed does not update lastRefresh
- [x] PointEditor runtimeOf is O(1) lookup
- [x] PointEditor editable row references preserved
- [x] real backend executable JAR smoke passes
- [x] auth boundaries pass
- [x] static Vue assets pass
- [x] ApiResult real contract passes
- [x] RAW aggregate real contract passes
- [x] temporary device lifecycle passes
- [x] P0 Maven regression passes via nofork fallback; default fork mode is environment issue
- [x] frontend verify passes
- [x] secret scan passes
- [x] no BLOCKER remains
