# Task 02.1 Realtime Scale Baseline & Bottleneck Inventory

生成时间：2026-09-07 10:50 +0800

## 1. Scope

本文件只建立 `Task 02 — Realtime & Large Data Performance` 的测量基线和瓶颈清单，不引入生产性能架构改造。

本轮 production behavior diff 预期为 0：不新增 API contract，不修改 Java/Vue/TS 生产逻辑，不实现 delta、WebSocket backend、虚拟表格、分页或 compact DTO。

## 2. Baseline

- branch: `feature_2.0`
- start commit: `b37171bd952ac1da32ee25956d0715c485373479`
- working tree at start: clean
- Task 01 status: `PASS / COMPLETE`

## 3. Environment

Timing 数字只代表本机 Node/Vitest benchmark 环境，用于相对排序，不作为 SLA。

| Item | Value |
|---|---|
| OS | Windows 10 amd64 |
| CPU | Intel Core i7-10700 |
| Physical cores | 8 |
| Logical cores | 16 |
| RAM | 约 31.7 GiB |
| Node | v22.23.2 |
| npm | 12.0.2 |
| Maven | 3.6.3 |
| Maven Java | 17.0.15 |
| Default `java -version` on PATH | 21.0.7 |

## 4. Current Realtime Architecture

```text
RealtimeView
    ↓
loadRealtimeRowsByContext
    ↓
GET /api/data/realtime
    ↓
RealtimeDataQueryApplicationService.getAllRealtimeData()
    ↓
ConfigManager.getAllDeviceIds()
    ↓
ConfigManager.getDataPoints(device)
    ↓
build all CacheKey
    ↓
cacheManager.getAll(allCacheKeys)
    ↓
per point:
pointRuntimeStateService.snapshot(...)
PointRealtimePayload.fromPoint(...)
applyCachedValue(...)
    ↓
AllDeviceRealtimeDataResponse
    ↓
Jackson JSON serialize
    ↓
HTTP
    ↓
Axios JSON parse
    ↓
normalizeAllDeviceRealtimeRows()
    ↓
normalizeRealtimeRows()
    ↓
realtimeRows.value = rows
    ↓
filteredRealtimeRows
    ↓
buildRealtimeSummary()
    ↓
native <table>
v-for every filtered row
```

Current facts:

- polling: `5000 ms`
- request count: all-device mode uses `1 × GET /api/data/realtime`
- backend cache: single `cacheManager.getAll(allCacheKeys)` bulk cache call
- response: full `AllDeviceRealtimeDataResponse` snapshot
- frontend: full JSON parse + full normalization per refresh
- render: native `<table>` with `v-for` over every filtered row

## 5. Scale Dataset

Synthetic aggregate follows the current real contract:

- aggregate: `status`, `deviceCount`, `dataCount`, `devices[]`, `timestamp`
- device: `status`, `deviceId`, `dataCount`, `data`, `timestamp`
- data: `Record<pointId, PointRealtimePayload>`
- payload width: representative rich point payload, not `{ value: 1 }` only
- additionalConfig: included with typical protocol fields, but not intentionally extreme

| Dataset | Devices | Points/device | Total points |
|---:|---:|---:|---:|
| 10k | 10 | 1,000 | 10,000 |
| 50k | 50 | 1,000 | 50,000 |
| 100k | 100 | 1,000 | 100,000 |

Single-device baseline:

| Dataset | Devices | Points/device | Total points |
|---:|---:|---:|---:|
| single 1k | 1 | 1,000 | 1,000 |
| single 5k | 1 | 5,000 | 5,000 |
| single 10k | 1 | 10,000 | 10,000 |

Benchmark command:

```bash
cd collector-desktop
npx vitest bench src/features/realtime/utils/realtime-scale.bench.ts --run --outputJson <output-json>
```

Benchmark settings:

- benchmark-only fixture: `src/features/realtime/utils/realtime-scale-fixture.ts`
- benchmark file: `src/features/realtime/utils/realtime-scale.bench.ts`
- samples: heavy operations use 8 samples; lighter operations have more samples from Vitest/Tinybench
- reported timing: median ms
- no hard threshold or CI SLA

## 6. Payload Baseline

Raw JSON size is measured by `JSON.stringify(...)` plus UTF-8 byte length. Compression is not measured in this baseline.

| Points | Devices | Raw JSON bytes | MiB / refresh | Bytes / point | JSON stringify median | JSON parse median |
|---:|---:|---:|---:|---:|---:|---:|
| 10,000 | 10 | 15,135,252 | 14.43 MiB | 1,513.5 | 99.27 ms | 74.76 ms |
| 50,000 | 50 | 75,809,163 | 72.30 MiB | 1,516.2 | 558.46 ms | 445.03 ms |
| 100,000 | 100 | 151,651,557 | 144.63 MiB | 1,516.5 | 1,199.98 ms | 858.59 ms |

Five-second polling raw transfer model:

Formula:

```text
MiB / refresh = rawBytes / 1024 / 1024
MiB / second = MiB / refresh / 5
MiB / minute = MiB / second × 60
GiB / hour = rawBytes × (1 / 5) × 3600 / 1024³
```

| Points | MiB / refresh | MiB / second | MiB / minute | GiB / hour |
|---:|---:|---:|---:|---:|
| 10,000 | 14.43 | 2.89 | 173.21 | 10.15 |
| 50,000 | 72.30 | 14.46 | 867.57 | 50.83 |
| 100,000 | 144.63 | 28.93 | 1,735.51 | 101.69 |

Compression note:

- `server.compression.enabled=true`
- mime types include `application/json`
- `server.compression.min-response-size=2048`
- 本轮未测 gzip / compressed wire size；以上均为 raw JSON size。

## 7. Backend Complexity Inventory

GOOD already established in Task 01.4A:

- all-device realtime remains `1` aggregate endpoint call from frontend
- backend aggregate uses `1` bulk cache lookup: `cacheManager.getAll(allCacheKeys)`
- no per-device frontend HTTP fan-out

Current O(P) backend work per aggregate refresh:

| Step | Scale model | 100k implication |
|---|---:|---:|
| `ConfigManager.getAllDeviceIds()` | O(D) | 100 device ids |
| `ConfigManager.getDataPoints(device)` | O(D) calls, O(P) returned points | 100 calls, 100k point configs |
| `buildCacheKeys(deviceId, dataPoints)` | O(P) | ~100k `CacheKey` entries |
| `cacheManager.getAll(allCacheKeys)` | 1 bulk call over P keys | 1 call, 100k keys |
| `pointRuntimeStateService.snapshot(...)` | O(P) | ~100k runtime snapshot accesses |
| `PointRealtimePayload.fromPoint(...)` | O(P) | ~100k DTO payload allocations |
| `payload.applyCachedValue(...)` | O(P) | ~100k value/quality metadata applications |
| Jackson serialize | O(payload bytes) | ~145 MiB raw JSON serialize |

Likely cost candidates:

- Backend CPU / allocation: O(P) payload construction and runtime snapshot access.
- Network / serialization: raw payload width dominates at 50k/100k.
- Allocation pressure candidate: each 100k refresh creates large Maps/Lists, ~100k payload objects, serialized JSON, and later ~100k frontend row objects.

Not claimed:

- No GC bottleneck is confirmed here; profiler evidence is required in a later task.

## 8. Frontend CPU Benchmark

All-device mode:

| Points | Devices | Normalize median | Summary median | Filter no keyword | Filter many matches | Filter zero matches | Device-name fallback filter |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 10,000 | 10 | 31.79 ms | 0.67 ms | ~0.00 ms | 1.63 ms | 3.30 ms | 4.36 ms |
| 50,000 | 50 | 173.73 ms | 4.48 ms | ~0.00 ms | 14.60 ms | 24.25 ms | 31.74 ms |
| 100,000 | 100 | 335.45 ms | 9.69 ms | ~0.00 ms | 32.21 ms | 41.58 ms | 76.41 ms |

Single-device mode:

| Points | Raw JSON bytes | MiB / refresh | Bytes / point | Normalize median | Summary median | Filter many matches | Filter zero matches |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1,000 | 1,510,526 | 1.44 | 1,510.5 | 2.25 ms | 0.06 ms | 0.12 ms | 0.24 ms |
| 5,000 | 7,565,503 | 7.22 | 1,513.1 | 16.43 ms | 0.29 ms | 0.64 ms | 1.29 ms |
| 10,000 | 15,134,232 | 14.43 | 1,513.4 | 35.87 ms | 0.55 ms | 1.33 ms | 2.93 ms |

Observed behavior:

- `normalizeAllDeviceRealtimeRows()` is O(P) and allocates one normalized row object per point via `Object.entries(...).map(...)`.
- `buildRealtimeSummary()` is O(P), currently implemented as `rows.filter(isGoodQuality).length`.
- `filteredRealtimeRows` no-keyword path returns the current rows reference and is effectively O(1).
- active keyword filtering is O(P) and scans `pointName`, `pointCode`, `address`, and `deviceName` fallback source.
- all-device vs single-device at the same 10k scale is close; the dominant CPU cost is point count and payload shape, not device count alone.

## 9. DOM / Render Baseline

Current rendered-row bound:

```text
Does RealtimeView currently have a bounded rendered-row count?
NO
```

Current implementation:

```vue
<tr v-for="row in filteredRealtimeRows">
```

There is no current pagination, virtualization, windowing, row limit, cursor, or summary-only mode in RealtimeView.

The table has 12 columns. Lower-bound DOM estimate, excluding `strong`, `span`, `code`, `button`, text nodes, Vue component bookkeeping, style/layout/paint cost:

| Points | Rendered rows | `<td>` lower bound |
|---:|---:|---:|
| 10,000 | 10,000 | 120,000 |
| 50,000 | 50,000 | 600,000 |
| 100,000 | 100,000 | 1,200,000 |

Boundary:

- Vitest/Node benchmark measures JSON generation/parse, normalization, summary, and filter CPU.
- It does not measure Chrome DOM render time, Vue patch time, layout, paint, or user input responsiveness.
- DOM/render requires browser profiling in a later task.

## 10. Device Lookup Conditional Hotspot

Current implementation:

```ts
function deviceDisplayName(deviceId: string): string {
  return deviceStore.devices.find((device) => device.normalizedId === deviceId)?.displayName || deviceId || "-";
}
```

Hot paths:

- filter: `row.deviceName || deviceDisplayName(...)`
- table render: `row.deviceName || deviceDisplayName(...)`

Current condition:

- `PointRealtimePayload.fromPoint()` usually includes `deviceName`.
- Therefore this is not always active.
- Correct classification: `CONDITIONAL O(P×D) HOTSPOT` when `row.deviceName` is absent.

Worst-case theoretical upper bound:

- 100k points / 100 devices
- if fallback fires for every row: `100,000 × up to 100 device comparisons ≈ 10,000,000 comparisons / pass`
- if both active filter and render fallback fire, the same class of lookup can occur more than once per refresh / user input change

Synthetic fallback benchmark, zero-match keyword:

| Points | Devices | Median |
|---:|---:|---:|
| 10,000 | 10 | 4.36 ms |
| 50,000 | 50 | 31.74 ms |
| 100,000 | 100 | 76.41 ms |

## 11. DTO Width

Current Java `PointRealtimePayload` fields: `57`.

### Realtime table required / currently used fields

The table and row actions currently use these fields or aliases:

```text
pointId
pointCode
pointName
deviceId
deviceName
dataType
address / registerAddress / pointAddress
readWrite
scalingFactor / scale / factor
value / currentValue / rawValue
unit
timestamp / collectTime / lastUpdateTime
qualityLevel / qualityDescription / quality / qualityCode / status
qualityAvailable
qualityAcceptable
processSuccess
processCostMs / processingTime / costMs / elapsedMs
```

Fields present in Java DTO and used by the current table path:

```text
pointId
pointCode
pointName
deviceId
deviceName
dataType
address
readWrite
scalingFactor
value
rawValue
unit
timestamp
lastUpdateTime
qualityLevel
qualityDescription
quality
status
qualityAvailable
qualityAcceptable
processSuccess
processingTime
```

### Detail-only / not currently required by Realtime table

```text
id
unitId
commonAddress
pointAlias
groupId
offset
deadband
minValue
maxValue
collectionMode
priority
cacheEnabled
cacheDuration
alarmEnabled
createTime
updateTime
precision
remark
additionalConfig
baseCollectionInterval
currentCollectionInterval
minCollectionInterval
maxCollectionInterval
pointChangeThreshold
stableCount
lastValue
changeRate
lastAdjustTime
processedValue
hasCachedValue
processMessage
skipped
processorName
processingTimeAvailable
metadata
```

02.1 conclusion:

- Current aggregate snapshot carries many fields that the realtime table does not need.
- Do not remove or split fields in 02.1.
- This is evidence for a later compact snapshot contract / field selection task.

## 12. Refresh Model

Current model:

```text
FULL SNAPSHOT POLLING
```

- interval: 5 seconds
- every refresh sends full aggregate snapshot
- no `since`, `version`, `cursor`, `delta`, `changedOnly`, `ETag`, `revision`
- frontend replaces `realtimeRows.value` with newly normalized rows

Browser work per full refresh:

| Area | Scale |
|---|---|
| Network | O(P), proportional to payload bytes |
| JSON parse | O(payload bytes) |
| Normalize | O(P), new row object graph |
| Filter | O(P) when keyword active |
| Summary | O(P) |
| Vue reactive replacement | O(P)-scale object graph |
| DOM patch/render | up to O(P), currently unbounded |

Five-second effect at 100k:

- raw payload: 144.63 MiB / refresh
- JSON stringify median: 1,199.98 ms
- JSON parse median: 858.59 ms
- normalize median: 335.45 ms
- active filter zero-match median: 41.58 ms
- DOM lower bound: 100k rows + 1.2m cells

## 13. Hotspot Ranking

### P0

1. Unbounded native table DOM rendering
   - Evidence: rendered rows equal `filteredRealtimeRows.length`; no row bound.
   - Scale: 100k rows and at least 1.2m `<td>` nodes before nested nodes.
   - Why it matters: payload/CPU optimizations cannot make a browser smoothly display 100k native table rows without bounding render work.
   - Recommended next task: `02.2 Frontend Realtime Render Bounding & Lookup Optimization`.

2. Full rich payload snapshot every 5 seconds
   - Evidence: synthetic representative aggregate is 144.63 MiB raw JSON at 100k, about 101.69 GiB/hour raw if refreshed every 5 seconds.
   - Scale: raw bytes per point about 1.5 KiB with current rich DTO shape.
   - Why it matters: even before DOM, serialization + parse + network payload dominate the refresh cycle.
   - Recommended next task: `02.3 Compact Realtime Snapshot Contract`.

### P1

1. JSON serialize / parse cost
   - Evidence: 100k JSON stringify median 1,199.98 ms; JSON parse median 858.59 ms in Node benchmark.
   - Scale: O(payload bytes).
   - Why it matters: consumes a large portion of a 5-second polling window before frontend logic/render.
   - Recommended next task: compact payload and later browser profiling.

2. Frontend normalization allocation
   - Evidence: 100k `normalizeAllDeviceRealtimeRows()` median 335.45 ms; creates new rows per refresh.
   - Scale: one normalized row allocation per point.
   - Why it matters: adds recurring CPU and allocation pressure after JSON parse.
   - Recommended next task: render bounding first, then normalize/row identity optimization.

3. Backend per-point DTO/runtime work
   - Evidence: current code does `snapshot + fromPoint + applyCachedValue` per point despite single bulk cache lookup.
   - Scale: ~100k payload objects and runtime snapshot accesses per 100k refresh.
   - Why it matters: backend can become CPU/allocation bottleneck after request-count N+1 has been removed.
   - Recommended next task: compact response and backend allocation profiling.

4. Conditional `deviceDisplayName()` O(P×D)
   - Evidence: fallback uses `deviceStore.devices.find(...)`; benchmark mirror is 76.41 ms at 100k/100 when `deviceName` is missing.
   - Scale: worst case about 10m device comparisons per pass.
   - Why it matters: normally masked by `deviceName`, but can regress sharply if compact DTO omits names without adding a lookup map.
   - Recommended next task: include lookup-map rule in 02.2 / 02.3.

### P2

1. Summary and active keyword filter O(P)
   - Evidence: 100k summary median 9.69 ms; active filter median 32.21–41.58 ms in Node.
   - Scale: O(P) per recompute / keyword change.
   - Why it matters: not the top bottleneck relative to DOM and payload, but user search at 100k still has visible CPU cost.
   - Recommended next task: evaluate after render bounding and compact payload.

2. Existing soak scripts are not UI realtime baseline
   - Evidence: `scripts/soak/**` targets telemetry pipeline, Redis/TDengine/MQTT/outbox/ACK soak.
   - Scale: useful for backend pipeline later, not a substitute for RealtimeView UI payload/DOM baseline.
   - Recommended next task: keep separate from UI scale benchmark.

## 14. Priority: DOM vs Payload vs Backend CPU

Priority 1: Browser DOM/render bounding

- Evidence: 100k rows implies at least 1.2m `<td>` nodes plus nested nodes, and RealtimeView has no rendered-row bound.
- Reason: without bounding DOM, 100k full table rendering is structurally unsafe regardless of backend/cache improvements.

Priority 2: Full snapshot payload / compact realtime contract

- Evidence: 100k aggregate is 144.63 MiB raw per refresh; JSON stringify median 1.20s and parse median 0.86s; 5-second raw transfer model is ~101.69 GiB/hour.
- Reason: after render is bounded, network/serialization/parse become the next largest recurring cost.

Priority 3: Backend per-point allocation and change-aware refresh

- Evidence: backend request count and cache lookup are already fixed, but still does O(P) runtime snapshot + DTO allocation + full serialize every 5 seconds.
- Reason: should be optimized after agreeing the compact/delta contract, otherwise backend optimization may preserve an oversized response shape.

## 15. Proposed Task 02 Roadmap

- 02.2: Frontend Realtime Render Bounding & Lookup Optimization
  - Bound rendered rows without changing backend contract.
  - Add device-name lookup map before any compact response can remove `deviceName` safely.
  - Preserve latest-request-wins and HTTP fallback.
- 02.3: Compact Realtime Snapshot Contract
  - Design table-focused compact payload / field selection from DTO-width evidence.
  - Keep existing rich/detail path separate and backward compatible.
- 02.4: Change-Aware Realtime Refresh
  - Evaluate `since/version/cursor/delta/changedOnly/ETag/revision` style refresh after compact contract exists.
  - Do not conflate with backend WebSocket implementation unless explicitly scoped.
- 02.5: Large-Scale Realtime Regression & Soak
  - Add repeatable browser profiling / real backend scale smoke around agreed 10k/50k/100k budgets.
  - Keep telemetry pipeline soak separate from UI realtime scale baseline.

## 16. Regression Verification

本轮只新增 benchmark-only TypeScript fixture/bench 与文档，没有修改 production Java、production Vue、production TypeScript、API contract 或运行行为。

| Check | Command | Result |
|---|---|---|
| Frontend typecheck | `npm --prefix collector-desktop run typecheck` | PASS |
| Frontend regression tests | `npm --prefix collector-desktop test` | PASS, 64 files / 427 tests |
| Frontend verify | `npm --prefix collector-desktop run verify` | PASS |
| Realtime scale benchmark | `npx vitest bench src/features/realtime/utils/realtime-scale.bench.ts --run --outputJson <output-json>` | PASS |
| Java related test | `cmd.exe /c mvn -B -ntp -DforkCount=0 -pl collector-application -am test` | PASS, 141 tests |
| Git diff check | `git diff --check` | PASS |

Benchmark output was written to a local temp file and is intentionally not committed.

## 17. Acceptance Checklist

- [x] 10k baseline measured
- [x] 50k baseline measured
- [x] 100k baseline measured
- [x] payload bytes measured
- [x] bytes / point calculated
- [x] 5-second bandwidth calculated
- [x] normalize benchmark measured
- [x] summary benchmark measured
- [x] filter cost measured and modeled
- [x] single-device scale measured
- [x] backend complexity documented
- [x] full snapshot refresh model documented
- [x] current DTO width documented
- [x] RealtimeView required field subset documented
- [x] DOM row count documented
- [x] DOM cell lower bound documented
- [x] `deviceDisplayName` conditional P×D documented
- [x] Node benchmark vs browser render boundary documented
- [x] P0/P1/P2 hotspot ranking created
- [x] Priority 1/2/3 given
- [x] no premature optimization implemented
- [x] npm test passed
- [x] npm verify passed
- [x] diff check passed
