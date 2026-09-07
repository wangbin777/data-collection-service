# Real Backend Smoke

本文件记录 `Frontend Production Readiness / Task 01.7 Real Backend Smoke & Regression` 的真实后端 smoke。该 smoke 必须启动最终打包出的 Spring Boot executable JAR，并通过真实 HTTP socket 验证控制台集成链路。

## 目的

验证以下真实链路可用：

```text
build Vue
  -> sync static assets
  -> package collector-boot executable jar
  -> start real java -jar
  -> real TCP HTTP requests
  -> auth / static console / dashboard datasource / local temporary device / realtime contracts
  -> cleanup
  -> shutdown owned Java process
```

该 smoke 不用 `MockMvc`、`Mockito`、`Vitest mock` 或只读 Controller 代码替代真实 HTTP 验证。

## 测试边界

本 smoke 验证真实 Spring Boot / Controller / Auth / Config / Realtime / Monitor / Static Web integration。为保证开发机可重复执行，默认不依赖真实 TDengine、Redis、MQTT Cloud 或真实 PLC。

启动 JAR 时通过临时命令行参数关闭外部依赖：

```text
--spring.profiles.active=dev
--server.port=19090
--telemetry.tdengine.enabled=false
--collector.report.enabled=false
--collector.report.mqtt.enabled=false
--collector.report.shadow.persistence-enabled=false
--collector.alarm.state.enabled=false
--collector.cache.type=local
--spring.data.redis.stream.enabled=false
--collector.config.loader=file
```

这些覆盖只存在于 smoke 进程，不修改 `application.yml`。

## 如何构建

在仓库根目录执行：

```bash
npm --prefix collector-desktop run typecheck
npm --prefix collector-desktop test
npm --prefix collector-desktop run build:web
cmd.exe /c mvn -B -ntp -pl collector-boot -am clean package -DskipTests
```

非 Windows Git Bash 环境可直接执行 Maven：

```bash
mvn -B -ntp -pl collector-boot -am clean package -DskipTests
```

产物必须是：

```text
collector-boot/target/data-collection-service-0.0.1-SNAPSHOT.jar
```

不要启动 `original-*.jar` 或 `*.original`。

## 如何运行

Windows PowerShell：

```powershell
powershell.exe -ExecutionPolicy Bypass -File scripts/run-console-real-smoke.ps1 \
  -JarPath collector-boot/target/data-collection-service-0.0.1-SNAPSHOT.jar
```

默认端口为 `19090`，用于避免误伤开发者已经运行的 `9090` 实例。可通过参数覆盖：

```powershell
powershell.exe -ExecutionPolicy Bypass -File scripts/run-console-real-smoke.ps1 \
  -JarPath collector-boot/target/data-collection-service-0.0.1-SNAPSHOT.jar \
  -Port 19091 \
  -Token ops-token \
  -StartupTimeoutSeconds 90
```

需要人工排查时可追加 `-KeepServer`，否则脚本结束时会关闭自己启动的 Java 进程。

## 启动参数

| 参数 | 默认值 | 说明 |
|---|---:|---|
| `JarPath` | `collector-boot/target/data-collection-service-0.0.1-SNAPSHOT.jar` | 已构建的 Spring Boot executable JAR。 |
| `Port` | `19090` | smoke 使用的 HTTP 端口。启动前端口被占用会 fail fast。 |
| `Token` | `ops-token` | dev profile 的运维 token。不要使用生产 token。 |
| `StartupTimeoutSeconds` | `90` | 轮询 `/collector/health` 的最大等待秒数。 |
| `KeepServer` | `false` | 保留本次脚本启动的 Java 进程用于人工排查。 |

## 鉴权 token

脚本只使用 dev profile 的 `ops-token`，并验证：

- 不带 `X-Collector-Token` 访问受保护 API 返回 `401`。
- `invalid-smoke-token` 返回 `401`。
- `ops-token` 可以访问业务 API 和 `/monitor/runtime`。

不得在脚本、文档、日志或报告中加入生产 token、真实密码、Redis 密码、TDengine 密码或云端密钥。

## Mandatory checks

脚本必须验证：

- executable JAR 存在，且不是 `original-*.jar` / `*.original`。
- 启动前 smoke 端口未被占用；端口占用时输出 `Smoke port already in use` 并退出 `1`。
- `java -jar` 真实启动，并通过 HTTP 轮询 `/collector/health` 判定 ready。
- `/collector/health` 不带 token 可访问且返回 JSON。
- `/collector/actuator/health` 存在且返回 actuator JSON。
- `/collector/desktop/index.html` 返回 HTML。
- 从 index 实际解析至少一个 JS asset 和一个 CSS asset，并真实 GET 成功。
- no-token / invalid-token 鉴权边界返回 `401`。
- valid token 可以访问 `/collector/api/data/realtime` 和 `/collector/monitor/runtime`。
- `GET /collector/api/config/devices` 为 `ApiResult` envelope，包含 `code=200` 和 `data`。
- `GET /collector/api/data/realtime` 为 RAW aggregate DTO，不是嵌套在 `ApiResult.data` 中。
- Dashboard datasource 路由真实存在并返回 JSON。
- 创建固定 ID 的本地临时设备 `smoke-local-http-01`，且 `startAfterSave=false`。
- 读取本地临时设备、设备列表、点位配置。
- 验证单设备实时、单点实时、全设备聚合实时、设备摘要。
- 验证本次 aggregate smoke 只发送 `1 × GET /api/data/realtime`，不通过脚本对每台设备补 N 次 realtime 请求。
- finally 中只清理 `smoke-local-http-01`，并验证聚合结果不再包含该设备。
- 默认关闭本脚本自己启动的 Java PID。

## Expected degraded checks

在 `telemetry.tdengine.enabled=false` 时，历史/告警/存储相关 endpoint 可能返回业务级 `status=disabled` 或明确的 unavailable snapshot。脚本会将这类可解析 JSON 响应标记为 `[DEGRADED]`，而不是伪装成数据健康。

以下情况不能降级通过：

- 路由 `404`。
- 鉴权 `401/403`。
- 未处理 `500`。
- HTML 错误页。
- JSON contract 破坏。

## Temporary device strategy

脚本创建固定本地临时设备：

```text
deviceId: smoke-local-http-01
protocolType: HTTP
connectionType: HTTP
host: 127.0.0.1
port: 9
url: http://127.0.0.1:9/smoke
startAfterSave: false
collectionMode: MANUAL
```

该设备不会启动采集，不会连接真实 PLC，也不会访问用户设备。

## Cleanup strategy

脚本只删除：

```text
smoke-local-http-01
```

不会执行 bulk delete、clear all configs、reload all devices，也不会停止未知 Java 进程。清理和进程停止均放在 `try/finally` 之后的收尾路径中；脚本只停止自己 `Start-Process` 创建的 Java PID。

## 日志

后端 stdout / stderr 写入：

```text
collector-boot/target/real-smoke/backend-stdout.log
collector-boot/target/real-smoke/backend-stderr.log
```

该目录位于 Maven `target/` 下，不提交 Git。

## 退出码

- 全部 mandatory assertion 通过：输出 `REAL BACKEND SMOKE PASSED`，退出码 `0`。
- 任一 mandatory assertion 失败：输出 `REAL BACKEND SMOKE FAILED`，退出码 `1`。

## Task 01.7 真实执行结果

执行时间：2026-09-04 16:40:08 +0800 附近。

已验证命令：

```bash
npm --prefix collector-desktop run typecheck
npm --prefix collector-desktop test
npm --prefix collector-desktop run build:web
npm --prefix collector-desktop run verify
cmd.exe /c mvn -B -ntp -pl collector-boot -am clean package -DskipTests
cmd.exe /c mvn -B -ntp -pl collector-boot -am -Pp0-regression test
node scripts/scan-config-secrets.mjs
git diff --check
powershell.exe -NoProfile -ExecutionPolicy Bypass -File scripts/run-console-real-smoke.ps1 -JarPath collector-boot/target/data-collection-service-0.0.1-SNAPSHOT.jar -Port 19090 -Token ops-token -StartupTimeoutSeconds 90
```

真实 smoke 结果：

- `REAL BACKEND SMOKE PASSED`
- 启动 PID：`30516`
- 端口：`19090`
- context path：`/collector`
- `/collector/health`：HTTP 200，JSON 可解析，当前 status 为 `DOWN`。
- `/collector/actuator/health`：HTTP 200，Actuator JSON，status 为 `UP`。
- `/collector/desktop/index.html`：HTTP 200 HTML。
- JS asset：HTTP 200，body 非空。
- CSS asset：HTTP 200，body 非空。
- no-token protected API：HTTP 401。
- invalid token：HTTP 401。
- valid `ops-token`：进入业务接口，RAW aggregate DTO HTTP 200。
- Dashboard 数据源：配置、运行态、上报、系统、缓存、性能均 HTTP 200 JSON。
- 预期降级：`dashboard recent alarms` 返回 TDengine 告警历史存储未启用；`dashboard storage` 返回 TDengine 未启用。
- 临时设备：`smoke-local-http-01` 创建成功，`startAfterSave=false`，未启动设备。
- 单设备实时、单点实时、全设备聚合和设备摘要均通过真实 HTTP 验证。
- aggregate access log：一次验证请求只产生 `1 × GET /api/data/realtime`，脚本未对每台设备发 N 次 aggregate 验证请求。
- 清理：只删除 `smoke-local-http-01`，readback 不再返回成功设备，aggregate after cleanup 不再包含该设备。
- 后端进程：脚本停止自己启动的 Java PID，端口仅剩 `TIME_WAIT`，无 `LISTENING` 残留。
- 端口占用：已用本地临时 listener 验证端口占用时输出 `Smoke port already in use`，exit code 为 `1`，且不会启动后端进程。

本轮 smoke 暴露并修复了两个问题：

1. `ApiResult.statusSuccess(...)` 未设置 `code=200`，导致真实 `/api/config/devices` envelope contract 与前端 `requestApiData<T>()` 期望不一致。
2. smoke 脚本初版在 PowerShell 单元素 pipeline 上读取 `.Count` 失败，且 catch 分支可能输出失败后仍因 failure count 为 0 返回成功；已改为数组包裹计数并在 catch 中记录 mandatory failure。
