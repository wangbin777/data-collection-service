# 4A 真实环境 Soak 基线

本目录只提供 4A 长稳容量基线入口，不进入默认 `mvn test`。采集源使用测试侧模拟点值，但数据会进入真实 Spring Boot Context、真实 `TelemetryPostProcessPipeline`、Redis、TDengine V2 和 MQTT Cloud Outbox/ACK 链路。

## 本地依赖

优先使用已启动的本机 Redis、TDengine 和 MQTT Broker。需要临时环境时可执行：

```powershell
docker compose -f docker-compose-soak.yml up -d redis-soak tdengine-soak mqtt-soak
```

该 compose 只用于测试环境，不会删除数据卷。Redis 默认无密码；如果本机 Redis 有密码，运行脚本时传入 `-RedisPassword`。

## 快速 Smoke

```powershell
./scripts/soak/run-soak.ps1 `
  -Points 1000 `
  -Devices 5 `
  -DurationSeconds 120 `
  -CollectionIntervalMs 1000 `
  -RedisPassword ""
```

## 10k/50k/100k 基线

```powershell
./scripts/soak/run-soak.ps1 -Points 10000 -Devices 10 -DurationSeconds 1800 -RedisPassword ""
./scripts/soak/run-soak.ps1 -Points 50000 -Devices 50 -DurationSeconds 7200 -RedisPassword ""
./scripts/soak/run-soak.ps1 -Points 100000 -Devices 100 -DurationSeconds 7200 -RedisPassword ""
```

脚本参数支持：

- `Points`：点位总数。
- `Devices`：设备数；为 0 时按每 1000 点一个设备估算。
- `DurationSeconds`：运行时长，手工 Soak 可设置为 28800 或 86400。
- `CollectionIntervalMs`：每轮模拟采集间隔。
- `SpreadWithinInterval`：是否把单轮点位提交均匀摊开，默认开启，避免把容量基线误测成瞬时突发压测。
- `IngressMode`：入口模式，`point` 表示逐点进入 `TelemetryIngressService`，`batch` 表示按设备批次进入 `CollectorDataPostProcessor.saveBatchAsync`。
- `Scenario`：记录场景名，支持配合容器控制做 `redis-outage`、`tdengine-outage`、`cloud-outage`、`redis-tdengine-outage`、`tdengine-cloud-outage`、`triple-outage`。
- `MetricsOutput`：结果目录，默认 `target/soak-results/<timestamp>`。

## 真实故障注入

脚本默认不会自动停止任何服务。只有明确传入 `-AllowServiceControl` 时，才会按容器名在本机测试环境中停止和恢复依赖：

```powershell
./scripts/soak/run-soak.ps1 `
  -Points 10000 `
  -Devices 10 `
  -DurationSeconds 600 `
  -Scenario redis-tdengine-outage `
  -OutageStartSeconds 120 `
  -OutageDurationSeconds 180 `
  -RedisPassword "" `
  -AllowServiceControl
```

不要把该模式指向生产环境。容器名默认是 `data-collection-redis`、`data-collection-tdengine`、`data-collection-mqtt`，可以通过脚本参数覆盖。

## 输出

每次运行会生成：

- `run-info.json`：Git、JDK、JVM、环境版本和运行参数。
- `metrics.csv`：周期采样的 CPU、Heap、GC、线程、线程池、Redis、History、Cloud Outbox/ACK 指标。
- `summary.json`：最终吞吐、P50/P95/P99、队列峰值、Backlog 和容量估算。

结果目录位于 `target/soak-results`，不应提交到 Git。
