# Monitor 模块使用说明

本模块提供健康检查、缓存/设备/性能/系统/异常监控，以及报表与告警的骨架能力。以下按功能说明主要入口与使用方式。

## 健康检查
- `monitor.health.SystemHealthService` 聚合多级缓存与连接管理器健康信息。
- `GET /health`（`api.controller.HealthController`）返回结构化 `HealthStatus`，包括各组件状态与详细数据。

## 缓存监控
- 数据模型：`CacheMetricsSnapshot`。
- 服务：`CacheMonitorService` 调用 `MultiLevelCacheManager.getStatistics()` 获取读写次数、命中率、分层统计。
- REST：`GET /monitor/cache`（`MonitorController`）。

## 设备连接监控
- 数据模型：`DeviceConnectionSnapshot`、`DeviceStatusSnapshot`。
- 服务：`DeviceMonitorService` 读取 `ConnectionManager` 的所有连接，统计在线/离线与 `ConnectionMetrics`。
- REST：`GET /monitor/devices`。

## 采集性能监控
- 数据模型：`CollectorMetrics`、`PerformanceMetrics`。
- 服务：`PerformanceMonitorService` 从 `CollectionManager` 获取每个 `ProtocolCollector` 的 `getStatistics()` 结果，输出处理点数、速率、成功率、平均延迟。
- REST：`GET /monitor/performance`。

## 系统资源监控
- 数据模型：`SystemResourceSnapshot`，包含堆/非堆内存、CPU、线程、线程池状态。
- 服务：`SystemResourceMonitorService` 基于 JMX、`ThreadPoolTaskExecutor` 获取指标。
- REST：`GET /monitor/system`。

## 异常监控
- 数据模型：`ExceptionStatsSnapshot`、`ExceptionSummary`。
- 服务：`ExceptionMonitorService` 提供 `record(Throwable, deviceId, pointId)` 记录异常，内部统计总数/分类/最近列表，`getStats()` 返回快照。
- REST：`GET /monitor/errors`。
- 使用建议：在采集流程 catch 块中调用 `ExceptionMonitorService.record(...)`。

## 统计报表
- 模型：`ReportTask`、`ReportResult`。
- 调度器：`ReportScheduler` 提供 `registerTask` 与 `@Scheduled` 轮询逻辑，后续可在此实现实际报表生成/存储。

## 告警框架
- 模型：`AlertLevel`、`AlertCondition`、`AlertRule`。
- 管理器：`AlertManager` 支持注册/删除规则，`evaluate(metric, value)` 检查阈值并输出日志，可扩展通知渠道。

## 接入指南
1. **接入异常记录**：在关键 catch 块注入 `ExceptionMonitorService`，调用 `record`。
2. **扩展指标**：`PerformanceMonitorService`、`SystemResourceMonitorService` 可按需增加字段或数据来源。
3. **自定义报表**：调用 `ReportScheduler.registerTask` 注册任务，然后在 `schedule()` 中实现实际统计逻辑和结果持久化。
4. **告警通知**：在评估逻辑中对接邮件、短信或 Webhook；也可将 `AlertManager` 与指标服务联动，实现阈值触发。

## REST API 总览
| 接口 | 描述 |
| --- | --- |
| `GET /health` | 系统健康状态 |
| `GET /monitor/cache` | 缓存指标 |
| `GET /monitor/devices` | 设备/连接指标 |
| `GET /monitor/performance` | 采集性能指标 |
| `GET /monitor/system` | JVM/系统资源指标 |
| `GET /monitor/errors` | 异常统计 |

后续如需导出 Prometheus 指标、接入通知渠道或实现具体报表，可在现有服务的基础上扩展。
