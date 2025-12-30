# Monitor 模块使用说明

监控模块统一归口于 `com.wangbin.collector.monitor`，围绕“采集性能 / 设备状态 / 系统资源 / 缓存命中 / 异常 / 报表 / 告警”七大场景提供能力。本文档说明功能点、如何落地、以及如何与现有采集/缓存流程衔接。

## 模块定位与能力矩阵
- **性能监控**：实时统计采集速率（每秒/每分钟点数）、批量任务执行时间与成功率、设备级数据延迟、时间片调度效率。
- **设备状态监控**：覆盖设备在线/离线、响应时间、采集成功率、异常分类（连接/超时/解析等）。
- **系统资源监控**：聚合 JVM 堆/非堆内存、CPU 使用率、线程池活跃度、磁盘空间与 I/O。
- **缓存系统监控**：输出多级缓存命中率、容量、更新频率、过期统计，用于评估缓存策略。
- **异常与错误监控**：采集过程中出现的异常可按类别/设备累计，并保留最近 100 条明细。
- **统计报告与趋势分析**：`ReportScheduler` 支持实时大盘、历史趋势、设备效率排行、定期报表。
- **告警机制**：`AlertManager` 可配置阈值、级别、通知渠道（可扩展邮件/短信/Webhook），并记录告警历史。

## 主要包与职责
| 包名 | 说明 |
| --- | --- |
| `monitor.metrics` | 各类指标模型与采集服务（缓存/设备/性能/系统/异常/报表/告警骨架） |
| `monitor.health` | 健康检查聚合，生成 `HealthStatus` 返回 `/health` |
| `monitor.alert` | 告警条件、规则、级别与评估器 |
| `monitor.log` | 操作、数据日志出口（可桥接外部日志系统） |
| `monitor.config` | 监控所需的配置对象（统计周期、阈值等预留位） |

### Metric 服务对照
- `CacheMonitorService`：依赖 `MultiLevelCacheManager`，提供命中率、读写次数、分层容量、热点 Key；覆盖“缓存命中率统计 / 大小趋势 / 刷新频率”。
- `DeviceMonitorService`：扫描 `ConnectionManager` 中的连接及其 `ConnectionMetrics`，输出在线率、心跳超时、响应时间；覆盖“设备连接状态 / 响应时间 / 采集成功率 / 异常分类”。
- `PerformanceMonitorService`：基于 `CollectionManager` 里每个 `ProtocolCollector` 的 `getStatistics()` 计算处理速率、成功率、平均延迟、累计点数；对应“性能监控 / 批量任务耗时 / 设备延迟”。
- `SystemResourceMonitorService`：使用 `MemoryMXBean`、`OperatingSystemMXBean` 与线程池指标给出 JVM、CPU、磁盘、线程池状态。
- `ExceptionMonitorService`：`record(Throwable, deviceId, pointId)` 记录异常类别，维护分类统计、设备统计、最近 100 条异常列表。
- `ReportScheduler` + `ReportTask/ReportResult`：用于定期生成实时统计、历史趋势、排行、周期性报表。
- `AlertManager` + `AlertRule/AlertCondition`：定义阈值与通知级别，`evaluate(metric, value)` 返回触发结果，后续可集成通知渠道。

## REST API
`MonitorController` 将监控能力统一暴露：

| 接口 | 说明 | 典型用途 |
| --- | --- | --- |
| `GET /health` | 健康检查 | 探活、K8s/LB 就绪检查 |
| `GET /monitor/cache` | 缓存指标快照 | 命中率、容量、过期分析 |
| `GET /monitor/devices` | 设备与连接指标 | 在线/离线、响应时间、异常分类 |
| `GET /monitor/performance` | 采集性能列表 | 采集速率、批量成功率、平均延迟 |
| `GET /monitor/system` | JVM/系统资源 | CPU、内存、线程池、磁盘 I/O |
| `GET /monitor/errors` | 异常统计 | 分类计数、设备分布、最近异常 |

所有返回值均为 UTF-8 JSON，可直接对接前端大屏或二次封装为 Prometheus exporter。

## 与现有功能的衔接
1. **异常监控接入**  
   - `BaseCollector`、`ConnectionManager` 已注入 `ExceptionMonitorService`，在 `connect/readPoint/writePoint` 等 catch 块调用 `record`，可精准定位异常类别与设备。  
   - 扩展其他链路时，直接注入该服务并在异常处调用 `record(e, deviceId, pointId)` 即可。

2. **缓存与数据质量**  
   - `CollectorDataCacheAspect` 会把 `ProtocolCollector.readPoint(s)` 的 `ProcessResult` 写入多级缓存。  
   - `DataController` 在返回 API 时自动展开 `ProcessResult`，提供 `value/rawValue/processedValue/quality/metadata`，方便前端或告警逻辑直接使用采集值与质量指标。

3. **性能指标来源**  
   - 各采集器通过 `BaseCollector.getStatistics()` 暴露累计读写次数、耗时、错误数，`PerformanceMonitorService` 会换算速率与成功率。  
   - 若新增调度器（例如时间片调度），可在内部补充统计字段或调用 Performance 服务扩展模型。

4. **告警与报表联动**  
   - 在 `ReportScheduler` 中可注册“设备采集效率排行榜”“历史趋势分析”等任务，生成 `ReportResult` 后再调用 `AlertManager` 对比阈值触发告警。  
   - 告警级别 (`AlertLevel`) 支持信息/警告/错误/严重，可按需要扩展到日志、邮件、Webhook 等渠道。

## 使用建议
1. **查看实时状态**：部署后访问 `/monitor/performance`、`/monitor/devices`、`/monitor/system`，即可快速了解数据采集、连接、系统健康状况。  
2. **排查异常**：遇到采集失败，可先查 `/monitor/errors` 获取最近异常，再定位到具体设备/点位。  
3. **配置阈值**：结合 `AlertManager`，针对“采集成功率低于阈值”“延迟超过阈值”“缓存命中率过低”等场景注册告警规则。  
4. **定制报表**：在 `monitor.metrics.report` 包下新增 `ReportTask` 实现统计逻辑，注册到 `ReportScheduler` 即可按日/周/月自动生成报表。  
5. **扩展出口**：若要落地 Prometheus/InfluxDB，可在各 Service 中增加 exporter，将快照写入对应后端；或在 `monitor.log` 中统一输出结构化日志。

通过以上方案，监控模块已经与采集、缓存、连接管理等核心流程完成对接，可直接支撑性能监控、异常追踪、告警、报表等业务需求。如需进一步扩展，只需在现有 Service/Model 基础上追加字段或数据出口即可。
