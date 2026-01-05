data-collection-service
Generated skeleton project (Java 17, Spring Boot).
Run:
  mvn -U clean package
  java -jar target/data-collection-service-0.0.1-SNAPSHOT.jar


- 查询指定设备的指定数据点实时值 ：

```
GET /api/data/device/{deviceId}/point/
{pointId}
```
示例： GET /api/data/device/device-001/point/temperature
- 查询指定设备的所有数据点实时值 ：

```
GET /api/data/device/{deviceId}
```
支持可选参数 pointIds 筛选特定数据点：

```
GET /api/data/device/device-001?
pointIds=temperature,humidity
```
- 查询所有设备基本信息 ：

```
GET /api/data/devices
```
- 查询指定设备的所有数据点配置 ：

```
GET /api/data/device/{deviceId}/points
```



- 在 DataPoint 增加上报/变化/事件配置解析（src/main/java/com/wangbin/collector/common/domain/entity/DataPoint.java:344 起），支持通过 additionalConfig 配置 reportEnabled、changeThreshold、changeMinIntervalMs、eventEnabled、   
  eventMinIntervalMs，并继续遵循“无别名=只保留 raw”的约束。
- ReportProperties 新增调度与限流参数（src/main/java/com/wangbin/collector/core/report/config/ReportProperties.java:56 起），包括默认 10s 周期、最小变化触发间隔、事件默认间隔、网关 MPS、单包字段/字节限制与 schemaVersion，便于
  统一调优。
- DeviceShadow/ShadowManager 扩展为完整的触发引擎：影子保存最后上报值/变化/事件时间（src/main/java/com/wangbin/collector/core/report/shadow/DeviceShadow.java:17-107），ShadowManager 在 apply 中同时计算“变化触发 + 事件触发”并返
  回 ShadowUpdateResult（src/main/java/com/wangbin/collector/core/report/shadow/ShadowManager.java:31）；事件支持 AlarmRule、metadata 标记及质量退化三种来源，并套用最小间隔（ShadowManager.java:90 起）。
- CacheReportService 彻底重写：定时快照 + 变化立即刷新 + 事件即时上报，并附带网关限流（快照受限、事件不受限），同时在所有 chunk 上报后回写影子状态（src/main/java/com/wangbin/collector/core/report/service/                      
  CacheReportService.java:1-309）。事件上报使用 MessageConstant.MESSAGE_TYPE_EVENT_POST，快照/事件都打上 schemaVersion、seq、batchId/chunk*，符合 200 字段与 128KB 双阈值。
- 事件/变化默认行为说明：变化触发仅在点位配置 changeThreshold 时生效；事件由 alarmRule、metadata.event*、或质量跌至 WARNING 以下触发，并可借 eventMinIntervalMs 限制频率。

配置与使用要点

- 要启用模式 B（变化触发），为点位 additionalConfig 设置：{"changeThreshold":5.0,"changeMinIntervalMs":3000} 等；模式 C（事件）可沿用 alarmRule，或在处理链中写入 metadata.eventTriggered=true/eventType。
- 全局上报节奏参数集中在 collector.report.*：默认 interval=10000、minReportIntervalMs=2000、eventMinIntervalMs=5000、maxGatewayMessagesPerSecond=200、maxPropertiesPerMessage=200、maxPayloadBytes=131072，可按设备规模调优。
- 仍需确保点位有 pointAlias 才能进设备快照；否则只保留 raw（但事件仍可触发，上报字段取 pointCode/pointId）。

测试

- mvn -q -DskipTests "-Dspring-boot.repackage.skip=true" package

若后续需更多告警元信息或云端 schema，对应可在 ShadowManager.EventInfo 和 CacheReportService.dispatchEvent 中扩展。



## 优化

- DataPoint 现在直接解析 reportEnabled/changeThreshold/changeMinIntervalMs/eventEnabled/eventMinIntervalMs，并继续以 pointAlias 作为上报字段判定，保证在配置层就能筛选“入快照/变化/事件”点（src/main/java/com/wangbin/collector/  
  common/domain/entity/DataPoint.java:344）。
  - ReportData 增强为属性映射 + 时间戳双 Map，并在 addProperty 时防止重复字段；新增 Logger 后若重复字段会记录并跳过，同时 chunk 仍携带 propertyTs、quality、batchId/seq/chunk* 元数据（src/main/java/com/wangbin/collector/core/    
    report/model/ReportData.java:20-198）。
  - 影子层重构：DeviceShadow 维护最新值、上次上报时间戳、变化/事件触发时间及事件签名；ShadowManager 在 apply 中同步计算“变化触发 + 事件触发”，事件去重按 eventType + message hash 且受 eventMinIntervalMs 控制，满足“同类型事件窗口
    内最多 1 次”的约束（src/main/java/com/wangbin/collector/core/report/shadow 包）。
  - CacheReportService 实现三种模式并加入限流/去重/按字段回写：定时快照（切包后异步上报）、变化触发立即刷、事件触发即时单独发送；chunk 上报采用 FlushTracker 跟踪成功/失败，只有成功的字段才回写，失败字段保留等待下一轮，且整包仅在
    所有 chunk 成功时才清脏；快照上报受 maxPropertiesPerMessage、maxPayloadBytes、maxGatewayMessagesPerSecond、minReportIntervalMs 约束，事件通道不受限（src/main/java/com/wangbin/collector/core/report/service/                   
    CacheReportService.java:1-330）。
  - 事件上报使用 MessageConstant.MESSAGE_TYPE_EVENT_POST，并携带规则 ID/名称、等级、别名等上下文；变化/事件的最小间隔可 per-point 配置，默认值来自 collector.report.*。ReportProperties 新增 minReportIntervalMs、                  
    eventMinIntervalMs、maxGatewayMessagesPerSecond、maxPropertiesPerMessage、maxPayloadBytes、schemaVersion 等开箱参数（src/main/java/com/wangbin/collector/core/report/config/ReportProperties.java:56-86）。

  测试

  - mvn -q -DskipTests "-Dspring-boot.repackage.skip=true" package

  如需后续拓展（例如更多事件字段、失败重试策略或云端回执）可在 FlushTracker/dispatchEvent 基础上继续增强。



##  优化 
• 当前实现还缺少/不满足的点：

- DataPoint 仍旧以 “只要 alias 非空就默认 reportEnabled=true”，没有落实“reportEnabled 默认 false、必须显式开启”以及 reportField 字段。也没有处理“无别名即 raw-only”之外的“reportEnabled=false 时即便有别名也不进快照”的逻辑。
- FieldUniquenessValidator 尚未接入/未对 reportField 冲突做降级处理，ReportData.addProperty() 虽防止覆盖，但遇到冲突只是日志 warn，没有将冲突点降级为 raw-only。
- 切包 splitSnapshot 虽同步字段，但没有在发送失败时保留 chunk 以便下一轮重试；flush 回写只基于成功 chunk，但失败 chunk 没有复位 shadow 状态或触发下次发送。
- 事件防风暴仅通过 eventType+message 判断，未实现“事件内容 hash 去重 + eventMinIntervalMs 内最多 1 次”完整规则，也未将 eventMinIntervalMs 应用于 metadata 驱动的事件。
- 上述功能缺乏单元测试；要求的 DataPoint/reportField、变化触发、事件触发、防风暴、切包同步、字段冲突等测试尚未编写。




## 优化 
- src/main/java/com/wangbin/collector/common/domain/entity/DataPoint.java:376-426 现集中解析 reportEnabled/reportField/change/event 配置，getReportField() 先取显式字段再回退到别名，无别名则强制 raw-only；isReportEnabled() 只有
  在标记为 true 且字段不冲突时才返回 true。
  - src/main/java/com/wangbin/collector/core/report/config/ReportProperties.java:36-88 补齐调度与限流参数（intervalMs、minReportIntervalMs、maxGatewayMessagesPerSecond、maxPropertiesPerMessage、maxPayloadBytes、schemaVersion），
    供聚合/限流逻辑统一读取。
  - src/main/java/com/wangbin/collector/core/report/service/CacheReportService.java:204-333,267-322,489-522 重写切包与发送流程：splitSnapshot 恒加 chunk 元数据，payload/字段双阈值切分；发送前登记 chunk key，handleChunkResult 失
    败按设备重试并仅对成功 chunk 回写影子；FlushTracker 跟踪 in-flight 数和每 chunk 的重试次数。
  - src/main/java/com/wangbin/collector/core/report/validator/FieldUniquenessValidator.java:18-41/src/main/java/com/wangbin/collector/core/report/shadow/ShadowManager.java:48-146 保证设备内 reportField 唯一并在影子层实现变化阈值
    与事件防风暴（最小间隔 + eventType+内容指纹去重）。
  - 新增单元测试覆盖关键路径：DataPointTest 验证字段解析(src/test/java/com/wangbin/collector/common/domain/entity/DataPointTest.java:13)、FieldUniquenessValidatorTest 验证冲突降级(…/validator/                                    
    FieldUniquenessValidatorTest.java:26)、ShadowManagerTest 覆盖变化触发与事件节流(…/shadow/ShadowManagerTest.java:27,59)、CacheReportServiceTest 校验切包时属性/质量/时间戳同步(…/service/CacheReportServiceTest.java:15)。

  测试

  - mvn -q test

  未完成/注意

  - CacheReportService.java:204-248 中 maxPayloadBytes 仍按字段名+值字符串长度粗估 MQTT 载荷。如果需要严格的 UTF-8 JSON 字节级校验，后续需替换为真实序列化测算。

  后续可按需在实际点位 additionalConfig 中补齐 reportEnabled/reportField，并观察 128KB 限制是否需要更精确的 payload 计算。       



## 上报配置
• - 代码现已具备设备级聚合/变化/事件三类上报、字段唯一校验、切包与限流等逻辑，但默认依赖 ReportProperties 与点位 additionalConfig。若 collector.report.enabled=true 且 MQTT 配置正确，容器启动后即可按 10s 周期发快照（变化/事件按规
则触发），无需再单点实时推送。
- 启用步骤：① 在点位配置里设置 additionalConfig，至少包含 reportEnabled=true 与 pointAlias（或显式 reportField），缺少别名的点仍只写 raw 数据；② 按需设置 changeThreshold、changeMinIntervalMs、eventEnabled 等参数；③ 在         
  application.yml 中填好 collector.report.mqtt.*（brokerUrl/clientId/topic/qos 等）；④ 需要调整周期/限流可在同一配置项修改 intervalMs、maxPropertiesPerMessage、maxPayloadBytes 等。
- 配置完成后重启网关即可开始汇报。若要验证，可观察日志中 CacheReportService/MqttReportHandler 是否输出发送记录。



## 上报问题
shadowManager.apply(...) 会在三种情况下返回 eventInfo：

1. 点位自身告警规则命中：DataPoint.alarmRule 要有启用的规则，且 processResult.getFinalValue() 触发了 rule.checkAlarm()。
2. ProcessResult.metadata 主动标记事件：采集或处理链路把 metadata.put("eventTriggered", true)（或至少 metadata.put("eventType", "...")）写进去，并附带 eventLevel/eventMessage 等信息。
3. 质量降级：processResult.getQuality() 低于 QualityEnum.WARNING（例如 BAD / NOT_CONNECTED 等），就会自动生成 eventType=QUALITY 的事件。

除此之外，还要满足以下约束，否则即便触发条件成立也会过滤掉：

- DataPoint.isEventReportingEnabled() 必须为 true。默认如果 additionalConfig 没有 eventEnabled=false，就视为开启；但有些点位如果配置了 eventEnabled=false 或 alarmEnabled=0，事件会被整体关闭。
- 同一类型事件在 eventMinIntervalMs 内只允许出现一次。默认 5000ms，可通过 additionalConfig.eventMinIntervalMs（或 ReportProperties.eventMinIntervalMs 全局默认）调整。如果你频繁模拟同一事件，可能因为节流/去重被忽略。
- 事件指纹去重：eventType + message/ruleId/ruleName 在 eventMinIntervalMs 内重复出现也会被判定为重复。

因此，如果你每次都拿不到 eventInfo，请检查：

1. 点位的 additionalConfig 是否包含 eventEnabled=false 或 eventMinIntervalMs 过大；
2. 当前 ProcessResult 有没有 metadata 标记或质量降级；
3. 是否在上一条触发后尚未超过 eventMinIntervalMs（可以暂时把 interval 设为很小验证）；
4. 日志中是否出现 FieldUniquenessValidator 的冲突或其他降级提示——被降级的点虽然还能采集，但影子里不会做变化/事件判断。

如果你需要通过代码主动触发事件，可以在采集结果里加上：

ProcessResult result = new ProcessResult();
result.addMetadata("eventTriggered", true);
result.addMetadata("eventType", "CUSTOM_EVENT");
result.addMetadata("eventLevel", "WARNING");
result.addMetadata("eventMessage", "test");
