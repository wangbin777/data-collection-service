# 物联网数据采集服务

面向物联网与工业数据采集场景的高性能采集服务，基于 **Spring Boot 3.x + Java 17** 构建，内置 **配置下发、采集调度、数据处理、缓存、纵向转横向聚合、事件/告警、云平台上报以及监控观测** 的完整链路。

本项目可直接用于 **工业现场私有化部署**，也可作为 **工业物联网采集网关底座** 进行二次开发。
目前仅支持数据采集，不支持数据下发


---

## 一、项目背景与目标

### 1. 背景
在工业物联网场景中，设备侧通常以 **点位为单位** 产生大量纵向数据，而云平台与时序数据库更适合接收 **按时间聚合的横向数据结构**。  
同时，现场存在多协议、多厂家、多点位规模化采集的复杂需求。

### 2. 目标
- 统一多协议采集模型
- 高性能、可扩展的采集调度能力
- 将纵向点位数据转换为横向时间片数据
- 稳定、可控地向云平台上报
- 内置监控、告警与健康检查能力

---

## 二、核心能力概览

- 多协议采集（Modbus / OPC UA / IEC104 / MQTT 等）
- 纵向 → 横向数据聚合
- 单连接多设备云上报模型
- 自适应采集调度与限流
- 多级缓存 + 设备影子
- 事件 / 告警 / 变化检测
- 全链路运行监控与健康检查

---

## 三、支持协议

| 协议 | 支持情况 |
|---|---|
| MODBUS_TCP | ✅ 已支持 |
| MODBUS_RTU | ✅ 已支持 |
| OPC_UA | ✅ 已支持 |
| MQTT | ✅ 已支持 |
| IEC104 | ✅ 已支持 |
| OPC_DA | ⏸ 暂不支持 |
| SNMP | ⚠️ 未充分验证 |
| COAP | ⚠️ 未充分验证 |
| IEC61850 | ⚠️ 未充分验证 |

---

## 四、纵向 → 横向数据转换机制（核心设计）

### 1. 纵向数据示例

```text
ts=1234566789 , point=ua , val=1.0
ts=1234566789 , point=ub , val=2.0
ts=1234566789 , point=uc , val=3.0
```

### 2. 横向数据结果

```json
{
  "ts": 1234566789,
  "ua": 1.0,
  "ub": 2.0,
  "uc": 3.0
}
```

### 3. 聚合窗口策略

- 默认按时间片对齐  
  `windowTs = ts - (ts % windowSize)`
- 同一设备 + 同一窗口的数据合并为一条记录

### 4. 点位字段映射

| 配置字段 | 说明 |
|---|---|
| pointId | 采集点唯一标识 |
| reportField | 横向字段名 |
| reportEnabled | 是否参与聚合 |

### 5. 缺失点位处理

| 策略 | 行为 |
|---|---|
| SKIP | 忽略缺失字段 |
| LAST | 使用上一次缓存值 |
| NULL | 明确写为 null |

---

## 五、云平台上报模型

### 1. 单 MQTT 连接多设备

- 网关仅维护一个 MQTT 连接
- payload 中携带逻辑 deviceId
- 云端根据 deviceId 路由

### 2. Topic 模板

```text
/iot/{productKey}/{deviceName}/properties/report
```

### 3. Payload 示例

```json
{
  "deviceId": "DEVICE_001",
  "ts": 1234566789,
  "properties": {
    "ua": 1.0,
    "ub": 2.0,
    "uc": 3.0
  }
}
```

### 4. ACK 与重试

- ACK 超时自动重试
- 支持最大重试次数
- 支持指数退避

---

## 六、主要模块说明

### 1. 配置与设备管理
- ConfigSyncService
- ConfigManager
- ConfigUpdateEvent
- FieldUniquenessValidator

### 2. 采集调度
- CollectionScheduler
- AdaptiveCollectionUtil
- PerformanceStatsSnapshot

### 3. 数据处理与缓存
- Processor Pipeline
- MultiLevelCacheManager
- DeviceShadow / ShadowManager

### 4. 上报、事件与告警
- CacheReportService
- ReportProperties
- Alert / Notification

### 5. 监控与健康
- MonitorController
- HealthController

---

## 七、API 接口概览

| 控制器 | 路径 | 说明 |
|---|---|---|
| DataController | /api/data/device/{deviceId} | 查询设备数据 |
| DataController | /api/data/device/{deviceId}/point/{pointId} | 查询点位 |
| DeviceController | /api/device/{deviceId}/start | 启动采集 |
| DeviceController | /api/device/{deviceId}/stop | 停止采集 |
| MonitorController | /monitor/** | 系统监控 |
| HealthController | /health | 探活 |

---

## 八、目录结构

```text
src/main/java/com/wangbin/collector
├── api
├── common
├── core
│   ├── cache
│   ├── collector
│   ├── config
│   ├── connection
│   ├── processor
│   └── report
├── monitor
└── storage
```

---

## 九、技术栈

- Java 17（Temurin）
- Spring Boot 3.x
- Maven 3.9+
- Redis 7（可选）
- Docker / Docker Compose

---

## 十、快速开始

```bash
mvn clean package -DskipTests
java -jar target/data-collection-service.jar --spring.profiles.active=dev
```

---

## 十一、Docker Compose

```bash
docker compose up -d
docker compose logs -f app
```

---

## 十二、典型配置示例（Modbus TCP）

```json
{
  "deviceId": "MODBUS_001",
  "protocol": "MODBUS_TCP",
  "points": [
    {
      "pointId": "ua",
      "reportField": "ua",
      "reportEnabled": true,
      "address": 0,
      "dataType": "FLOAT"
    }
  ]
}
```

---

## 十三、监控与排障

- /monitor/cache
- /monitor/devices
- /monitor/performance
- /monitor/system
- /monitor/errors
- /health

---

