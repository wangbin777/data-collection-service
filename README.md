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