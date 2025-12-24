package com.wangbin.collector.core.collector.protocol.base;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.common.domain.entity.DeviceInfo;
import com.wangbin.collector.common.domain.enums.DataQuality;
import com.wangbin.collector.common.exception.CollectorException;
import com.wangbin.collector.core.config.CollectorProperties;
import com.wangbin.collector.core.connection.manager.ConnectionManager;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 基础采集器抽象类
 */
@Slf4j
public abstract class BaseCollector implements ProtocolCollector {

    @Getter
    protected DeviceInfo deviceInfo;

    protected boolean connected = false;
    protected String connectionStatus = "DISCONNECTED";
    protected String lastError;
    protected long lastConnectTime;
    protected long lastDisconnectTime;
    protected long lastActivityTime;

    // 统计信息
    protected AtomicLong totalReadCount = new AtomicLong(0);
    protected AtomicLong totalWriteCount = new AtomicLong(0);
    protected AtomicLong totalErrorCount = new AtomicLong(0);
    protected AtomicLong totalBytesRead = new AtomicLong(0);
    protected AtomicLong totalBytesWrite = new AtomicLong(0);
    protected AtomicLong totalReadTime = new AtomicLong(0);
    protected AtomicLong totalWriteTime = new AtomicLong(0);

    // 订阅的点位
    protected final Set<String> subscribedPointsSet = ConcurrentHashMap.newKeySet();

    // 数据转换器
    protected Map<String, DataConverter> dataConverters = new HashMap<>();

    // 使用Map存储订阅的点位对象，而不是Set只存ID
    protected final Map<String, DataPoint> subscribedPointMap = new ConcurrentHashMap<>();

    @Autowired
    protected CollectorProperties collectorProperties;

    @Autowired
    protected ConnectionManager connectionManager;

    @Override
    public void init(DeviceInfo deviceInfo) throws CollectorException {
        this.deviceInfo = deviceInfo;
        // 初始化数据转换器
        initDataConverters();

        log.info("采集器初始化完成: {} [{}]", deviceInfo.getDeviceName(), getCollectorType());
    }

    @Override
    public void connect() throws CollectorException {
        if (isConnected()) {
            log.warn("采集器已连接: {}", deviceInfo.getDeviceId());
            return;
        }

        try {
            log.info("开始连接设备: {}", deviceInfo.getDeviceId());
            connectionStatus = "CONNECTING";

            // 执行实际连接逻辑
            doConnect();

            connected = true;
            connectionStatus = "CONNECTED";
            lastConnectTime = System.currentTimeMillis();
            lastActivityTime = System.currentTimeMillis();

            log.info("设备连接成功: {}", deviceInfo.getDeviceId());
        } catch (Exception e) {
            connected = false;
            connectionStatus = "ERROR";
            lastError = e.getMessage();
            totalErrorCount.incrementAndGet();
            log.error("设备连接失败: {}", deviceInfo.getDeviceId(), e);
            throw new CollectorException("设备连接失败", deviceInfo.getDeviceId(), null, DataQuality.DEVICE_ERROR);
        }
    }

    @Override
    public void disconnect() throws CollectorException {
        if (!isConnected()) {
            log.warn("采集器已断开: {}", deviceInfo.getDeviceId());
            return;
        }

        try {
            log.info("开始断开设备: {}", deviceInfo.getDeviceId());

            // 取消所有订阅
            unsubscribe(new ArrayList<>());

            // 执行实际断开逻辑
            doDisconnect();

            connected = false;
            connectionStatus = "DISCONNECTED";
            lastDisconnectTime = System.currentTimeMillis();

            log.info("设备断开成功: {}", deviceInfo.getDeviceId());
        } catch (Exception e) {
            connectionStatus = "ERROR";
            lastError = e.getMessage();
            totalErrorCount.incrementAndGet();
            log.error("设备断开失败: {}", deviceInfo.getDeviceId(), e);
            throw new CollectorException("设备断开失败", deviceInfo.getDeviceId(), null, DataQuality.DEVICE_ERROR);
        }
    }

    @Override
    public Object readPoint(DataPoint point) throws CollectorException {
        checkConnection();

        long startTime = System.currentTimeMillis();
        try {
            log.debug("读取点位: {}.{}", deviceInfo.getDeviceId(), point.getPointName());

            // 执行实际读取逻辑
            Object rawValue = doReadPoint(point);

            // 数据转换
            Object processedValue = convertData(point, rawValue);

            // 数据验证
            validateData(point, processedValue);

            // 更新统计
            totalReadCount.incrementAndGet();
            totalReadTime.addAndGet(System.currentTimeMillis() - startTime);
            lastActivityTime = System.currentTimeMillis();

            log.debug("点位读取成功: {}.{} = {}",
                    deviceInfo.getDeviceId(), point.getPointName(), processedValue);

            return processedValue;
        } catch (Exception e) {
            totalErrorCount.incrementAndGet();
            lastError = e.getMessage();
            log.error("点位读取失败: {}.{}", deviceInfo.getDeviceId(), point.getPointName(), e);
            throw new CollectorException("点位读取失败", deviceInfo.getDeviceId(),
                    point.getPointId(), DataQuality.DEVICE_ERROR);
        }
    }

    @Override
    public Map<String, Object> readPoints(List<DataPoint> points) throws CollectorException {
        checkConnection();

        long startTime = System.currentTimeMillis();
        Map<String, Object> results = new HashMap<>();

        try {
            log.debug("批量读取点位: {}, 数量: {}", deviceInfo.getDeviceId(), points.size());

            // 执行实际批量读取逻辑
            Map<String, Object> rawValues = doReadPoints(points);

            // 数据转换和验证
            for (DataPoint point : points) {
                String pointId = point.getPointId();
                Object rawValue = rawValues.get(pointId);

                if (rawValue != null) {
                    Object processedValue = convertData(point, rawValue);
                    validateData(point, processedValue);
                    results.put(pointId, processedValue);
                } else {
                    results.put(pointId, null);
                }
            }

            // 更新统计
            totalReadCount.addAndGet(points.size());
            totalReadTime.addAndGet(System.currentTimeMillis() - startTime);
            lastActivityTime = System.currentTimeMillis();

            log.debug("批量点位读取成功: {}, 数量: {},值：{}", deviceInfo.getDeviceId(), points.size(),new ObjectMapper().writeValueAsString(rawValues));

            return results;
        } catch (Exception e) {
            totalErrorCount.incrementAndGet();
            lastError = e.getMessage();
            log.error("批量点位读取失败: {}", deviceInfo.getDeviceId(), e);
            throw new CollectorException("批量点位读取失败", deviceInfo.getDeviceId(),
                    null, DataQuality.DEVICE_ERROR);
        }
    }

    @Override
    public boolean writePoint(DataPoint point, Object value) throws CollectorException {
        checkConnection();

        long startTime = System.currentTimeMillis();
        try {
            log.debug("写入点位: {}.{} = {}", deviceInfo.getDeviceId(), point.getPointName(), value);

            // 验证写入权限
            if (!"W".equals(point.getReadWrite()) && !"RW".equals(point.getReadWrite())) {
                throw new CollectorException("点位没有写入权限", deviceInfo.getDeviceId(),
                        point.getPointId(), DataQuality.CONFIG_ERROR);
            }

            // 数据验证
            validateData(point, value);

            // 数据转换（反向）
            Object rawValue = convertDataForWrite(point, value);

            // 执行实际写入逻辑
            boolean result = doWritePoint(point, rawValue);

            // 更新统计
            totalWriteCount.incrementAndGet();
            totalWriteTime.addAndGet(System.currentTimeMillis() - startTime);
            lastActivityTime = System.currentTimeMillis();

            log.debug("点位写入成功: {}.{}", deviceInfo.getDeviceId(), point.getPointName());

            return result;
        } catch (Exception e) {
            totalErrorCount.incrementAndGet();
            lastError = e.getMessage();
            log.error("点位写入失败: {}.{}", deviceInfo.getDeviceId(), point.getPointName(), e);
            throw new CollectorException("点位写入失败", deviceInfo.getDeviceId(),
                    point.getPointId(), DataQuality.DEVICE_ERROR);
        }
    }

    @Override
    public Map<String, Boolean> writePoints(Map<DataPoint, Object> points) throws CollectorException {
        checkConnection();

        long startTime = System.currentTimeMillis();
        Map<String, Boolean> results = new HashMap<>();

        try {
            log.debug("批量写入点位: {}, 数量: {}", deviceInfo.getDeviceId(), points.size());

            Map<DataPoint, Object> rawValues = new HashMap<>();

            // 数据验证和转换
            for (Map.Entry<DataPoint, Object> entry : points.entrySet()) {
                DataPoint point = entry.getKey();
                Object value = entry.getValue();

                // 验证写入权限
                if (!"W".equals(point.getReadWrite()) && !"RW".equals(point.getReadWrite())) {
                    results.put(point.getPointId(), false);
                    continue;
                }

                // 数据验证
                validateData(point, value);

                // 数据转换（反向）
                Object rawValue = convertDataForWrite(point, value);
                rawValues.put(point, rawValue);
            }

            // 执行实际批量写入逻辑
            Map<String, Boolean> writeResults = doWritePoints(rawValues);

            // 更新统计
            totalWriteCount.addAndGet(points.size());
            totalWriteTime.addAndGet(System.currentTimeMillis() - startTime);
            lastActivityTime = System.currentTimeMillis();

            log.debug("批量点位写入成功: {}, 数量: {}", deviceInfo.getDeviceId(), points.size());

            return writeResults;
        } catch (Exception e) {
            totalErrorCount.incrementAndGet();
            lastError = e.getMessage();
            log.error("批量点位写入失败: {}", deviceInfo.getDeviceId(), e);
            throw new CollectorException("批量点位写入失败", deviceInfo.getDeviceId(),
                    null, DataQuality.DEVICE_ERROR);
        }
    }


    @Override
    public void subscribe(List<DataPoint> points) throws CollectorException {
        checkConnection();

        try {
            log.debug("订阅点位: {}, 数量: {}", deviceInfo.getDeviceId(), points.size());

            // 执行实际订阅逻辑
            doSubscribe(points);

            // 记录订阅的点位
            for (DataPoint point : points) {
                subscribedPointMap.put(point.getPointId(), point);
            }

            log.info("点位订阅成功: {}, 数量: {}", deviceInfo.getDeviceId(), points.size());
        } catch (Exception e) {
            totalErrorCount.incrementAndGet();
            lastError = e.getMessage();
            log.error("点位订阅失败: {}", deviceInfo.getDeviceId(), e);
            throw new CollectorException("点位订阅失败", deviceInfo.getDeviceId(),
                    null, DataQuality.DEVICE_ERROR);
        }
    }

    @Override
    public void unsubscribe(List<DataPoint> points) throws CollectorException {
        if (!isConnected()) {
            return;
        }

        try {
            if (points.isEmpty()) {
                // 取消所有订阅
                log.debug("取消所有订阅: {}", deviceInfo.getDeviceId());

                // 获取所有订阅的点位对象
                List<DataPoint> allSubscribedPoints = new ArrayList<>(subscribedPointMap.values());
                if (!allSubscribedPoints.isEmpty()) {
                    doUnsubscribe(allSubscribedPoints);
                }
                subscribedPointMap.clear();

                log.info("所有点位取消订阅成功: {}", deviceInfo.getDeviceId());
            } else {
                log.debug("取消订阅点位: {}, 数量: {}", deviceInfo.getDeviceId(), points.size());

                // 执行实际取消订阅逻辑
                doUnsubscribe(points);

                // 移除订阅的点位
                for (DataPoint point : points) {
                    subscribedPointMap.remove(point.getPointId());
                }

                log.info("点位取消订阅成功: {}, 数量: {}", deviceInfo.getDeviceId(), points.size());
            }
        } catch (Exception e) {
            totalErrorCount.incrementAndGet();
            lastError = e.getMessage();
            log.error("点位取消订阅失败: {}", deviceInfo.getDeviceId(), e);
            throw new CollectorException("点位取消订阅失败", deviceInfo.getDeviceId(),
                    null, DataQuality.DEVICE_ERROR);
        }
    }

    @Override
    public Map<String, Object> getDeviceStatus() throws CollectorException {
        try {
            Map<String, Object> status = new HashMap<>();
            status.put("deviceId", deviceInfo.getDeviceId());
            status.put("deviceName", deviceInfo.getDeviceName());
            status.put("connected", connected);
            status.put("connectionStatus", connectionStatus);
            status.put("lastConnectTime", lastConnectTime);
            status.put("lastActivityTime", lastActivityTime);
            status.put("subscribedPoints", subscribedPointsSet.size());
            status.put("totalReadCount", totalReadCount.get());
            status.put("totalWriteCount", totalWriteCount.get());
            status.put("totalErrorCount", totalErrorCount.get());

            // 获取设备特定状态
            Map<String, Object> deviceSpecificStatus = doGetDeviceStatus();
            if (deviceSpecificStatus != null) {
                status.putAll(deviceSpecificStatus);
            }

            return status;
        } catch (Exception e) {
            log.error("获取设备状态失败: {}", deviceInfo.getDeviceId(), e);
            throw new CollectorException("获取设备状态失败", deviceInfo.getDeviceId(),
                    null, DataQuality.DEVICE_ERROR);
        }
    }

    @Override
    public Object executeCommand(String command, Map<String, Object> params) throws CollectorException {
        checkConnection();

        try {
            log.debug("执行设备命令: {}, 命令: {}", deviceInfo.getDeviceId(), command);

            // 执行实际命令
            Integer slaveId = (Integer)params.getOrDefault("slaveId", 1);
            Object result = doExecuteCommand(slaveId,command, params);

            lastActivityTime = System.currentTimeMillis();

            log.debug("设备命令执行成功: {}, 命令: {}", deviceInfo.getDeviceId(), command);

            return result;
        } catch (Exception e) {
            totalErrorCount.incrementAndGet();
            lastError = e.getMessage();
            log.error("设备命令执行失败: {}, 命令: {}", deviceInfo.getDeviceId(), command, e);
            throw new CollectorException("设备命令执行失败", deviceInfo.getDeviceId(),
                    null, DataQuality.DEVICE_ERROR);
        }
    }

    @Override
    public boolean isConnected() {
        return connected;
    }

    @Override
    public String getConnectionStatus() {
        return connectionStatus;
    }

    @Override
    public String getLastError() {
        return lastError;
    }

    @Override
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalReadCount", totalReadCount.get());
        stats.put("totalWriteCount", totalWriteCount.get());
        stats.put("totalErrorCount", totalErrorCount.get());
        stats.put("totalBytesRead", totalBytesRead.get());
        stats.put("totalBytesWrite", totalBytesWrite.get());

        long avgReadTime = totalReadCount.get() > 0 ?
                totalReadTime.get() / totalReadCount.get() : 0;
        long avgWriteTime = totalWriteCount.get() > 0 ?
                totalWriteTime.get() / totalWriteCount.get() : 0;

        stats.put("averageReadTime", avgReadTime);
        stats.put("averageWriteTime", avgWriteTime);
        stats.put("lastActivityTime", lastActivityTime);
        stats.put("connectionDuration", connected ? System.currentTimeMillis() - lastConnectTime : 0);
        stats.put("subscribedPoints", subscribedPointsSet.size());

        return stats;
    }

    @Override
    public void resetStatistics() {
        totalReadCount.set(0);
        totalWriteCount.set(0);
        totalErrorCount.set(0);
        totalBytesRead.set(0);
        totalBytesWrite.set(0);
        totalReadTime.set(0);
        totalWriteTime.set(0);
    }

    @Override
    public void destroy() {
        try {
            disconnect();
            log.info("采集器销毁完成: {}", deviceInfo.getDeviceId());
        } catch (Exception e) {
            log.error("采集器销毁失败: {}", deviceInfo.getDeviceId(), e);
        }
    }
    @Override
    public void rebuildReadPlans(String deviceId, List<DataPoint> points){
        try {
            buildReadPlans(deviceId,points);
            log.info("执行计划准备完成: {}", deviceId);
        } catch (Exception e) {
            log.error("执行计划准备失败: {}", deviceId, e);
        }
    }

    // =============== 抽象方法 ===============

    protected abstract void doConnect() throws Exception;
    protected abstract void doDisconnect() throws Exception;
    protected abstract Object doReadPoint(DataPoint point) throws Exception;
    protected abstract Map<String, Object> doReadPoints(List<DataPoint> points) throws Exception;
    protected abstract boolean doWritePoint(DataPoint point, Object value) throws Exception;
    protected abstract Map<String, Boolean> doWritePoints(Map<DataPoint, Object> points) throws Exception;
    protected abstract void doSubscribe(List<DataPoint> points) throws Exception;
    protected abstract void doUnsubscribe(List<DataPoint> points) throws Exception;
    protected abstract Map<String, Object> doGetDeviceStatus() throws Exception;
    protected abstract Object doExecuteCommand(int unitId,String command, Map<String, Object> params) throws Exception;
    protected abstract void buildReadPlans(String deviceId, List<DataPoint> points) throws Exception;

    // =============== 辅助方法 ===============

    /**
     * 检查连接状态
     */
    protected void checkConnection() {
        if (!isConnected()) {
            throw new IllegalStateException("设备未连接: " + deviceInfo.getDeviceId());
        }
    }

    /**
     * 初始化数据转换器
     */
    protected void initDataConverters() {
        // 注册默认的数据转换器
        dataConverters.put("default", new DefaultDataConverter());
        dataConverters.put("scale", new ScaleDataConverter());
        dataConverters.put("boolean", new BooleanDataConverter());
    }

    /**
     * 数据转换
     */
    protected Object convertData(DataPoint point, Object rawValue) {
        if (rawValue == null) {
            return null;
        }

        // 应用缩放因子和偏移量
        Double scaledValue = point.getActualValue(convertToDouble(rawValue));

        // 应用数据转换器
        DataConverter converter = dataConverters.get(point.getDataType());
        if (converter != null) {
            return converter.convert(scaledValue, point);
        }

        return scaledValue;
    }

    /**
     * 写入数据转换
     */
    protected Object convertDataForWrite(DataPoint point, Object value) {
        if (value == null) {
            return null;
        }

        // 反向应用缩放因子和偏移量
        Double rawValue = convertToDouble(value);
        if (point.getScalingFactor() != null && point.getScalingFactor() != 0) {
            rawValue = rawValue / point.getScalingFactor();
        }
        if (point.getOffset() != null) {
            rawValue = rawValue - point.getOffset();
        }

        return rawValue;
    }

    /**
     * 数据验证
     */
    protected void validateData(DataPoint point, Object value) {
        if (value == null) {
            return;
        }

        if (value instanceof Number) {
            double doubleValue = ((Number) value).doubleValue();

            // 检查值范围
            if (!point.isValueValid(doubleValue)) {
                /*throw new IllegalArgumentException(
                        String.format("点位值超出范围: %s, 值: %f, 范围: [%f, %f]",
                                point.getPointName(), doubleValue,
                                point.getMinValue(), point.getMaxValue()));*/
                log.warn(String.format("点位值超出范围: %s, 值: %f, 范围: [%f, %f]",point.getPointName(), doubleValue,point.getMinValue(), point.getMaxValue()));
            }
        }
    }

    /**
     * 转换为Double
     */
    private Double convertToDouble(Object value) {
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        } else if (value instanceof String) {
            try {
                return Double.parseDouble((String) value);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("无法转换为数字: " + value);
            }
        } else if (value instanceof Boolean) {
            return ((Boolean) value) ? 1.0 : 0.0;
        } else {
            throw new IllegalArgumentException("不支持的数据类型: " + value.getClass().getName());
        }
    }

    // =============== 数据转换器接口和实现 ===============

    /**
     * 数据转换器接口
     */
    protected interface DataConverter {
        Object convert(Object value, DataPoint point);
    }

    /**
     * 默认数据转换器
     */
    protected class DefaultDataConverter implements DataConverter {
        @Override
        public Object convert(Object value, DataPoint point) {
            return value;
        }
    }

    /**
     * 缩放数据转换器
     */
    protected class ScaleDataConverter implements DataConverter {
        @Override
        public Object convert(Object value, DataPoint point) {
            if (value instanceof Number) {
                double scaledValue = ((Number) value).doubleValue();

                // 应用精度（小数位数）
                if (point.getPrecision() != null) {
                    double factor = Math.pow(10, point.getPrecision());
                    scaledValue = Math.round(scaledValue * factor) / factor;
                }

                return scaledValue;
            }
            return value;
        }
    }

    /**
     * 布尔数据转换器
     */
    protected class BooleanDataConverter implements DataConverter {
        @Override
        public Object convert(Object value, DataPoint point) {
            if (value instanceof Number) {
                double numValue = ((Number) value).doubleValue();
                return numValue != 0;
            } else if (value instanceof String) {
                String strValue = ((String) value).toLowerCase();
                return "true".equals(strValue) || "1".equals(strValue) || "on".equals(strValue);
            }
            return value;
        }
    }
}
