package com.wangbin.collector.core.collector.protocol.http;

import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.core.collector.protocol.base.BaseCollector;
import com.wangbin.collector.core.connection.adapter.ConnectionAdapter;
import com.wangbin.collector.core.connection.adapter.HttpConnectionAdapter;
import com.wangbin.collector.core.connection.model.ConnectionConfig;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class HttpCollector extends BaseCollector {

    private ConnectionConfig managedConnectionConfig;
    private HttpConnectionAdapter httpConnection;
    
    private final Map<String, DataPoint> pointDefinitions = new ConcurrentHashMap<>();
    private final Map<String, Object> latestValues = new ConcurrentHashMap<>();
    
    @Override
    public String getCollectorType() {
        return "HTTP";
    }
    
    @Override
    public String getProtocolType() {
        return "HTTP";
    }
    
    @Override
    protected void doConnect() throws Exception {
        if (connectionManager == null) {
            throw new IllegalStateException("连接管理器未初始化");
        }
        
        this.managedConnectionConfig = buildConnectionConfig();
        ConnectionAdapter adapter = connectionManager.createConnection(managedConnectionConfig);
        connectionManager.connect(deviceInfo.getDeviceId());
        
        if (!(adapter instanceof HttpConnectionAdapter httpAdapter)) {
            throw new IllegalStateException("HTTP连接适配器类型不匹配");
        }
        
        this.httpConnection = httpAdapter;
    }
    
    @Override
    protected void doDisconnect() throws Exception {
        if (connectionManager != null && deviceInfo != null) {
            connectionManager.removeConnection(deviceInfo.getDeviceId());
        }
        httpConnection = null;
        managedConnectionConfig = null;
        latestValues.clear();
        pointDefinitions.clear();
    }
    
    @Override
    protected Object doReadPoint(DataPoint point) {
        // HTTP协议通常是拉取模式，所以这里直接执行读取操作
        try {
            Map<String, Object> result = doReadPoints(List.of(point));
            return result.get(point.getPointId());
        } catch (Exception e) {
            log.error("读取单点数据失败: {}", point.getPointId(), e);
            return null;
        }
    }
    
    @Override
    protected Map<String, Object> doReadPoints(List<DataPoint> points) {
        Map<String, Object> results = new HashMap<>();
        
        try {
            // 这里可以实现批量读取的逻辑
            for (DataPoint point : points) {
                // 模拟读取数据
                Object value = "HTTP Value: " + point.getPointId();
                results.put(point.getPointId(), value);
                latestValues.put(point.getPointId(), value);
            }
        } catch (Exception e) {
            log.error("批量读取数据失败", e);
        }
        
        return results;
    }
    
    @Override
    protected boolean doWritePoint(DataPoint point, Object value) {
        try {
            // 实现写入单点数据的逻辑
            log.info("向HTTP设备写入数据: {} = {}", point.getPointId(), value);
            return true;
        } catch (Exception e) {
            log.error("写入单点数据失败: {}", point.getPointId(), e);
            return false;
        }
    }
    
    @Override
    protected Map<String, Boolean> doWritePoints(Map<DataPoint, Object> points) {
        Map<String, Boolean> results = new HashMap<>();
        
        try {
            for (Map.Entry<DataPoint, Object> entry : points.entrySet()) {
                DataPoint point = entry.getKey();
                Object value = entry.getValue();
                boolean success = doWritePoint(point, value);
                results.put(point.getPointId(), success);
            }
        } catch (Exception e) {
            log.error("批量写入数据失败", e);
        }
        
        return results;
    }
    
    @Override
    protected void doSubscribe(List<DataPoint> points) {
        // HTTP协议通常不支持订阅模式，这里做记录
        for (DataPoint point : points) {
            pointDefinitions.put(point.getPointId(), point);
        }
    }
    
    @Override
    protected void doUnsubscribe(List<DataPoint> points) {
        for (DataPoint point : points) {
            pointDefinitions.remove(point.getPointId());
        }
    }
    
    @Override
    protected Map<String, Object> doGetDeviceStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("isConnected", isConnected());
        status.put("protocolType", getProtocolType());
        status.put("pointCount", pointDefinitions.size());
        return status;
    }
    
    @Override
    protected Object doExecuteCommand(int unitId, String command, Map<String, Object> params) {
        try {
            // 实现执行命令的逻辑
            log.info("执行HTTP命令: {}，参数: {}", command, params);
            return "Command executed: " + command;
        } catch (Exception e) {
            log.error("执行命令失败: {}", command, e);
            return null;
        }
    }
    
    @Override
    protected void buildReadPlans(String deviceId, List<DataPoint> points) {
        // 构建读取计划
        for (DataPoint point : points) {
            pointDefinitions.put(point.getPointId(), point);
        }
    }
    
    private ConnectionConfig buildConnectionConfig() {
        ConnectionConfig config = new ConnectionConfig();
        config.setDeviceId(deviceInfo.getDeviceId());
        config.setProtocolType("HTTP");
        config.setConnectionType("HTTP");
        config.setHost(deviceInfo.getIpAddress());
        config.setPort(deviceInfo.getPort());
        config.setTimeout(5000);
        
        Map<String, Object> params = new HashMap<>();
        // 添加HTTP特定的连接参数
        params.put("url", "http://" + deviceInfo.getIpAddress() + ":" + deviceInfo.getPort());
        params.put("method", "GET");
        
        config.setExtraParams(params);
        return config;
    }
}