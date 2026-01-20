package com.wangbin.collector.core.collector.protocol.websocket;

import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.common.domain.entity.DeviceConnection;
import com.wangbin.collector.core.collector.protocol.base.BaseCollector;
import com.wangbin.collector.core.connection.adapter.ConnectionAdapter;
import com.wangbin.collector.core.connection.adapter.WebSocketConnectionAdapter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

@Slf4j
public class WebSocketCollector extends BaseCollector {

    private WebSocketConnectionAdapter webSocketConnection;

    private final Map<String, DataPoint> pointDefinitions = new ConcurrentHashMap<>();
    private final Map<String, Object> latestValues = new ConcurrentHashMap<>();
    private final Map<String, Long> latestTimestamps = new ConcurrentHashMap<>();
    
    private final Consumer<String> messageListener = this::handleWebSocketMessage;
    
    @Override
    public String getCollectorType() {
        return "WEBSOCKET";
    }
    
    @Override
    public String getProtocolType() {
        return "WEBSOCKET";
    }
    
    @Override
    protected void doConnect() throws Exception {
        if (connectionManager == null) {
            throw new IllegalStateException("连接管理器未初始化");
        }
        
        DeviceConnection connectionConfig = prepareConnectionConfig();
        ConnectionAdapter adapter = connectionManager.createConnection(deviceInfo, connectionConfig);
        connectionManager.connect(deviceInfo.getDeviceId());
        
        if (!(adapter instanceof WebSocketConnectionAdapter webSocketAdapter)) {
            throw new IllegalStateException("WebSocket连接适配器类型不匹配");
        }
        
        this.webSocketConnection = webSocketAdapter;
    }
    
    @Override
    protected void doDisconnect() throws Exception {
        if (connectionManager != null && deviceInfo != null) {
            connectionManager.removeConnection(deviceInfo.getDeviceId());
        }
        
        webSocketConnection = null;
        latestValues.clear();
        latestTimestamps.clear();
        pointDefinitions.clear();
    }
    
    @Override
    protected Object doReadPoint(DataPoint point) {
        // WebSocket通常是推送模式，但也支持拉取
        return latestValues.get(point.getPointId());
    }
    
    @Override
    protected Map<String, Object> doReadPoints(List<DataPoint> points) {
        Map<String, Object> results = new HashMap<>();
        
        for (DataPoint point : points) {
            results.put(point.getPointId(), latestValues.get(point.getPointId()));
        }
        
        return results;
    }
    
    @Override
    protected boolean doWritePoint(DataPoint point, Object value) {
        try {
            String message = buildWriteMessage(point, value);
            webSocketConnection.send(message.getBytes());
            log.info("向WebSocket设备写入数据: {} = {}", point.getPointId(), value);
            return true;
        } catch (Exception e) {
            log.error("写入单点数据失败: {}", point.getPointId(), e);
            return false;
        }
    }
    
    @Override
    protected Map<String, Boolean> doWritePoints(Map<DataPoint, Object> points) {
        Map<String, Boolean> results = new HashMap<>();
        
        for (Map.Entry<DataPoint, Object> entry : points.entrySet()) {
            DataPoint point = entry.getKey();
            Object value = entry.getValue();
            boolean success = doWritePoint(point, value);
            results.put(point.getPointId(), success);
        }
        
        return results;
    }
    
    @Override
    protected void doSubscribe(List<DataPoint> points) {
        // WebSocket通常通过发送订阅消息来实现订阅
        for (DataPoint point : points) {
            pointDefinitions.put(point.getPointId(), point);
            // 发送订阅消息到服务器
            try {
                String subscribeMessage = buildSubscribeMessage(point);
                webSocketConnection.send(subscribeMessage.getBytes());
            } catch (Exception e) {
                log.error("订阅点位失败: {}", point.getPointId(), e);
            }
        }
    }
    
    @Override
    protected void doUnsubscribe(List<DataPoint> points) {
        for (DataPoint point : points) {
            pointDefinitions.remove(point.getPointId());
            // 发送取消订阅消息到服务器
            try {
                String unsubscribeMessage = buildUnsubscribeMessage(point);
                webSocketConnection.send(unsubscribeMessage.getBytes());
            } catch (Exception e) {
                log.error("取消订阅点位失败: {}", point.getPointId(), e);
            }
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
            String commandMessage = buildCommandMessage(command, params);
            webSocketConnection.send(commandMessage.getBytes());
            log.info("执行WebSocket命令: {}，参数: {}", command, params);
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
    
    /**
     * 处理接收到的WebSocket消息
     */
    private void handleWebSocketMessage(String message) {
        try {
            // 解析消息并更新点位值
            log.debug("接收到WebSocket消息: {}", message);
            
            // 这里需要根据实际的消息格式进行解析
            // 示例：假设消息格式为 {"pointId": "value"}
            
            // 更新最新值和时间戳
            // latestValues.put(pointId, value);
            // latestTimestamps.put(pointId, System.currentTimeMillis());
            
        } catch (Exception e) {
            log.error("解析WebSocket消息失败: {}", message, e);
        }
    }
    
    /**
     * 构建连接配置
     */
    private DeviceConnection prepareConnectionConfig() {
        DeviceConnection config = requireConnectionConfig();
        if (config.getConnectionType() == null || config.getConnectionType().isBlank()) {
            config.setConnectionType("WEBSOCKET");
        }
        if (config.getHost() == null && deviceInfo.getIpAddress() != null) {
            config.setHost(deviceInfo.getIpAddress());
         }
         if (config.getPort() == null && deviceInfo.getPort() != null) {
             config.setPort(deviceInfo.getPort());
         }
        if (config.getUrl() == null && config.getHost() != null && config.getPort() != null) {
            String scheme = Boolean.TRUE.equals(config.getSslEnabled()) ? "wss" : "ws";
            config.setUrl(scheme + "://" + config.getHost() + ":" + config.getPort());
        }
        /*Map<String, Object> props = config.getProperties();
        if (props == null) {
            props = new HashMap<>();
            config.setProperties(props);
        }
        props.putIfAbsent("path", "/ws");*/
        return config;
    }
    
    /**
     * 构建订阅消息
     */
    private String buildSubscribeMessage(DataPoint point) {
        // 根据实际的协议格式构建订阅消息
        return "{\"action\": \"subscribe\", \"pointId\": \"" + point.getPointId() + "\"}";
    }
    
    /**
     * 构建取消订阅消息
     */
    private String buildUnsubscribeMessage(DataPoint point) {
        // 根据实际的协议格式构建取消订阅消息
        return "{\"action\": \"unsubscribe\", \"pointId\": \"" + point.getPointId() + "\"}";
    }
    
    /**
     * 构建写入消息
     */
    private String buildWriteMessage(DataPoint point, Object value) {
        // 根据实际的协议格式构建写入消息
        return "{\"action\": \"write\", \"pointId\": \"" + point.getPointId() + "\", \"value\": \"" + value + "\"}";
    }
    
    /**
     * 构建命令消息
     */
    private String buildCommandMessage(String command, Map<String, Object> params) {
        // 根据实际的协议格式构建命令消息
        return "{\"action\": \"command\", \"command\": \"" + command + "\", \"params\": " + params + "}";
    }
}
