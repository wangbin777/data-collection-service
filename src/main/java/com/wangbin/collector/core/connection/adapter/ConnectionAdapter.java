package com.wangbin.collector.core.connection.adapter;

import com.wangbin.collector.common.domain.entity.DeviceConnection;
import com.wangbin.collector.common.domain.entity.DeviceInfo;
import com.wangbin.collector.common.domain.enums.ConnectionStatus;
import com.wangbin.collector.core.connection.model.ConnectionMetrics;

import java.util.Map;

/**
 * 连接适配器接口
 * @param <C> 客户端类型
 */
public interface ConnectionAdapter<C> {

    void connect() throws Exception;

    void disconnect() throws Exception;

    void reconnect() throws Exception;

    void send(byte[] data) throws Exception;

    void send(String data) throws Exception;

    void send(Object data) throws Exception;

    byte[] receive() throws Exception;

    String receiveAsString() throws Exception;

    byte[] receive(long timeout) throws Exception;

    void heartbeat() throws Exception;

    void authenticate() throws Exception;

    ConnectionStatus getStatus();

    DeviceConnection getConnectionConfig();

    DeviceInfo getDeviceInfo();

    ConnectionMetrics getMetrics();

    boolean isConnected();

    boolean isAuthenticated();

    String getConnectionId();

    String getDeviceId();

    long getLastActivityTime();

    void updateActivityTime();

    C getClient();

    Map<String, Object> getConnectionParams();

    void setConnectionParam(String key, Object value);

    Map<String, Object> getStatistics();

    void resetStatistics();

    boolean healthCheck();
}
