package com.wangbin.collector.core.connection.adapter;

import com.wangbin.collector.common.domain.enums.ConnectionStatus;
import com.wangbin.collector.core.connection.model.ConnectionConfig;
import com.wangbin.collector.core.connection.model.ConnectionMetrics;

import java.util.Map;

/**
 * 连接适配器接口
 * @param <C> 协议客户端类型
 */
public interface ConnectionAdapter<C> {

    /**
     * 建立连接
     */
    void connect() throws Exception;

    /**
     * 断开连接
     */
    void disconnect() throws Exception;

    /**
     * 重新连接
     */
    void reconnect() throws Exception;

    /**
     * 发送数据
     */
    void send(byte[] data) throws Exception;

    /**
     * 发送数据（字符串）
     */
    void send(String data) throws Exception;

    /**
     * 发送数据（对象）
     */
    void send(Object data) throws Exception;

    /**
     * 接收数据
     */
    byte[] receive() throws Exception;

    /**
     * 接收数据（字符串）
     */
    String receiveAsString() throws Exception;

    /**
     * 接收数据（带超时）
     */
    byte[] receive(long timeout) throws Exception;

    /**
     * 发送心跳
     */
    void heartbeat() throws Exception;

    /**
     * 认证
     */
    void authenticate() throws Exception;

    /**
     * 获取连接状态
     */
    ConnectionStatus getStatus();

    /**
     * 获取连接配置
     */
    ConnectionConfig getConfig();

    /**
     * 获取连接指标
     */
    ConnectionMetrics getMetrics();

    /**
     * 是否已连接
     */
    boolean isConnected();

    /**
     * 是否已认证
     */
    boolean isAuthenticated();

    /**
     * 获取连接ID
     */
    String getConnectionId();

    /**
     * 获取设备ID
     */
    String getDeviceId();

    /**
     * 获取最后活动时间
     */
    long getLastActivityTime();

    /**
     * 更新活动时间
     */
    void updateActivityTime();

    /**
     * 获取协议特定客户端
     */
    C getClient();

    /**
     * 获取连接参数
     */
    Map<String, Object> getConnectionParams();

    /**
     * 设置连接参数
     */
    void setConnectionParam(String key, Object value);

    /**
     * 获取连接统计信息
     */
    Map<String, Object> getStatistics();

    /**
     * 重置统计信息
     */
    void resetStatistics();

    /**
     * 执行连接健康检查
     * @return 连接是否健康
     */
    boolean healthCheck();
}