package com.wangbin.collector.core.collector.protocol.base;

import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.common.domain.entity.DeviceInfo;
import com.wangbin.collector.common.exception.CollectorException;

import java.util.List;
import java.util.Map;

/**
 * 协议采集器接口
 */
public interface ProtocolCollector {

    /**
     * 初始化采集器
     */
    void init(DeviceInfo deviceInfo) throws CollectorException;

    /**
     * 连接设备
     */
    void connect() throws CollectorException;

    /**
     * 断开连接
     */
    void disconnect() throws CollectorException;

    /**
     * 读取单个点位
     */
    Object readPoint(DataPoint point) throws CollectorException;

    /**
     * 批量读取点位
     */
    Map<String, Object> readPoints(List<DataPoint> points) throws CollectorException;

    /**
     * 写入单个点位
     */
    boolean writePoint(DataPoint point, Object value) throws CollectorException;

    /**
     * 批量写入点位
     */
    Map<String, Boolean> writePoints(Map<DataPoint, Object> points) throws CollectorException;

    /**
     * 订阅点位变化
     */
    void subscribe(List<DataPoint> points) throws CollectorException;

    /**
     * 取消订阅
     */
    void unsubscribe(List<DataPoint> points) throws CollectorException;

    /**
     * 获取设备状态
     */
    Map<String, Object> getDeviceStatus() throws CollectorException;

    /**
     * 执行设备命令
     */
    Object executeCommand(String command, Map<String, Object> params) throws CollectorException;

    /**
     * 获取采集器类型
     */
    String getCollectorType();

    /**
     * 获取协议类型
     */
    String getProtocolType();

    /**
     * 是否已连接
     */
    boolean isConnected();

    /**
     * 获取连接状态
     */
    String getConnectionStatus();

    /**
     * 获取最后错误信息
     */
    String getLastError();

    /**
     * 获取统计信息
     */
    Map<String, Object> getStatistics();

    /**
     * 重置统计信息
     */
    void resetStatistics();

    /**
     * 销毁采集器
     */
    void destroy();
}
