package com.wangbin.collector.core.collector.manager;

import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.common.domain.entity.DeviceInfo;
import com.wangbin.collector.common.exception.CollectorException;
import com.wangbin.collector.core.collector.factory.CollectorFactory;
import com.wangbin.collector.core.collector.protocol.base.ProtocolCollector;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 设备管理器 - 负责设备生命周期管理和基础操作
 */
@Slf4j
@Service
public class CollectionManager {

    @Autowired
    private CollectorFactory collectorFactory;

    @Getter
    private final Map<String, ProtocolCollector> collectors = new ConcurrentHashMap<>();

    @PostConstruct
    public void init() {
        log.info("设备管理器初始化完成");
    }

    @PreDestroy
    public void destroy() {
        log.info("开始销毁设备管理器...");
        destroyAllCollectors();
        log.info("设备管理器销毁完成");
    }

    /**
     * 注册设备
     */
    public void registerDevice(DeviceInfo deviceInfo) throws CollectorException {
        String deviceId = deviceInfo.getDeviceId();

        synchronized (collectors) {
            if (collectors.containsKey(deviceId)) {
                log.warn("设备已注册: {}", deviceId);
                return;
            }

            try {
                ProtocolCollector collector = collectorFactory.createCollector(deviceInfo);
                collectors.put(deviceId, collector);
                log.info("设备注册成功: {}", deviceId);
            } catch (Exception e) {
                log.error("设备注册失败: {}", deviceId, e);
                throw new CollectorException("设备注册失败", deviceId, null, e);
            }
        }
    }

    /**
     * 注销设备
     */
    public void unregisterDevice(String deviceId) throws CollectorException {
        synchronized (collectors) {
            ProtocolCollector collector = collectors.remove(deviceId);
            if (collector != null) {
                try {
                    collector.destroy();
                    log.info("设备注销成功: {}", deviceId);
                } catch (Exception e) {
                    log.error("设备注销失败: {}", deviceId, e);
                    throw new CollectorException("设备注销失败", deviceId, null, e);
                }
            }
        }
    }

    /**
     * 连接设备
     */
    public void connectDevice(String deviceId) throws CollectorException {
        ProtocolCollector collector = getCollector(deviceId);
        if (collector == null) {
            throw new CollectorException("设备未注册", deviceId, null);
        }

        try {
            collector.connect();
            log.info("设备连接成功: {}", deviceId);
        } catch (Exception e) {
            log.error("设备连接失败: {}", deviceId, e);
            throw new CollectorException("设备连接失败", deviceId, null, e);
        }
    }

    /**
     * 断开设备连接
     */
    public void disconnectDevice(String deviceId) throws CollectorException {
        ProtocolCollector collector = getCollector(deviceId);
        if (collector == null) {
            throw new CollectorException("设备未注册", deviceId, null);
        }

        try {
            collector.disconnect();
            log.info("设备断开成功: {}", deviceId);
        } catch (Exception e) {
            log.error("设备断开失败: {}", deviceId, e);
            throw new CollectorException("设备断开失败", deviceId, null, e);
        }
    }

    /**
     * 重新连接设备
     */
    public void reconnectDevice(String deviceId) throws CollectorException {
        ProtocolCollector collector = getCollector(deviceId);
        if (collector == null) {
            throw new CollectorException("设备未注册", deviceId, null);
        }

        try {
            if (collector.isConnected()) {
                collector.disconnect();
            }

            Thread.sleep(1000);
            collector.connect();
            log.info("设备重新连接成功: {}", deviceId);
        } catch (Exception e) {
            log.error("设备重新连接失败: {}", deviceId, e);
            throw new CollectorException("设备重新连接失败", deviceId, null, e);
        }
    }

    /**
     * 读取单个点位
     */
    public Object readPoint(String deviceId, DataPoint point) throws CollectorException {
        ProtocolCollector collector = getCollector(deviceId);
        if (collector == null) {
            throw new CollectorException("设备未注册", deviceId, null);
        }

        return collector.readPoint(point);
    }

    /**
     * 批量读取点位
     */
    public Map<String, Object> readPoints(String deviceId, List<DataPoint> points) throws CollectorException {
        ProtocolCollector collector = getCollector(deviceId);
        if (collector == null) {
            throw new CollectorException("设备未注册", deviceId, null);
        }

        return collector.readPoints(points);
    }

    /**
     * 写入单个点位
     */
    public boolean writePoint(String deviceId, DataPoint point, Object value) throws CollectorException {
        ProtocolCollector collector = getCollector(deviceId);
        if (collector == null) {
            throw new CollectorException("设备未注册", deviceId, null);
        }

        return collector.writePoint(point, value);
    }

    /**
     * 批量写入点位
     */
    public Map<String, Boolean> writePoints(String deviceId, Map<DataPoint, Object> points) throws CollectorException {
        ProtocolCollector collector = getCollector(deviceId);
        if (collector == null) {
            throw new CollectorException("设备未注册", deviceId, null);
        }

        return collector.writePoints(points);
    }

    /**
     * 订阅点位
     */
    public void subscribePoints(String deviceId, List<DataPoint> points) throws CollectorException {
        ProtocolCollector collector = getCollector(deviceId);
        if (collector == null) {
            throw new CollectorException("设备未注册", deviceId, null);
        }

        collector.subscribe(points);
    }

    /**
     * 取消订阅点位
     */
    public void unsubscribePoints(String deviceId, List<DataPoint> points) throws CollectorException {
        ProtocolCollector collector = getCollector(deviceId);
        if (collector == null) {
            throw new CollectorException("设备未注册", deviceId, null);
        }

        collector.unsubscribe(points);
    }

    /**
     * 获取设备状态
     */
    public Map<String, Object> getDeviceStatus(String deviceId) throws CollectorException {
        ProtocolCollector collector = getCollector(deviceId);
        if (collector == null) {
            throw new CollectorException("设备未注册", deviceId, null);
        }

        return collector.getDeviceStatus();
    }

    /**
     * 执行设备命令
     */
    public Object executeCommand(String deviceId, String command, Map<String, Object> params)
            throws CollectorException {
        ProtocolCollector collector = getCollector(deviceId);
        if (collector == null) {
            throw new CollectorException("设备未注册", deviceId, null);
        }

        return collector.executeCommand(command, params);
    }

    /**
     * 获取设备采集器
     */
    public ProtocolCollector getCollector(String deviceId) {
        return collectors.get(deviceId);
    }

    /**
     * 获取所有设备ID
     */
    public List<String> getAllDeviceIds() {
        return new ArrayList<>(collectors.keySet());
    }

    /**
     * 获取活跃采集器
     */
    public List<ProtocolCollector> getActiveCollectors() {
        return collectors.values().stream()
                .filter(ProtocolCollector::isConnected)
                .toList();
    }

    /**
     * 检查设备是否已连接
     */
    public boolean isDeviceConnected(String deviceId) {
        ProtocolCollector collector = collectors.get(deviceId);
        return collector != null && collector.isConnected();
    }

    /**
     * 获取设备基本信息
     */
    public Map<String, Object> getDeviceBasicInfo(String deviceId) {
        ProtocolCollector collector = collectors.get(deviceId);
        if (collector == null) {
            return Collections.emptyMap();
        }

        Map<String, Object> info = new HashMap<>();
        info.put("deviceId", deviceId);
        info.put("collectorType", collector.getCollectorType());
        info.put("isConnected", collector.isConnected());
        //info.put("lastConnectTime", collector.getLastConnectTime());
        return info;
    }

    /**
     * 销毁所有采集器
     */
    private void destroyAllCollectors() {
        for (ProtocolCollector collector : collectors.values()) {
            try {
                collector.destroy();
            } catch (Exception e) {
                log.error("采集器销毁失败: {}", collector.getCollectorType(), e);
            }
        }
        collectors.clear();
        log.info("所有采集器已销毁");
    }
}