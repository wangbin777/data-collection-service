package com.wangbin.collector.core.collector;

import com.wangbin.collector.core.collector.scheduler.CollectionScheduler;
import com.wangbin.collector.core.collector.statistics.CollectionStatistics;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * 采集服务 - 对外提供统一的采集接口
 */
@Slf4j
@Service
public class CollectionService {

    @Autowired
    private CollectionScheduler collectionScheduler;

    @Autowired
    private CollectionStatistics collectionStatistics;

    /**
     * 启动设备采集
     */
    public boolean startDevice(String deviceId) {
        return collectionScheduler.startDevice(deviceId);
    }

    /**
     * 停止设备采集
     */
    public boolean stopDevice(String deviceId) {
        return collectionScheduler.stopDevice(deviceId);
    }

    /**
     * 重新加载所有设备
     */
    public void reloadAllDevices() {
        collectionScheduler.reloadAllDevices();
    }

    /**
     * 获取设备状态
     */
    public Map<String, Object> getDeviceStatus(String deviceId) {
        return collectionScheduler.getDeviceScheduleStatus(deviceId);
    }

    /**
     * 获取所有设备统计
     */
    public Map<String, Map<String, Object>> getAllStatistics() {
        return collectionStatistics.getAllStatistics();
    }

    /**
     * 获取运行中的设备列表
     */
    public List<String> getRunningDevices() {
        return collectionScheduler.getRunningDevices();
    }

    /**
     * 检查设备是否在运行
     */
    public boolean isDeviceRunning(String deviceId) {
        return collectionScheduler.isDeviceRunning(deviceId);
    }
}