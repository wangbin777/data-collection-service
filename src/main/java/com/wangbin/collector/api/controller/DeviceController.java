package com.wangbin.collector.api.controller;

import com.wangbin.collector.core.collector.CollectionService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 设备管理控制器
 * 提供设备采集的启动、停止与状态查询接口
 */
@Slf4j
@RestController
@RequestMapping("/api/device")
public class DeviceController {

    @Autowired
    private CollectionService collectionService;

    @PostMapping("/{deviceId}/start")
    public Map<String, Object> startDevice(@PathVariable String deviceId) {
        Map<String, Object> result = baseResult(deviceId);
        try {
            boolean success = collectionService.startDevice(deviceId);
            if (success) {
                result.put("status", "success");
                result.put("message", "设备启动成功");
            } else {
                result.put("status", "error");
                result.put("message", "设备已启动或启动失败");
            }
        } catch (Exception e) {
            log.error("启动设备失败: {}", deviceId, e);
            result.put("status", "error");
            result.put("message", "启动异常: " + e.getMessage());
        }
        return result;
    }

    @PostMapping("/{deviceId}/stop")
    public Map<String, Object> stopDevice(@PathVariable String deviceId) {
        Map<String, Object> result = baseResult(deviceId);
        try {
            boolean success = collectionService.stopDevice(deviceId);
            if (success) {
                result.put("status", "success");
                result.put("message", "设备已停止");
            } else {
                result.put("status", "error");
                result.put("message", "设备停止失败或已停止");
            }
        } catch (Exception e) {
            log.error("停止设备失败: {}", deviceId, e);
            result.put("status", "error");
            result.put("message", "停止异常: " + e.getMessage());
        }
        return result;
    }

    @PostMapping("/reload")
    public Map<String, Object> reloadAllDevices() {
        Map<String, Object> result = new HashMap<>();
        result.put("timestamp", System.currentTimeMillis());
        try {
            collectionService.reloadAllDevices();
            result.put("status", "success");
            result.put("message", "已重新加载所有设备");
        } catch (Exception e) {
            log.error("重新加载所有设备失败", e);
            result.put("status", "error");
            result.put("message", "重新加载异常: " + e.getMessage());
        }
        return result;
    }

    @GetMapping("/{deviceId}/status")
    public Map<String, Object> getDeviceStatus(@PathVariable String deviceId) {
        Map<String, Object> result = baseResult(deviceId);
        try {
            Map<String, Object> status = collectionService.getDeviceStatus(deviceId);
            result.put("status", "success");
            result.put("data", status);
        } catch (Exception e) {
            log.error("获取设备状态失败: {}", deviceId, e);
            result.put("status", "error");
            result.put("message", "获取状态失败: " + e.getMessage());
        }
        return result;
    }

    @GetMapping("/statistics")
    public Map<String, Object> getAllStatistics() {
        Map<String, Object> result = new HashMap<>();
        result.put("timestamp", System.currentTimeMillis());
        try {
            Map<String, Map<String, Object>> stats = collectionService.getAllStatistics();
            result.put("status", "success");
            result.put("data", stats);
        } catch (Exception e) {
            log.error("获取采集统计失败", e);
            result.put("status", "error");
            result.put("message", "获取统计异常: " + e.getMessage());
        }
        return result;
    }

    @GetMapping("/running")
    public Map<String, Object> getRunningDevices() {
        Map<String, Object> result = new HashMap<>();
        result.put("timestamp", System.currentTimeMillis());
        try {
            List<String> devices = collectionService.getRunningDevices();
            result.put("status", "success");
            result.put("data", devices);
            result.put("count", devices.size());
        } catch (Exception e) {
            log.error("获取运行设备列表失败", e);
            result.put("status", "error");
            result.put("message", "获取设备列表异常: " + e.getMessage());
        }
        return result;
    }

    @GetMapping("/{deviceId}/running")
    public Map<String, Object> isDeviceRunning(@PathVariable String deviceId) {
        Map<String, Object> result = baseResult(deviceId);
        try {
            boolean running = collectionService.isDeviceRunning(deviceId);
            result.put("status", "success");
            result.put("running", running);
        } catch (Exception e) {
            log.error("查询设备运行状态失败: {}", deviceId, e);
            result.put("status", "error");
            result.put("message", "查询运行状态异常: " + e.getMessage());
        }
        return result;
    }

    private Map<String, Object> baseResult(String deviceId) {
        Map<String, Object> result = new HashMap<>();
        result.put("deviceId", deviceId);
        result.put("timestamp", System.currentTimeMillis());
        return result;
    }
}

