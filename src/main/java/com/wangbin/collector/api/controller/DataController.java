package com.wangbin.collector.api.controller;

import com.wangbin.collector.core.cache.manager.MultiLevelCacheManager;
import com.wangbin.collector.core.cache.model.CacheKey;
import com.wangbin.collector.core.config.manager.ConfigManager;
import com.wangbin.collector.common.domain.entity.DataPoint;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 数据查询控制器
 * 提供API接口查询采集的数据
 */
@Slf4j
@RestController
@RequestMapping("/api/data")
public class DataController {

    @Autowired
    @Qualifier("multiLevelCacheManager")
    private MultiLevelCacheManager cacheManager;

    @Autowired
    private ConfigManager configManager;

    /**
     * 查询指定设备的指定数据点的实时值
     */
    @GetMapping("/device/{deviceId}/point/{pointId}")
    public Map<String, Object> getPointData(
            @PathVariable String deviceId,
            @PathVariable String pointId) {
        
        Map<String, Object> result = new HashMap<>();
        try {
            // 构建缓存键
            CacheKey cacheKey = CacheKey.dataKey(deviceId, pointId);
            
            // 从缓存中获取数据
            Object value = cacheManager.get(cacheKey);
            
            // 获取数据点信息
            DataPoint dataPoint = configManager.getDataPoint(deviceId, pointId);
            
            if (dataPoint != null) {
                result.put("deviceId", deviceId);
                result.put("pointId", pointId);
                result.put("pointName", dataPoint.getPointName());
                result.put("pointCode", dataPoint.getPointCode());
                result.put("dataType", dataPoint.getDataType());
                result.put("value", value);
                result.put("timestamp", System.currentTimeMillis());
                result.put("status", "success");
            } else {
                result.put("status", "error");
                result.put("message", "数据点不存在");
            }
        } catch (Exception e) {
            log.error("查询数据失败: deviceId={}, pointId={}", deviceId, pointId, e);
            result.put("status", "error");
            result.put("message", "查询失败: " + e.getMessage());
        }
        
        return result;
    }

    /**
     * 查询指定设备的所有数据点的实时值
     */
    @GetMapping("/device/{deviceId}")
    public Map<String, Object> getDeviceData(
            @PathVariable String deviceId,
            @RequestParam(required = false) List<String> pointIds) {
        
        Map<String, Object> result = new HashMap<>();
        try {
            // 获取设备的所有数据点
            List<DataPoint> dataPoints = configManager.getDataPoints(deviceId);
            
            if (dataPoints.isEmpty()) {
                result.put("status", "error");
                result.put("message", "设备不存在或无数据点");
                return result;
            }
            
            // 如果指定了pointIds，只查询这些数据点
            if (pointIds != null && !pointIds.isEmpty()) {
                dataPoints = dataPoints.stream()
                        .filter(point -> pointIds.contains(point.getPointId()))
                        .toList();
            }
            
            // 批量查询数据
            List<CacheKey> cacheKeys = new ArrayList<>();
            for (DataPoint point : dataPoints) {
                cacheKeys.add(CacheKey.dataKey(deviceId, point.getPointId()));
            }
            
            Map<CacheKey, Object> values = cacheManager.getAll(cacheKeys);
            
            // 组织结果
            Map<String, Map<String, Object>> dataMap = new HashMap<>();
            for (DataPoint point : dataPoints) {
                CacheKey cacheKey = CacheKey.dataKey(deviceId, point.getPointId());
                Object value = values.get(cacheKey);
                
                Map<String, Object> pointData = new HashMap<>();
                pointData.put("pointId", point.getPointId());
                pointData.put("pointName", point.getPointName());
                pointData.put("pointCode", point.getPointCode());
                pointData.put("dataType", point.getDataType());
                pointData.put("value", value);
                
                dataMap.put(point.getPointId(), pointData);
            }
            
            result.put("deviceId", deviceId);
            result.put("dataCount", dataMap.size());
            result.put("data", dataMap);
            result.put("timestamp", System.currentTimeMillis());
            result.put("status", "success");
            
        } catch (Exception e) {
            log.error("查询设备数据失败: deviceId={}", deviceId, e);
            result.put("status", "error");
            result.put("message", "查询失败: " + e.getMessage());
        }
        
        return result;
    }

    /**
     * 查询所有设备的基本信息
     */
    @GetMapping("/devices")
    public Map<String, Object> getAllDevices() {
        
        Map<String, Object> result = new HashMap<>();
        try {
            // 获取所有设备ID
            List<String> deviceIds = configManager.getAllDeviceIds();
            
            List<Map<String, Object>> devices = new ArrayList<>();
            for (String deviceId : deviceIds) {
                Map<String, Object> deviceInfo = new HashMap<>();
                deviceInfo.put("deviceId", deviceId);
                
                // 获取设备数据点数量
                List<DataPoint> dataPoints = configManager.getDataPoints(deviceId);
                deviceInfo.put("pointCount", dataPoints.size());
                
                devices.add(deviceInfo);
            }
            
            result.put("deviceCount", devices.size());
            result.put("devices", devices);
            result.put("timestamp", System.currentTimeMillis());
            result.put("status", "success");
            
        } catch (Exception e) {
            log.error("查询所有设备失败", e);
            result.put("status", "error");
            result.put("message", "查询失败: " + e.getMessage());
        }
        
        return result;
    }

    /**
     * 查询指定设备的所有数据点配置
     */
    @GetMapping("/device/{deviceId}/points")
    public Map<String, Object> getDevicePoints(
            @PathVariable String deviceId) {
        
        Map<String, Object> result = new HashMap<>();
        try {
            // 获取设备的所有数据点
            List<DataPoint> dataPoints = configManager.getDataPoints(deviceId);
            
            List<Map<String, Object>> pointsInfo = new ArrayList<>();
            for (DataPoint point : dataPoints) {
                Map<String, Object> pointInfo = new HashMap<>();
                pointInfo.put("pointId", point.getPointId());
                pointInfo.put("pointName", point.getPointName());
                pointInfo.put("pointCode", point.getPointCode());
                pointInfo.put("dataType", point.getDataType());
                pointInfo.put("unit", point.getUnit());
                //pointInfo.put("description", point.getDescription());
                
                pointsInfo.add(pointInfo);
            }
            
            result.put("deviceId", deviceId);
            result.put("pointCount", pointsInfo.size());
            result.put("points", pointsInfo);
            result.put("timestamp", System.currentTimeMillis());
            result.put("status", "success");
            
        } catch (Exception e) {
            log.error("查询设备数据点失败: deviceId={}", deviceId, e);
            result.put("status", "error");
            result.put("message", "查询失败: " + e.getMessage());
        }
        
        return result;
    }
}