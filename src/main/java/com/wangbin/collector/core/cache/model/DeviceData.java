package com.wangbin.collector.core.cache.model;

import lombok.Data;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * 设备数据模型
 * 用于封装整个设备的所有点位数据
 */
@Data
public class DeviceData implements Serializable {

    private static final long serialVersionUID = 1L;

    // 设备ID
    private String deviceId;

    // 点位数据映射 (pointId -> PointData)
    private Map<String, PointData> pointDataMap;

    // 数据采集时间戳（毫秒）
    private long timestamp;

    // 设备状态
    private String status;

    // 数据质量统计（0-100）
    private int qualityScore;

    public DeviceData() {
        this.pointDataMap = new HashMap<>();
        this.timestamp = System.currentTimeMillis();
        this.status = "ONLINE";
        this.qualityScore = 100;
    }

    public DeviceData(String deviceId) {
        this();
        this.deviceId = deviceId;
    }

    /**
     * 添加或更新点位数据
     * @param pointData 点位数据
     */
    public void addOrUpdatePointData(PointData pointData) {
        if (pointData != null && pointData.getPointId() != null) {
            pointDataMap.put(pointData.getPointId(), pointData);
            // 更新设备数据时间戳为最新点位的时间戳
            this.timestamp = Math.max(this.timestamp, pointData.getTimestamp());
            // 简单的质量评分计算（取所有点位质量的平均值）
            updateQualityScore();
        }
    }

    /**
     * 更新数据质量评分
     */
    private void updateQualityScore() {
        if (pointDataMap.isEmpty()) {
            this.qualityScore = 0;
            return;
        }
        int totalQuality = pointDataMap.values().stream()
                .mapToInt(PointData::getQuality)
                .sum();
        this.qualityScore = totalQuality / pointDataMap.size();
    }

    /**
     * 获取指定点位数据
     * @param pointId 点位ID
     * @return 点位数据
     */
    public PointData getPointData(String pointId) {
        return pointDataMap.get(pointId);
    }

    /**
     * 移除点位数据
     * @param pointId 点位ID
     */
    public void removePointData(String pointId) {
        pointDataMap.remove(pointId);
        updateQualityScore();
    }

    /**
     * 获取点位数量
     * @return 点位数量
     */
    public int getPointCount() {
        return pointDataMap.size();
    }
}