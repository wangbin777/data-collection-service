package com.wangbin.collector.core.config.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 配置更新事件
 *
 * 用于在配置发生变化时通知相关组件进行更新
 * 支持多种配置类型的更新事件
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ConfigUpdateEvent {

    // ==================== 事件基本信息 ====================

    /**
     * 事件ID，用于唯一标识一个配置更新事件
     */
    private String eventId;

    /**
     * 配置类型
     * 可选值:
     *   - "device": 设备配置更新
     *   - "points": 数据点配置更新
     *   - "connection": 连接配置更新
     *   - "collection": 采集配置更新
     *   - "all": 全部配置更新
     */
    private String configType;

    /**
     * 设备ID
     * 当配置类型为device/points/connection/collection时，此字段表示受影响的设备
     * 当配置类型为all时，此字段可能为null
     */
    private String deviceId;

    /**
     * 事件源，记录哪个服务触发了此事件
     * 可选值: "config-sync", "manual", "api", "system"
     */
    private String source;

    /**
     * 事件创建时间
     */
    private Date createTime;

    /**
     * 事件更新时间
     */
    private Date updateTime;

    // ==================== 事件状态信息 ====================

    /**
     * 事件状态
     * 可选值: "pending", "processing", "completed", "failed"
     */
    private String status;

    /**
     * 事件处理结果消息
     */
    private String message;

    /**
     * 事件处理的异常信息
     */
    private String error;

    /**
     * 事件处理的开始时间
     */
    private Date processStartTime;

    /**
     * 事件处理的结束时间
     */
    private Date processEndTime;

    /**
     * 事件处理的持续时间（毫秒）
     */
    private Long processDuration;

    // ==================== 配置变更详情 ====================

    /**
     * 连接配置是否发生变化
     * 当configType为"device"时，此字段表示设备的连接参数是否变化
     */
    private Boolean connectionChanged;

    /**
     * 数据点数量变化
     * 当configType为"points"时，此字段表示数据点数量的变化
     * 正数表示新增，负数表示减少
     */
    private Integer pointCountChange;

    /**
     * 配置版本号
     */
    private Long configVersion;

    /**
     * 上次配置版本号
     */
    private Long previousVersion;

    /**
     * 变更的配置项详情
     * key: 配置项名称
     * value: 变更详情（旧值->新值）
     */
    private Map<String, String> changeDetails;

    /**
     * 影响的组件列表
     * 用于记录哪些组件需要处理此事件
     */
    private String[] affectedComponents;

    // ==================== 上下文信息 ====================

    /**
     * 操作用户
     */
    private String operator;

    /**
     * 操作IP地址
     */
    private String operatorIp;

    /**
     * 额外参数，用于存储事件相关的扩展信息
     */
    private Map<String, Object> extraParams;

    // ==================== 业务方法 ====================

    /**
     * 静态工厂方法，创建设备配置更新事件
     *
     * @param deviceId 设备ID
     * @param connectionChanged 连接是否变化
     * @return 配置更新事件
     */
    public static ConfigUpdateEvent createDeviceUpdateEvent(String deviceId, boolean connectionChanged) {
        return ConfigUpdateEvent.builder()
                .eventId(generateEventId())
                .configType("device")
                .deviceId(deviceId)
                .source("config-sync")
                .createTime(new Date())
                .status("pending")
                .connectionChanged(connectionChanged)
                .build();
    }

    /**
     * 静态工厂方法，创建数据点配置更新事件
     *
     * @param deviceId 设备ID
     * @param pointCountChange 数据点数量变化
     * @return 配置更新事件
     */
    public static ConfigUpdateEvent createPointsUpdateEvent(String deviceId, int pointCountChange) {
        return ConfigUpdateEvent.builder()
                .eventId(generateEventId())
                .configType("points")
                .deviceId(deviceId)
                .source("config-sync")
                .createTime(new Date())
                .status("pending")
                .pointCountChange(pointCountChange)
                .build();
    }

    /**
     * 静态工厂方法，创建连接配置更新事件
     *
     * @param deviceId 设备ID
     * @return 配置更新事件
     */
    public static ConfigUpdateEvent createConnectionUpdateEvent(String deviceId) {
        return ConfigUpdateEvent.builder()
                .eventId(generateEventId())
                .configType("connection")
                .deviceId(deviceId)
                .source("config-sync")
                .createTime(new Date())
                .status("pending")
                .connectionChanged(true)
                .build();
    }

    /**
     * 静态工厂方法，创建采集配置更新事件
     *
     * @param deviceId 设备ID
     * @return 配置更新事件
     */
    public static ConfigUpdateEvent createCollectionUpdateEvent(String deviceId) {
        return ConfigUpdateEvent.builder()
                .eventId(generateEventId())
                .configType("collection")
                .deviceId(deviceId)
                .source("config-sync")
                .createTime(new Date())
                .status("pending")
                .build();
    }

    /**
     * 静态工厂方法，创建全量配置更新事件
     *
     * @return 配置更新事件
     */
    public static ConfigUpdateEvent createAllUpdateEvent() {
        return ConfigUpdateEvent.builder()
                .eventId(generateEventId())
                .configType("all")
                .source("system")
                .createTime(new Date())
                .status("pending")
                .build();
    }

    /**
     * 生成事件ID
     * 格式: EVT_时间戳_随机数
     *
     * @return 事件ID
     */
    private static String generateEventId() {
        return String.format("EVT_%d_%d",
                System.currentTimeMillis(),
                (int)(Math.random() * 1000));
    }

    /**
     * 开始处理事件
     */
    public void startProcessing() {
        this.status = "processing";
        this.processStartTime = new Date();
        this.updateTime = new Date();
    }

    /**
     * 完成事件处理
     *
     * @param message 处理结果消息
     */
    public void complete(String message) {
        this.status = "completed";
        this.message = message;
        this.processEndTime = new Date();
        this.updateTime = new Date();
        calculateProcessDuration();
    }

    /**
     * 事件处理失败
     *
     * @param error 错误信息
     */
    public void fail(String error) {
        this.status = "failed";
        this.error = error;
        this.processEndTime = new Date();
        this.updateTime = new Date();
        calculateProcessDuration();
    }

    /**
     * 计算处理持续时间
     */
    private void calculateProcessDuration() {
        if (processStartTime != null && processEndTime != null) {
            this.processDuration = processEndTime.getTime() - processStartTime.getTime();
        }
    }

    /**
     * 检查事件是否已处理完成
     *
     * @return 完成返回true，否则返回false
     */
    public boolean isCompleted() {
        return "completed".equals(status);
    }

    /**
     * 检查事件是否处理失败
     *
     * @return 失败返回true，否则返回false
     */
    public boolean isFailed() {
        return "failed".equals(status);
    }

    /**
     * 检查事件是否在处理中
     *
     * @return 处理中返回true，否则返回false
     */
    public boolean isProcessing() {
        return "processing".equals(status);
    }

    /**
     * 检查事件是否等待处理
     *
     * @return 等待处理返回true，否则返回false
     */
    public boolean isPending() {
        return "pending".equals(status);
    }

    /**
     * 添加变更详情
     *
     * @param fieldName 字段名称
     * @param oldValue 旧值
     * @param newValue 新值
     */
    public void addChangeDetail(String fieldName, Object oldValue, Object newValue) {
        if (changeDetails == null) {
            changeDetails = new HashMap<>();
        }

        String changeDetail = String.format("%s -> %s",
                oldValue != null ? oldValue.toString() : "null",
                newValue != null ? newValue.toString() : "null");

        changeDetails.put(fieldName, changeDetail);
    }

    /**
     * 添加额外参数
     *
     * @param key 参数键
     * @param value 参数值
     */
    public void addExtraParam(String key, Object value) {
        if (extraParams == null) {
            extraParams = new HashMap<>();
        }
        extraParams.put(key, value);
    }

    /**
     * 获取额外参数
     *
     * @param key 参数键
     * @return 参数值，不存在返回null
     */
    public Object getExtraParam(String key) {
        if (extraParams == null || key == null) {
            return null;
        }
        return extraParams.get(key);
    }

    /**
     * 判断是否影响指定组件
     *
     * @param component 组件名称
     * @return 影响返回true，否则返回false
     */
    public boolean affectsComponent(String component) {
        if (affectedComponents == null || component == null) {
            return false;
        }

        for (String affectedComponent : affectedComponents) {
            if (component.equals(affectedComponent)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 设置影响的组件列表
     *
     * @param components 组件名称数组
     */
    public void setAffectedComponents(String... components) {
        this.affectedComponents = components;
    }

    /**
     * 获取事件摘要信息
     *
     * @return 事件摘要
     */
    public String getSummary() {
        StringBuilder summary = new StringBuilder();
        summary.append("配置更新事件: ").append(eventId).append("\n");
        summary.append("类型: ").append(configType).append("\n");
        summary.append("设备: ").append(deviceId != null ? deviceId : "全部").append("\n");
        summary.append("状态: ").append(status).append("\n");
        summary.append("创建时间: ").append(createTime).append("\n");

        if (connectionChanged != null && connectionChanged) {
            summary.append("连接配置已变更\n");
        }

        if (pointCountChange != null && pointCountChange != 0) {
            summary.append("数据点数量变化: ").append(pointCountChange).append("\n");
        }

        if (message != null) {
            summary.append("消息: ").append(message).append("\n");
        }

        if (error != null) {
            summary.append("错误: ").append(error).append("\n");
        }

        return summary.toString();
    }

    /**
     * 获取事件处理性能信息
     *
     * @return 性能信息字符串
     */
    public String getPerformanceInfo() {
        if (processDuration != null) {
            return String.format("处理耗时: %dms", processDuration);
        }
        return "未处理完成";
    }

    /**
     * 重置事件状态为等待处理
     */
    public void resetToPending() {
        this.status = "pending";
        this.message = null;
        this.error = null;
        this.processStartTime = null;
        this.processEndTime = null;
        this.processDuration = null;
        this.updateTime = new Date();
    }

    /**
     * 克隆事件（浅拷贝）
     *
     * @return 克隆的事件对象
     */
    public ConfigUpdateEvent clone() {
        return ConfigUpdateEvent.builder()
                .eventId(this.eventId)
                .configType(this.configType)
                .deviceId(this.deviceId)
                .source(this.source)
                .createTime(this.createTime)
                .updateTime(this.updateTime)
                .status(this.status)
                .message(this.message)
                .error(this.error)
                .processStartTime(this.processStartTime)
                .processEndTime(this.processEndTime)
                .processDuration(this.processDuration)
                .connectionChanged(this.connectionChanged)
                .pointCountChange(this.pointCountChange)
                .configVersion(this.configVersion)
                .previousVersion(this.previousVersion)
                .changeDetails(this.changeDetails != null ? new HashMap<>(this.changeDetails) : null)
                .affectedComponents(this.affectedComponents != null ? this.affectedComponents.clone() : null)
                .operator(this.operator)
                .operatorIp(this.operatorIp)
                .extraParams(this.extraParams != null ? new HashMap<>(this.extraParams) : null)
                .build();
    }
}