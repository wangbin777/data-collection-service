package com.wangbin.collector.common.domain.entity;

import lombok.Data;

import java.util.Date;
import java.util.Map;
import java.util.Objects;

/**
 * 设备信息实体类
 *
 * 用于存储和管理设备的基本信息、连接配置、状态等
 * 支持多种通信协议和连接方式
 */
@Data
public class DeviceInfo {

    // ==================== 基本信息 ====================

    /** 主键ID */
    private Long id;

    /** 设备唯一标识 */
    private String deviceId;

    /** 设备名称 */
    private String deviceName;

    /** 设备别名/显示名称 */
    private String deviceAlias;

    /** 产品密钥，用于设备认证 */
    private String productKey;

    /** 产品名称 */
    private String productName;

    /** 设备分组ID */
    private String groupId;

    /** 设备分组名称 */
    private String groupName;

    // ==================== 连接配置 ====================

    /** 通信协议类型 */
    private String protocolType;

    /** 连接类型: TCP, HTTP, MQTT, WEBSOCKET, etc. */
    private String connectionType;

    /** 设备IP地址 */
    private String ipAddress;

    /** 设备端口号 */
    private Integer port;

    /** 协议相关配置，JSON格式 */
    private Map<String, Object> protocolConfig;

    /** 连接相关配置，JSON格式 */
    private Map<String, Object> connectionConfig;

    /** 认证相关配置，JSON格式 */
    private Map<String, Object> authConfig;

    // ==================== 采集配置 ====================

    /** 数据采集间隔（秒） */
    private Integer collectionInterval = 2000;

    /** 数据上报间隔（秒） */
    private Integer reportInterval;

    // ==================== 设备状态 ====================

    /** 设备状态: ONLINE, OFFLINE, ERROR */
    private String status;

    /** 最后在线时间 */
    private Date lastOnlineTime;

    /** 最后离线时间 */
    private Date lastOfflineTime;

    /** 最后错误信息 */
    private String lastError;

    /** 当前重试次数 */
    private Integer retryCount;

    /** 最大重试次数 */
    private Integer maxRetryCount;

    // ==================== 系统字段 ====================

    /** 创建时间 */
    private Date createTime;

    /** 更新时间 */
    private Date updateTime;

    /** 备注信息 */
    private String remark;

    // ==================== 业务方法 ====================

    /**
     * 判断设备是否在线
     *
     * @return 在线返回true，否则返回false
     */
    public boolean isOnline() {
        return "ONLINE".equals(status);
    }

    /**
     * 判断设备是否离线
     *
     * @return 离线返回true，否则返回false
     */
    public boolean isOffline() {
        return "OFFLINE".equals(status);
    }

    /**
     * 判断设备是否处于错误状态
     *
     * @return 错误状态返回true，否则返回false
     */
    public boolean isError() {
        return "ERROR".equals(status);
    }

    /**
     * 判断设备是否需要重连
     * 条件：设备离线且重试次数未达到最大值
     *
     * @return 需要重连返回true，否则返回false
     */
    public boolean needReconnect() {
        return !isOnline() && getCurrentRetryCount() < getEffectiveMaxRetryCount();
    }

    /**
     * 增加重试计数
     */
    public void incrementRetryCount() {
        if (retryCount == null) {
            retryCount = 0;
        }
        retryCount++;
    }

    /**
     * 重置重试计数
     */
    public void resetRetryCount() {
        retryCount = 0;
    }

    /**
     * 获取当前重试次数
     *
     * @return 当前重试次数，如果为null返回0
     */
    public int getCurrentRetryCount() {
        return retryCount != null ? retryCount : 0;
    }

    /**
     * 获取有效的最大重试次数
     *
     * @return 最大重试次数，如果为null返回默认值3
     */
    public int getEffectiveMaxRetryCount() {
        return maxRetryCount != null ? maxRetryCount : 3;
    }

    /**
     * 获取有效的采集间隔
     *
     * @return 采集间隔（秒），如果为null返回默认值1
     */
    public int getEffectiveCollectionInterval() {
        return collectionInterval != null ? collectionInterval : 1;
    }

    /**
     * 获取有效的上报间隔
     *
     * @return 上报间隔（秒），如果为null返回默认值5
     */
    public int getEffectiveReportInterval() {
        return reportInterval != null ? reportInterval : 5;
    }

    /**
     * 设置设备为在线状态
     */
    public void setOnline() {
        this.status = "ONLINE";
        this.lastOnlineTime = new Date();
        resetRetryCount();
        this.lastError = null;
    }

    /**
     * 设置设备为离线状态
     *
     * @param error 错误信息，可选
     */
    public void setOffline(String error) {
        this.status = "OFFLINE";
        this.lastOfflineTime = new Date();
        this.lastError = error;
    }

    /**
     * 设置设备为错误状态
     *
     * @param error 错误信息
     */
    public void setError(String error) {
        this.status = "ERROR";
        this.lastError = error;
    }

    /**
     * 获取连接地址
     *
     * @return 完整的连接地址，如：192.168.1.100:502
     */
    public String getConnectionAddress() {
        if (ipAddress == null || ipAddress.trim().isEmpty()) {
            return "";
        }
        if (port != null && port > 0) {
            return ipAddress + ":" + port;
        }
        return ipAddress;
    }

    /**
     * 从协议配置中获取指定参数的值
     *
     * @param key 参数键
     * @return 参数值，不存在返回null
     */
    public Object getProtocolParam(String key) {
        if (protocolConfig == null || key == null) {
            return null;
        }
        return protocolConfig.get(key);
    }

    /**
     * 从连接配置中获取指定参数的值
     *
     * @param key 参数键
     * @return 参数值，不存在返回null
     */
    public Object getConnectionParam(String key) {
        if (connectionConfig == null || key == null) {
            return null;
        }
        return connectionConfig.get(key);
    }

    /**
     * 从认证配置中获取指定参数的值
     *
     * @param key 参数键
     * @return 参数值，不存在返回null
     */
    public Object getAuthParam(String key) {
        if (authConfig == null || key == null) {
            return null;
        }
        return authConfig.get(key);
    }

    /**
     * 验证设备信息的必填字段
     *
     * @return 验证结果，所有必填字段存在返回true，否则返回false
     */
    public boolean validateRequiredFields() {
        return deviceId != null && !deviceId.trim().isEmpty() &&
                deviceName != null && !deviceName.trim().isEmpty() &&
                ipAddress != null && !ipAddress.trim().isEmpty() &&
                port != null && port > 0 &&
                protocolType != null && !protocolType.trim().isEmpty();
    }

    /**
     * 获取设备显示名称
     * 优先使用别名，如果没有则使用设备名称
     *
     * @return 设备显示名称
     */
    public String getDisplayName() {
        if (deviceAlias != null && !deviceAlias.trim().isEmpty()) {
            return deviceAlias.trim();
        }
        return deviceName != null ? deviceName : "未知设备";
    }

    /**
     * 判断设备是否支持连接类型
     *
     * @param connectionType 要检查的连接类型
     * @return 支持返回true，否则返回false
     */
    public boolean supportsConnectionType(String connectionType) {
        return this.connectionType != null &&
                this.connectionType.equalsIgnoreCase(connectionType);
    }

    /**
     * 判断设备是否支持协议类型
     *
     * @param protocolType 要检查的协议类型
     * @return 支持返回true，否则返回false
     */
    public boolean supportsProtocolType(String protocolType) {
        return this.protocolType != null &&
                this.protocolType.equalsIgnoreCase(protocolType);
    }

    /**
     * 生成设备的唯一标识字符串
     *
     * @return 设备唯一标识
     */
    public String getUniqueIdentifier() {
        return String.format("%s:%s", deviceId, getConnectionAddress());
    }

    /**
     * 获取设备离线时长（毫秒）
     *
     * @return 离线时长，如果设备在线或最后离线时间为null返回0
     */
    public long getOfflineDuration() {
        if (isOnline() || lastOfflineTime == null) {
            return 0;
        }
        return System.currentTimeMillis() - lastOfflineTime.getTime();
    }

    /**
     * 判断设备是否长时间离线
     * 默认超过5分钟视为长时间离线
     *
     * @return 长时间离线返回true，否则返回false
     */
    public boolean isLongTimeOffline() {
        return getOfflineDuration() > 5 * 60 * 1000; // 5分钟
    }

    /**
     * 判断设备配置是否发生变化
     *
     * @param other 另一个设备配置
     * @return 配置变化返回true，否则返回false
     */
    public boolean isConfigChanged(DeviceInfo other) {
        if (other == null) {
            return false;
        }
        return !Objects.equals(this.ipAddress, other.ipAddress) ||
                !Objects.equals(this.port, other.port) ||
                !Objects.equals(this.protocolType, other.protocolType) ||
                !Objects.equals(this.connectionType, other.connectionType) ||
                !Objects.equals(this.protocolConfig, other.protocolConfig) ||
                !Objects.equals(this.collectionInterval, other.collectionInterval) ||
                !Objects.equals(this.reportInterval, other.reportInterval);
    }

    /**
     * 设备状态描述
     *
     * @return 设备状态描述字符串
     */
    public String getStatusDescription() {
        if (isOnline()) {
            return "在线";
        } else if (isOffline()) {
            return "离线" + (lastError != null ? "(" + lastError + ")" : "");
        } else if (isError()) {
            return "错误" + (lastError != null ? "(" + lastError + ")" : "");
        }
        return "未知";
    }
}