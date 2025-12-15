package com.wangbin.collector.common.domain.entity;

import lombok.Data;

import java.util.Date;
import java.util.Map;

/**
 * 连接信息实体类
 * 用于记录和管理设备连接的状态、配置和统计信息
 */
@Data
public class ConnectionInfo {

    // ==================== 基础信息 ====================

    /** 数据库主键ID */
    private Long id;

    /** 连接唯一标识符 */
    private String connectionId;

    /** 关联的设备ID */
    private String deviceId;

    /** 设备名称（冗余字段，便于显示） */
    private String deviceName;

    // ==================== 连接配置 ====================

    /** 连接类型: TCP, HTTP, MQTT, WEBSOCKET */
    private String connectionType;

    /** 协议类型: MODBUS, OPC_UA, SNMP等 */
    private String protocolType;

    /** 远程地址（设备IP地址或域名） */
    private String remoteAddress;

    /** 远程端口号 */
    private Integer remotePort;

    /** 本地地址（采集服务IP地址） */
    private String localAddress;

    /** 本地端口号 */
    private Integer localPort;

    // ==================== 连接状态 ====================

    /** 连接状态: CONNECTING(连接中), CONNECTED(已连接), DISCONNECTED(已断开), ERROR(错误) */
    private String status;

    /** 连接建立时间 */
    private Date connectTime;

    /** 连接断开时间 */
    private Date disconnectTime;

    /** 连接持续时间（毫秒） */
    private Long duration;

    /** 当前重试次数 */
    private Integer retryCount;

    /** 最后一次错误信息 */
    private String lastError;

    // ==================== 连接参数 ====================

    /** 连接参数配置，JSON格式存储 */
    private Map<String, Object> connectionParams;

    /** 连接统计信息 */
    private Map<String, Object> stats;

    // ==================== 监控信息 ====================

    /** 最后心跳时间，用于检测连接是否存活 */
    private Date lastHeartbeatTime;

    /** 最后数据接收时间，用于监控数据流 */
    private Date lastDataTime;

    // ==================== 系统信息 ====================

    /** 记录创建时间 */
    private Date createTime;

    /** 记录最后更新时间 */
    private Date updateTime;

    /**
     * 连接统计信息内部类
     * 用于记录连接的各项统计指标
     */
    @Data
    public static class ConnectionStats {
        /** 总发送字节数 */
        private Long totalBytesSent;

        /** 总接收字节数 */
        private Long totalBytesReceived;

        /** 总发送消息数 */
        private Long totalMessagesSent;

        /** 总接收消息数 */
        private Long totalMessagesReceived;

        /** 总错误次数 */
        private Long totalErrors;

        /** 平均响应时间（毫秒） */
        private Double averageResponseTime;

        /** 最大响应时间（毫秒） */
        private Double maxResponseTime;

        /** 最后响应时间 */
        private Date lastResponseTime;

        /**
         * 构造函数，初始化统计值为0
         */
        public ConnectionStats() {
            this.totalBytesSent = 0L;
            this.totalBytesReceived = 0L;
            this.totalMessagesSent = 0L;
            this.totalMessagesReceived = 0L;
            this.totalErrors = 0L;
        }

        /**
         * 增加发送字节数
         * @param bytes 发送的字节数
         */
        public void addBytesSent(long bytes) {
            totalBytesSent += bytes;
        }

        /**
         * 增加接收字节数
         * @param bytes 接收的字节数
         */
        public void addBytesReceived(long bytes) {
            totalBytesReceived += bytes;
        }

        /**
         * 增加发送消息数（每次发送成功调用）
         */
        public void addMessageSent() {
            totalMessagesSent++;
        }

        /**
         * 增加接收消息数（每次接收成功调用）
         */
        public void addMessageReceived() {
            totalMessagesReceived++;
        }

        /**
         * 增加错误次数（每次发生错误时调用）
         */
        public void addError() {
            totalErrors++;
        }

        /**
         * 计算连接成功率（百分比）
         * 成功率 = (总消息数 - 错误数) / 总消息数 × 100%
         * @return 成功率百分比，0.0-100.0
         */
        public double getSuccessRate() {
            long total = totalMessagesSent + totalMessagesReceived;
            if (total == 0) {
                return 0.0;
            }
            return (total - totalErrors) * 100.0 / total;
        }
    }

    /**
     * 判断连接是否处于活跃状态
     * 活跃条件：状态为CONNECTED且最后心跳时间在2分钟内
     * @return true-活跃，false-不活跃
     */
    public boolean isActive() {
        return "CONNECTED".equals(status) && lastHeartbeatTime != null
                && (System.currentTimeMillis() - lastHeartbeatTime.getTime()) < 120000; // 2分钟
    }

    /**
     * 更新心跳时间到当前时间
     * 用于心跳机制，保持连接活跃性检测
     */
    public void updateHeartbeat() {
        this.lastHeartbeatTime = new Date();
    }

    /**
     * 更新数据接收时间到当前时间
     * 用于判断数据流的活跃性
     */
    public void updateDataTime() {
        this.lastDataTime = new Date();
    }

    /**
     * 计算连接持续时间
     * 如果连接已断开，计算从连接到断开的时间
     * 如果连接还在活跃，计算从连接到当前的时间
     */
    public void calculateDuration() {
        if (connectTime != null) {
            Date endTime = disconnectTime != null ? disconnectTime : new Date();
            this.duration = endTime.getTime() - connectTime.getTime();
        }
    }
}