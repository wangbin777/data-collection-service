package com.wangbin.collector.core.connection.model;

import com.wangbin.collector.common.domain.enums.ConnectionStatus;
import lombok.Data;

/**
 * 连接指标
 */
@Data
public class ConnectionMetrics {

    // 连接信息
    private ConnectionStatus status = ConnectionStatus.DISCONNECTED;
    private String connectionId;
    private String deviceId;
    private String remoteAddress;
    private String localAddress;

    // 时间信息
    private long connectTime;
    private long disconnectTime;
    private long authTime;
    private long lastHeartbeatTime;
    private long lastActivityTime;
    private long lastErrorTime;

    // 统计信息
    private long bytesSent;
    private long bytesReceived;
    private long messagesSent;
    private long messagesReceived;
    private long errors;
    private long heartbeats;
    private long reconnects;

    // 性能指标
    private double averageResponseTime;
    private double maxResponseTime;
    private double minResponseTime;
    private double successRate;
    private long connectionDuration;

    // 错误信息
    private String lastError;
    private String lastErrorStack;

    // 计算连接持续时间
    public long getConnectionDuration() {
        if (connectTime == 0) {
            return 0;
        }
        long endTime = disconnectTime > 0 ? disconnectTime : System.currentTimeMillis();
        return endTime - connectTime;
    }

    // 格式化连接持续时间
    public String getFormattedDuration() {
        long duration = getConnectionDuration();
        if (duration == 0) {
            return "0s";
        }

        long seconds = duration / 1000;
        long minutes = seconds / 60;
        long hours = minutes / 60;
        long days = hours / 24;

        StringBuilder sb = new StringBuilder();
        if (days > 0) {
            sb.append(days).append("d ");
            hours = hours % 24;
        }
        if (hours > 0) {
            sb.append(hours).append("h ");
            minutes = minutes % 60;
        }
        if (minutes > 0) {
            sb.append(minutes).append("m ");
            seconds = seconds % 60;
        }
        if (seconds > 0 || sb.length() == 0) {
            sb.append(seconds).append("s");
        }

        return sb.toString().trim();
    }

    // 计算空闲时间
    public long getIdleTime() {
        if (lastActivityTime == 0) {
            return 0;
        }
        return System.currentTimeMillis() - lastActivityTime;
    }

    // 判断是否超时
    public boolean isTimeout(long timeout) {
        return getIdleTime() > timeout;
    }

    // 获取统计信息摘要
    public String getSummary() {
        return String.format(
                "连接: %s, 状态: %s, 持续时间: %s, 发送: %d/%d, 接收: %d/%d, 错误: %d, 成功率: %.2f%%",
                connectionId, status.getDescription(), getFormattedDuration(),
                messagesSent, bytesSent, messagesReceived, bytesReceived,
                errors, successRate
        );
    }

    // 重置指标
    public void reset() {
        bytesSent = 0;
        bytesReceived = 0;
        messagesSent = 0;
        messagesReceived = 0;
        errors = 0;
        heartbeats = 0;
        reconnects = 0;
        averageResponseTime = 0;
        maxResponseTime = 0;
        minResponseTime = 0;
        successRate = 0;
        lastError = null;
        lastErrorStack = null;
    }
}
