package com.wangbin.collector.monitor.metrics;

import com.wangbin.collector.common.domain.enums.ConnectionStatus;
import lombok.Builder;
import lombok.Data;

/**
 * 单个设备连接状态快照。
 */
@Data
@Builder
public class DeviceConnectionSnapshot {

    private final String deviceId;
    private final ConnectionStatus status;
    private final boolean connected;

    private final long lastActivityTime;
    private final long idleTime;
    private final long bytesSent;
    private final long bytesReceived;
    private final long errors;
    private final double successRate;
    private final long connectionDuration;
}
