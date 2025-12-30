package com.wangbin.collector.monitor.metrics;

import com.wangbin.collector.core.connection.adapter.ConnectionAdapter;
import com.wangbin.collector.core.connection.manager.ConnectionManager;
import com.wangbin.collector.core.connection.model.ConnectionMetrics;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

/**
 * 设备与连接监控服务。
 */
@Service
@RequiredArgsConstructor
public class DeviceMonitorService {

    private final ConnectionManager connectionManager;

    public DeviceStatusSnapshot getDeviceStatus() {
        List<ConnectionAdapter> allConnections = connectionManager.getAllConnections();
        List<DeviceConnectionSnapshot> snapshots = allConnections.stream()
                .map(this::buildSnapshot)
                .collect(Collectors.toList());

        int activeConnections = (int) snapshots.stream()
                .filter(DeviceConnectionSnapshot::isConnected)
                .count();

        return DeviceStatusSnapshot.builder()
                .totalConnections(snapshots.size())
                .activeConnections(activeConnections)
                .connections(snapshots)
                .build();
    }

    private DeviceConnectionSnapshot buildSnapshot(ConnectionAdapter connection) {
        ConnectionMetrics metrics = connection.getMetrics();
        long idleTime = metrics != null ? metrics.getIdleTime() : 0;
        double successRate = metrics != null ? metrics.getSuccessRate() : 0.0;

        return DeviceConnectionSnapshot.builder()
                .deviceId(connection.getDeviceId())
                .status(connection.getStatus())
                .connected(connection.isConnected())
                .lastActivityTime(metrics != null ? metrics.getLastActivityTime() : 0L)
                .idleTime(idleTime)
                .bytesSent(metrics != null ? metrics.getBytesSent() : 0L)
                .bytesReceived(metrics != null ? metrics.getBytesReceived() : 0L)
                .errors(metrics != null ? metrics.getErrors() : 0L)
                .successRate(successRate)
                .connectionDuration(metrics != null ? metrics.getConnectionDuration() : 0L)
                .build();
    }
}
