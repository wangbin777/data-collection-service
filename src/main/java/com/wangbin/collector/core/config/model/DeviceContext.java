package com.wangbin.collector.core.config.model;

import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.common.domain.entity.DeviceConnection;
import com.wangbin.collector.common.domain.entity.DeviceInfo;
import lombok.Getter;
import lombok.ToString;
import org.springframework.beans.BeanUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Immutable snapshot that bundles device level caches for convenient access.
 */
@Getter
@ToString
public class DeviceContext {

    private final String deviceId;
    private final DeviceInfo deviceInfo;
    private final DeviceConnection connectionConfig;
    private final List<DataPoint> dataPoints;

    private DeviceContext(String deviceId,
                          DeviceInfo deviceInfo,
                          DeviceConnection connectionConfig,
                          List<DataPoint> dataPoints) {
        this.deviceId = deviceId;
        this.deviceInfo = deviceInfo;
        this.connectionConfig = connectionConfig;
        this.dataPoints = dataPoints;
    }

    public static DeviceContext of(DeviceInfo deviceInfo,
                                   DeviceConnection connectionConfig,
                                   List<DataPoint> dataPoints) {
        String deviceId = deviceInfo != null ? deviceInfo.getDeviceId() : null;
        DeviceConnection connectionSnapshot = copyConnection(connectionConfig);
        List<DataPoint> pointSnapshot = snapshotPoints(dataPoints);
        return new DeviceContext(deviceId, deviceInfo, connectionSnapshot, pointSnapshot);
    }

    public DeviceConnection copyConnectionConfig() {
        return copyConnection(this.connectionConfig);
    }

    public List<DataPoint> copyDataPoints() {
        return new ArrayList<>(dataPoints);
    }

    private static DeviceConnection copyConnection(DeviceConnection source) {
        if (source == null) {
            return null;
        }
        DeviceConnection target = new DeviceConnection();
        BeanUtils.copyProperties(source, target);
        return target;
    }

    private static List<DataPoint> snapshotPoints(List<DataPoint> points) {
        if (points == null || points.isEmpty()) {
            return Collections.emptyList();
        }
        List<DataPoint> snapshot = new ArrayList<>(points.size());
        for (DataPoint point : points) {
            if (point != null) {
                snapshot.add(point);
            }
        }
        return Collections.unmodifiableList(snapshot);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DeviceContext that)) return false;
        return Objects.equals(deviceId, that.deviceId);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(deviceId);
    }
}
