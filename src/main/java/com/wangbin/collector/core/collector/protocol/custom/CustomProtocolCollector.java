package com.wangbin.collector.core.collector.protocol.custom;

import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.core.collector.protocol.base.BaseCollector;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * 自定义TCP采集器的占位实现。
 * <p>
 * 该实现主要用于确保工厂创建时不会出现类型转换异常，
 * 实际采集逻辑需要根据协议要求在此类中补充。
 * </p>
 */
@Slf4j
public class CustomProtocolCollector extends BaseCollector {

    @Override
    public String getCollectorType() {
        return "CUSTOM_PTL";
    }

    @Override
    public String getProtocolType() {
        return "CUSTOM_PTL";
    }

    @Override
    protected void doConnect() {
        log.info("Custom TCP collector connected: {}", deviceInfo.getDeviceId());
    }

    @Override
    protected void doDisconnect() {
        log.info("Custom TCP collector disconnected: {}", deviceInfo.getDeviceId());
    }

    @Override
    protected Object doReadPoint(DataPoint point) {
        throw unsupported("readPoint");
    }

    @Override
    protected Map<String, Object> doReadPoints(List<DataPoint> points) {
        throw unsupported("readPoints");
    }

    @Override
    protected boolean doWritePoint(DataPoint point, Object value) {
        throw unsupported("writePoint");
    }

    @Override
    protected Map<String, Boolean> doWritePoints(Map<DataPoint, Object> points) {
        throw unsupported("writePoints");
    }

    @Override
    protected void doSubscribe(List<DataPoint> points) {
        throw unsupported("subscribe");
    }

    @Override
    protected void doUnsubscribe(List<DataPoint> points) {
        throw unsupported("unsubscribe");
    }

    @Override
    protected Map<String, Object> doGetDeviceStatus() {
        return Collections.emptyMap();
    }

    @Override
    protected Object doExecuteCommand(int unitId, String command, Map<String, Object> params) {
        throw unsupported("executeCommand");
    }

    @Override
    protected void buildReadPlans(String deviceId, List<DataPoint> points) {
        log.debug("Custom TCP collector buildReadPlans noop for device {}", deviceId);
    }

    private UnsupportedOperationException unsupported(String operation) {
        String message = String.format("CUSTOM_TCP collector does not implement %s yet", operation);
        log.warn(message);
        return new UnsupportedOperationException(message);
    }

}
