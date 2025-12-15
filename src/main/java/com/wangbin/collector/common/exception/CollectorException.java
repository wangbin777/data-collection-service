package com.wangbin.collector.common.exception;


import com.wangbin.collector.common.domain.enums.DataQuality;
import lombok.Getter;

/**
 * 采集器异常
 */

@Getter
public class CollectorException extends BusinessException {

    private final String deviceId;
    private final String pointId;
    private final DataQuality dataQuality;

    public CollectorException(String message, String deviceId, String pointId) {
        super(500, message);
        this.deviceId = deviceId;
        this.pointId = pointId;
        this.dataQuality = DataQuality.BAD;
    }

    public CollectorException(String message, String deviceId, String pointId, DataQuality dataQuality) {
        super(500, message);
        this.deviceId = deviceId;
        this.pointId = pointId;
        this.dataQuality = dataQuality;
    }

    public CollectorException(String message, String deviceId, String pointId, Throwable cause) {
        super(500, message);
        initCause(cause);
        this.deviceId = deviceId;
        this.pointId = pointId;
        this.dataQuality = DataQuality.BAD;
    }

    // 创建连接异常
    public static CollectorException connectionException(String message, String deviceId) {
        return new CollectorException(message, deviceId, null, DataQuality.DEVICE_ERROR);
    }

    // 创建采集异常
    public static CollectorException collectionException(String message, String deviceId, String pointId) {
        return new CollectorException(message, deviceId, pointId, DataQuality.DEVICE_ERROR);
    }

    // 创建超时异常
    public static CollectorException timeoutException(String deviceId, String pointId) {
        return new CollectorException("采集超时", deviceId, pointId, DataQuality.TIMEOUT);
    }

    // 创建配置异常
    public static CollectorException configException(String message, String deviceId, String pointId) {
        return new CollectorException(message, deviceId, pointId, DataQuality.CONFIG_ERROR);
    }
}