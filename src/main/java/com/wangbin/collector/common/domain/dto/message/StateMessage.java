package com.wangbin.collector.common.domain.dto.message;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Map;

/**
 * 状态上报消息
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class StateMessage extends BaseMessage {

    private String deviceId;
    private String deviceName;
    private String status; // ONLINE, OFFLINE, ERROR, etc.
    private String reason;
    private Map<String, Object> attributes;

    public StateMessage() {
        super();
    }

    public StateMessage(String deviceId, String status) {
        super("thing.state.update", null);
        this.deviceId = deviceId;
        this.status = status;
    }

    public StateMessage(String deviceId, String status, String reason) {
        this(deviceId, status);
        this.reason = reason;
    }
}
