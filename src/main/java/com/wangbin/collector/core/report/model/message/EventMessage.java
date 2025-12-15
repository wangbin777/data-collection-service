package com.wangbin.collector.core.report.model.message;

import lombok.Data;
import lombok.EqualsAndHashCode;
import java.util.Map;

/**
 * 设备事件消息
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class EventMessage extends IoTMessage {
    private String eventCode;
    private Map<String, Object> eventData;

    public EventMessage() {
        setMethod("thing.event.post");
    }
}