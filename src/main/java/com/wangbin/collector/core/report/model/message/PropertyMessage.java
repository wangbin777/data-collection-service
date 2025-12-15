package com.wangbin.collector.core.report.model.message;

import lombok.Data;

/**
 * 属性上报消息
 */
@Data
public class PropertyMessage extends IoTMessage {
    public PropertyMessage() {
        setMethod("thing.property.post");
        setTimestamp(System.currentTimeMillis());
    }

    // 设置属性值
    public void setProperty(String name, Object value) {
        addParam(name, value);
    }
}