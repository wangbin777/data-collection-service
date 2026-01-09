package com.wangbin.collector.common.domain.dto.message;

import com.wangbin.collector.common.constant.MessageConstant;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Map;

/**
 * 属性上报消息
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class PropertyMessage extends BaseMessage {

    private String deviceId;
    private String deviceName;
    private String productKey;

    public PropertyMessage() {
        super();
    }

    public PropertyMessage(String deviceId, Map<String, Object> properties) {
        super(MessageConstant.MESSAGE_TYPE_PROPERTY_POST, properties);
        this.deviceId = deviceId;
    }

    // 添加上报属性
    public void addProperty(String key, Object value) {
        if (getParams() == null) {
            setParams(new java.util.HashMap<>());
        }
        getParams().put(key, value);
    }

    // 获取属性值
    public Object getProperty(String key) {
        return getParams() != null ? getParams().get(key) : null;
    }
}
