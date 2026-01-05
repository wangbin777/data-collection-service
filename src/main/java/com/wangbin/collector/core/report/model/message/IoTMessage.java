package com.wangbin.collector.core.report.model.message;

import com.wangbin.collector.common.constant.MessageConstant;
import lombok.Data;
import java.util.HashMap;
import java.util.Map;

/**
 * 物联网消息基类
 */
@Data
public class IoTMessage {
    private String messageId;
    private String version = MessageConstant.MESSAGE_VERSION_1_0;
    private String method; // 消息类型: thing.property.post 等
    private String deviceId;
    private String productKey;
    private String deviceName;
    private String clientId;
    private String username;
    private String password;
    private Map<String, Object> params = new HashMap<>();
    private Map<String, Object> metadata = new HashMap<>();
    private Map<String, String> qualityMap = new HashMap<>();
    private Map<String, Long> propertyTsMap = new HashMap<>();
    private long timestamp;

    // 是否为认证消息
    public boolean isAuthMessage() {
        return MessageConstant.MESSAGE_TYPE_AUTH.equals(method);
    }

    // 是否为属性上报消息
    public boolean isPropertyMessage() {
        return MessageConstant.MESSAGE_TYPE_PROPERTY_POST.equals(method);
    }

    // 是否为状态更新消息
    public boolean isStateMessage() {
        return MessageConstant.MESSAGE_TYPE_STATE_UPDATE.equals(method);
    }

    // 是否为事件消息
    public boolean isEventMessage() {
        return MessageConstant.MESSAGE_TYPE_EVENT_POST.equals(method);
    }

    // 添加参数
    public void addParam(String key, Object value) {
        if (params == null) {
            params = new HashMap<>();
        }
        params.put(key, value);
    }

    // 获取参数
    public Object getParam(String key) {
        return params != null ? params.get(key) : null;
    }

    public void addQuality(String field, String value) {
        if (qualityMap == null) {
            qualityMap = new HashMap<>();
        }
        qualityMap.put(field, value);
    }

    public void addPropertyTimestamp(String field, Long value) {
        if (propertyTsMap == null) {
            propertyTsMap = new HashMap<>();
        }
        propertyTsMap.put(field, value);
    }
}
