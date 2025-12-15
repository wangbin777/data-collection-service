package com.wangbin.collector.common.domain.dto.message;

import lombok.Data;

import java.util.Map;

/**
 * 基础消息DTO
 */
@Data
public class BaseMessage {

    private String version = "1.0";
    private String method;
    private Long timestamp;
    private Map<String, Object> params;

    public BaseMessage() {
        this.timestamp = System.currentTimeMillis();
    }

    public BaseMessage(String method, Map<String, Object> params) {
        this();
        this.method = method;
        this.params = params;
    }

    // 转换为Map
    public Map<String, Object> toMap() {
        Map<String, Object> map = new java.util.HashMap<>();
        map.put("version", version);
        map.put("method", method);
        map.put("timestamp", timestamp);
        map.put("params", params);
        return map;
    }
}
