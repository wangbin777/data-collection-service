package com.wangbin.collector.core.report.adapter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wangbin.collector.core.report.model.message.EventMessage;
import com.wangbin.collector.core.report.model.message.IoTMessage;
import com.wangbin.collector.core.report.model.message.StateMessage;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * JSON协议编码器
 */
@Slf4j
public class JsonProtocolAdapter {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * 编码为JSON字符串
     */
    public String encodeToJson(IoTMessage message) {
        if (message == null) {
            return null;
        }
        try {
            Map<String, Object> jsonMap = new LinkedHashMap<>();
            jsonMap.put("id", message.getMessageId());
            jsonMap.put("version", message.getVersion());
            jsonMap.put("method", message.getMethod());
            putIfNotBlank(jsonMap, "productKey", message.getProductKey());
            putIfNotBlank(jsonMap, "deviceName", message.getDeviceName());
            long timestamp = message.getTimestamp() > 0 ? message.getTimestamp() : System.currentTimeMillis();
            jsonMap.put("timestamp", timestamp);

            Map<String, Object> params = buildParams(message);
            if (!params.isEmpty()) {
                jsonMap.put("params", params);
            }

            if (!message.getQualityMap().isEmpty()) {
                jsonMap.put("quality", message.getQualityMap());
            }
            if (!message.getPropertyTsMap().isEmpty()) {
                jsonMap.put("propertyTs", message.getPropertyTsMap());
            }
            if (!message.getMetadata().isEmpty()) {
                jsonMap.put("metadata", message.getMetadata());
            }

            return objectMapper.writeValueAsString(jsonMap);

        } catch (Exception e) {
            log.error("编码JSON消息失败", e);
            return null;
        }
    }

    private Map<String, Object> buildParams(IoTMessage message) {
        if (message.isAuthMessage()) {
            return buildAuthParams(message);
        }
        if (message.isStateMessage() && message instanceof StateMessage stateMessage) {
            Map<String, Object> params = new LinkedHashMap<>();
            if (stateMessage.getState() != null && !stateMessage.getState().isEmpty()) {
                params.putAll(stateMessage.getState());
            }
            if (message.getParams() != null && !message.getParams().isEmpty()) {
                params.putAll(message.getParams());
            }
            return params;
        }
        if (message.isEventMessage() && message instanceof EventMessage eventMessage) {
            Map<String, Object> params = new LinkedHashMap<>();
            putIfNotBlank(params, "eventCode", eventMessage.getEventCode());
            if (eventMessage.getEventData() != null && !eventMessage.getEventData().isEmpty()) {
                params.put("eventData", eventMessage.getEventData());
            }
            if (message.getParams() != null && !message.getParams().isEmpty()) {
                params.putAll(message.getParams());
            }
            return params;
        }

        Map<String, Object> params = new LinkedHashMap<>();
        if (message.getParams() != null && !message.getParams().isEmpty()) {
            params.putAll(message.getParams());
        }
        return params;
    }

    private Map<String, Object> buildAuthParams(IoTMessage message) {
        Map<String, Object> params = new LinkedHashMap<>();
        putIfNotBlank(params, "clientId", message.getClientId());
        putIfNotBlank(params, "username", message.getUsername());
        putIfNotBlank(params, "password", message.getPassword());
        putIfNotBlank(params, "productKey", message.getProductKey());
        putIfNotBlank(params, "deviceName", message.getDeviceName());
        return params;
    }

    private void putIfNotBlank(Map<String, Object> target, String key, String value) {
        if (value != null && !value.isEmpty()) {
            target.put(key, value);
        }
    }
}
