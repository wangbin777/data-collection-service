package com.wangbin.collector.core.report.adapter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.wangbin.collector.core.report.model.message.IoTMessage;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
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
        try {
            // 构建JSON结构
            Map<String, Object> jsonMap = new java.util.HashMap<>();

            if (message.isAuthMessage()) {
                // 认证消息格式
                jsonMap.put("version", message.getVersion());
                jsonMap.put("method", message.getMethod());
                Map<String, Object> params = new java.util.HashMap<>();
                params.put("productKey", message.getProductKey());
                params.put("deviceName", message.getDeviceName());
                params.put("clientId", message.getClientId());
                params.put("username", message.getUsername());
                params.put("password", message.getPassword());
                jsonMap.put("params", params);
            } else {
                // 业务消息格式
                jsonMap.put("version", message.getVersion());
                jsonMap.put("method", message.getMethod());
                jsonMap.put("params", message.getParams());
            }

            return objectMapper.writeValueAsString(jsonMap);

        } catch (Exception e) {
            log.error("编码JSON消息失败", e);
            return null;
        }
    }
}