package com.wangbin.collector.core.report.service;

import com.wangbin.collector.common.constant.MessageConstant;
import com.wangbin.collector.core.report.adapter.*;
import com.wangbin.collector.core.report.model.ReportData;
import com.wangbin.collector.core.report.model.message.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

/**
 * 物联网协议服务
 */
@Slf4j
@Service
public class IoTProtocolService {

    private final IoTProtocolAdapter protocolAdapter = new IoTProtocolAdapter();
    private final JsonProtocolAdapter jsonAdapter = new JsonProtocolAdapter();
    private final BinaryProtocolAdapter binaryAdapter = new BinaryProtocolAdapter();

    /**
     * 根据消息类型转换采集数据为物联网消息
     */
    public IoTMessage convertToIoTMessage(ReportData data, Map<String, Object> authInfo, String messageType) {
        if (data == null || authInfo == null) {
            return null;
        }

        String productKey = (String) authInfo.get(MessageConstant.FIELD_PRODUCT_KEY);
        String deviceName = (String) authInfo.get(MessageConstant.FIELD_DEVICE_NAME);

        if (productKey == null || deviceName == null) {
            log.warn("认证信息不完整，缺少productKey或deviceName");
            return null;
        }

        // 如果未指定消息类型，使用默认属性上报
        if (messageType == null) {
            messageType = MessageConstant.MESSAGE_TYPE_PROPERTY_POST;
        }

        // 根据消息类型创建不同的消息对象
        IoTMessage message;
        switch (messageType) {
            case MessageConstant.MESSAGE_TYPE_STATE_UPDATE:
                StateMessage stateMessage = new StateMessage();
                Map<String, Object> state = new HashMap<>();
                state.put(MessageConstant.FIELD_VERSION, MessageConstant.MESSAGE_VERSION_1_0);
                state.put("status", data.getValue());
                state.put("timestamp", data.getTimestamp());
                stateMessage.setState(state);
                message = stateMessage;
                break;

            case MessageConstant.MESSAGE_TYPE_EVENT_POST:
                EventMessage eventMessage = new EventMessage();
                eventMessage.setEventCode(data.getPointCode());
                Map<String, Object> eventData = new HashMap<>();
                eventData.put("value", data.getValue());
                eventData.put("timestamp", data.getTimestamp());
                if (data.getMetadata() != null) {
                    eventData.putAll(data.getMetadata());
                }
                eventMessage.setEventData(eventData);
                message = eventMessage;
                break;

            case MessageConstant.MESSAGE_TYPE_OTA_PROGRESS:
                // OTA升级进度消息
                IoTMessage otaMessage = new IoTMessage();
                otaMessage.setMethod(MessageConstant.MESSAGE_TYPE_OTA_PROGRESS);
                Map<String, Object> otaParams = new HashMap<>();
                otaParams.put("progress", data.getValue());
                otaParams.put("stage", data.getPointCode());
                otaMessage.setParams(otaParams);
                message = otaMessage;
                break;

            case MessageConstant.MESSAGE_TYPE_PROPERTY_POST:
            default:
                PropertyMessage propertyMessage = new PropertyMessage();
                Map<String, Object> propertyParams = new HashMap<>();
                propertyParams.put(data.getPointCode(), data.getValue());
                propertyMessage.setParams(propertyParams);
                message = propertyMessage;
                break;
        }

        // 设置公共字段
        message.setProductKey(productKey);
        message.setDeviceName(deviceName);
        message.setClientId(productKey + "." + deviceName);
        message.setUsername(deviceName + "&" + productKey);
        message.setTimestamp(data.getTimestamp());

        // 设置消息ID
        message.setMessageId(generateMessageId());

        // 从认证信息中获取密码（如果有）
        String password = (String) authInfo.get(MessageConstant.FIELD_PASSWORD);
        if (password != null) {
            message.setPassword(password);
        }

        // 添加数据质量信息
        if (data.getQuality() != null) {
            message.addParam("quality", data.getQuality().name());
        }

        // 添加元数据
        if (data.getMetadata() != null && !data.getMetadata().isEmpty()) {
            message.addParam("metadata", data.getMetadata());
        }

        return message;
    }

    /**
     * 转换采集数据为物联网消息（使用默认消息类型）
     */
    public IoTMessage convertToIoTMessage(ReportData data, Map<String, Object> authInfo) {
        return convertToIoTMessage(data, authInfo, MessageConstant.MESSAGE_TYPE_PROPERTY_POST);
    }

    /**
     * 编码为JSON格式
     */
    public String encodeToJson(IoTMessage message) {
        if (message == null) {
            return null;
        }
        return jsonAdapter.encodeToJson(message);
    }

    /**
     * 编码为二进制格式
     */
    public byte[] encodeToBinary(IoTMessage message) {
        if (message == null) {
            return null;
        }
        return binaryAdapter.encodeToBinary(message);
    }

    /**
     * 创建认证消息
     */
    public IoTMessage createAuthMessage(String productKey, String deviceName, String password) {
        IoTMessage authMessage = new IoTMessage();
        authMessage.setMethod(MessageConstant.MESSAGE_TYPE_AUTH);
        authMessage.setVersion(MessageConstant.MESSAGE_VERSION_1_0);
        authMessage.setProductKey(productKey);
        authMessage.setDeviceName(deviceName);
        authMessage.setClientId(productKey + "." + deviceName);
        authMessage.setUsername(deviceName + "&" + productKey);
        authMessage.setPassword(password);
        authMessage.setMessageId(generateMessageId());
        return authMessage;
    }

    /**
     * 批量转换
     */
    public Map<String, IoTMessage> batchConvert(Map<String, ReportData> dataMap,
                                                Map<String, Object> authInfo,
                                                String messageType) {
        Map<String, IoTMessage> result = new HashMap<>();

        for (Map.Entry<String, ReportData> entry : dataMap.entrySet()) {
            IoTMessage message = convertToIoTMessage(entry.getValue(), authInfo, messageType);
            if (message != null) {
                result.put(entry.getKey(), message);
            }
        }

        return result;
    }

    /**
     * 批量转换（默认消息类型）
     */
    public Map<String, IoTMessage> batchConvert(Map<String, ReportData> dataMap,
                                                Map<String, Object> authInfo) {
        return batchConvert(dataMap, authInfo, MessageConstant.MESSAGE_TYPE_PROPERTY_POST);
    }

    private String generateMessageId() {
        return System.currentTimeMillis() + "_" + (int)(Math.random() * 1000);
    }
}