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

        String resolvedType = messageType == null
                ? MessageConstant.MESSAGE_TYPE_PROPERTY_POST
                : messageType;

        IoTMessage message = switch (resolvedType) {
            case MessageConstant.MESSAGE_TYPE_STATE_UPDATE -> buildStateUpdateMessage(data);
            case MessageConstant.MESSAGE_TYPE_EVENT_POST -> buildEventMessage(data);
            case MessageConstant.MESSAGE_TYPE_OTA_PROGRESS -> buildOtaProgressMessage(data);
            default -> buildPropertyMessage(data);
        };

        // 设置公共字段
        message.setProductKey(productKey);
        message.setDeviceName(deviceName);
        message.setClientId(productKey + "." + deviceName);
        message.setUsername(deviceName + "&" + productKey);
        message.setTimestamp(data.getTimestamp());
        if (message.getDeviceId() == null) {
            message.setDeviceId(data.getDeviceId());
        }

        // 设置消息ID
        if (message.getMessageId() == null) {
            message.setMessageId(generateMessageId());
        }

        // 从认证信息中获取密码（如果有）
        String password = (String) authInfo.get(MessageConstant.FIELD_PASSWORD);
        if (password != null) {
            message.setPassword(password);
        }

        // 添加数据质量信息
        if (data.getQuality() != null) {
            message.addParam("quality", data.getQuality());
        }

        // 添加元数据
        if (data.getMetadata() != null && !data.getMetadata().isEmpty()) {
            message.addParam("metadata", data.getMetadata());
        }

        return message;
    }

    private IoTMessage buildStateUpdateMessage(ReportData data) {
        StateMessage stateMessage = new StateMessage();
        Map<String, Object> state = new HashMap<>();
        state.put(MessageConstant.FIELD_VERSION, MessageConstant.MESSAGE_VERSION_1_0);
        state.put("status", data.getValue());
        state.put("timestamp", data.getTimestamp());
        stateMessage.setState(state);
        return stateMessage;
    }

    private IoTMessage buildEventMessage(ReportData data) {
        EventMessage eventMessage = new EventMessage();
        eventMessage.setEventCode(data.getPointCode());
        Map<String, Object> eventData = new HashMap<>();
        eventData.put("value", data.getValue());
        eventData.put("timestamp", data.getTimestamp());
        if (data.getMetadata() != null) {
            eventData.putAll(data.getMetadata());
        }
        eventMessage.setEventData(eventData);
        return eventMessage;
    }

    private IoTMessage buildOtaProgressMessage(ReportData data) {
        IoTMessage otaMessage = new IoTMessage();
        otaMessage.setMethod(MessageConstant.MESSAGE_TYPE_OTA_PROGRESS);
        Map<String, Object> otaParams = new HashMap<>();
        otaParams.put("progress", data.getValue());
        otaParams.put("stage", data.getPointCode());
        otaMessage.setParams(otaParams);
        return otaMessage;
    }

    private IoTMessage buildPropertyMessage(ReportData data) {
        PropertyMessage propertyMessage = new PropertyMessage();
        Map<String, Object> propertyParams = new HashMap<>();
        if (data.hasProperties()) {
            propertyParams.putAll(data.getProperties());
            if (!data.getPropertyQuality().isEmpty()) {
                propertyMessage.addParam("quality", data.getPropertyQuality());
            }
            if (!data.getPropertyTs().isEmpty()) {
                propertyMessage.addParam("propertyTs", data.getPropertyTs());
            }
        } else {
            propertyParams.put(data.getPointCode(), data.getValue());
        }
        propertyMessage.setParams(propertyParams);
        propertyMessage.setDeviceId(data.getDeviceId());
        propertyMessage.setTimestamp(data.getTimestamp());
        propertyMessage.setMetadata(new HashMap<>(data.getMetadata()));
        if (!data.getPropertyQuality().isEmpty()) {
            propertyMessage.getQualityMap().putAll(data.getPropertyQuality());
        }
        if (!data.getPropertyTs().isEmpty()) {
            propertyMessage.getPropertyTsMap().putAll(data.getPropertyTs());
        }
        String batchId = data.getBatchId();
        if (batchId != null) {
            propertyMessage.setMessageId(batchId);
        }
        return propertyMessage;
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
