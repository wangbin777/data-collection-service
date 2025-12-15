package com.wangbin.collector.core.report.adapter;

import com.wangbin.collector.core.report.model.ReportData;
import com.wangbin.collector.core.report.model.message.IoTMessage;
import com.wangbin.collector.core.report.model.message.PropertyMessage;
import lombok.extern.slf4j.Slf4j;

/**
 * 物联网协议适配器
 */
@Slf4j
public class IoTProtocolAdapter {

    /**
     * 将采集数据转换为物联网属性消息
     */
    public IoTMessage convertToPropertyMessage(ReportData data, String productKey,
                                               String deviceName) {
        try {
            PropertyMessage message = new PropertyMessage();
            message.setProductKey(productKey);
            message.setDeviceName(deviceName);
            message.setClientId(productKey + "." + deviceName);
            message.setUsername(deviceName + "&" + productKey);

            // 设置属性值
            message.setProperty(data.getPointCode(), data.getValue());

            // 添加元数据
            if (data.getTimestamp() > 0) {
                message.addParam("timestamp", data.getTimestamp());
            }
            if (data.getMetadata() != null && !data.getMetadata().isEmpty()) {
                message.addParam("metadata", data.getMetadata());
            }

            return message;

        } catch (Exception e) {
            log.error("转换物联网消息失败: {}", data.getPointCode(), e);
            return null;
        }
    }

    /**
     * 转换为认证消息
     */
    public IoTMessage createAuthMessage(String productKey, String deviceName,
                                        String hashedPassword) {
        IoTMessage authMessage = new IoTMessage();
        authMessage.setMethod("auth");
        authMessage.setVersion("1.0");
        authMessage.setProductKey(productKey);
        authMessage.setDeviceName(deviceName);
        authMessage.setClientId(productKey + "." + deviceName);
        authMessage.setUsername(deviceName + "&" + productKey);
        authMessage.setPassword(hashedPassword);
        return authMessage;
    }
}