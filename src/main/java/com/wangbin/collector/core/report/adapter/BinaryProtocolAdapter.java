package com.wangbin.collector.core.report.adapter;

import com.wangbin.collector.core.report.model.message.IoTMessage;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/**
 * 二进制协议编码器
 */
@Slf4j
public class BinaryProtocolAdapter {

    /**
     * 编码为二进制数据
     */
    public byte[] encodeToBinary(IoTMessage message) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {

            // 版本（1字节）
            dos.writeByte(1);

            // 方法类型长度 + 方法
            writeString(dos, message.getMethod());

            // Message ID（如果有）
            if (message.getMessageId() != null) {
                writeString(dos, message.getMessageId());
            } else {
                dos.writeShort(0); // 长度为0
            }

            // 认证相关字段（认证消息才有）
            if (message.isAuthMessage()) {
                writeString(dos, message.getProductKey());
                writeString(dos, message.getDeviceName());
                writeString(dos, message.getClientId());
                writeString(dos, message.getUsername());
                writeString(dos, message.getPassword());
            } else {
                // 业务参数编码
                encodeParams(dos, message.getParams());
            }

            dos.flush();
            return baos.toByteArray();

        } catch (Exception e) {
            log.error("编码二进制消息失败", e);
            return null;
        }
    }

    private void writeString(DataOutputStream dos, String str) throws Exception {
        if (str == null) {
            dos.writeShort(0);
        } else {
            byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
            dos.writeShort(bytes.length);
            dos.write(bytes);
        }
    }

    private void encodeParams(DataOutputStream dos, Map<String, Object> params) throws Exception {
        if (params == null || params.isEmpty()) {
            dos.writeShort(0);
            return;
        }

        // 参数数量
        dos.writeShort(params.size());

        for (Map.Entry<String, Object> entry : params.entrySet()) {
            // 键
            writeString(dos, entry.getKey());

            // 值
            Object value = entry.getValue();
            if (value instanceof String) {
                dos.writeByte(1); // 字符串类型
                writeString(dos, (String) value);
            } else if (value instanceof Number) {
                if (value instanceof Integer) {
                    dos.writeByte(2); // 整数类型
                    dos.writeInt((Integer) value);
                } else if (value instanceof Long) {
                    dos.writeByte(3); // 长整数类型
                    dos.writeLong((Long) value);
                } else if (value instanceof Double) {
                    dos.writeByte(4); // 双精度类型
                    dos.writeDouble((Double) value);
                } else {
                    dos.writeByte(1);
                    writeString(dos, value.toString());
                }
            } else if (value instanceof Boolean) {
                dos.writeByte(5); // 布尔类型
                dos.writeBoolean((Boolean) value);
            } else {
                // 其他类型转为字符串
                dos.writeByte(1);
                writeString(dos, value.toString());
            }
        }
    }
}