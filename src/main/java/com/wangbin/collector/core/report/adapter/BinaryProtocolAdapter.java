package com.wangbin.collector.core.report.adapter;

import com.wangbin.collector.core.report.model.message.IoTMessage;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Binary protocol encoder.
 */
@Slf4j
public class BinaryProtocolAdapter {

    private static final byte MAGIC_NUMBER = (byte) 0x7E;
    private static final byte PROTOCOL_VERSION = 0x01;
    private static final byte MESSAGE_TYPE_REQUEST = 0x01;
    private final JsonProtocolAdapter jsonAdapter = new JsonProtocolAdapter();

    /**
     * Encode report data into the TCP binary packet defined in the mock docs.
     */
    public byte[] encodeToBinary(IoTMessage message) {
        if (message == null) {
            return null;
        }
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {

            byte[] messageIdBytes = toBytes(message.getMessageId());
            byte[] methodBytes = toBytes(message.getMethod());
            byte[] bodyBytes = buildBodyBytes(message);

            int totalLength = 7
                    + Short.BYTES + messageIdBytes.length
                    + Short.BYTES + methodBytes.length
                    + bodyBytes.length;

            dos.writeByte(MAGIC_NUMBER);
            dos.writeByte(PROTOCOL_VERSION);
            dos.writeByte(MESSAGE_TYPE_REQUEST);
            dos.writeInt(totalLength);

            dos.writeShort(messageIdBytes.length);
            if (messageIdBytes.length > 0) {
                dos.write(messageIdBytes);
            }

            dos.writeShort(methodBytes.length);
            if (methodBytes.length > 0) {
                dos.write(methodBytes);
            }

            if (bodyBytes.length > 0) {
                dos.write(bodyBytes);
            }

            dos.flush();
            return baos.toByteArray();

        } catch (Exception e) {
            log.error("Failed to encode binary message", e);
            return null;
        }
    }

    private byte[] buildBodyBytes(IoTMessage message) {
        String jsonBody = jsonAdapter.encodeToJson(message);
        if (jsonBody == null) {
            return new byte[0];
        }
        return jsonBody.getBytes(StandardCharsets.UTF_8);
    }

    private byte[] toBytes(String value) {
        if (value == null || value.isEmpty()) {
            return new byte[0];
        }
        return value.getBytes(StandardCharsets.UTF_8);
    }
}