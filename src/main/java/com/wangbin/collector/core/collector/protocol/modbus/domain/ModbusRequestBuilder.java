package com.wangbin.collector.core.collector.protocol.modbus.domain;

import com.digitalpetri.modbus.pdu.WriteMultipleRegistersRequest;
import com.wangbin.collector.core.collector.protocol.modbus.utils.ModbusUtils;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * Modbus请求构建器
 */
public class ModbusRequestBuilder {

    /**
     * 构建写多个寄存器请求
     */
    public static WriteMultipleRegistersRequest buildWriteMultipleRegisters(
            int startAddress, short[] values) {

        ByteBuffer buffer = ByteBuffer.allocate(values.length * 2);
        for (short value : values) {
            buffer.putShort(value);
        }
        buffer.flip();

        return new WriteMultipleRegistersRequest(
                startAddress,
                values.length,
                buffer.array()
        );
    }

    /**
     * 构建写多个寄存器请求（从整数列表）
     */
    public static WriteMultipleRegistersRequest buildWriteMultipleRegisters(
            int startAddress, List<Integer> values) {

        ByteBuffer buffer = ByteBuffer.allocate(values.size() * 2);
        for (Integer value : values) {
            buffer.putShort(value.shortValue());
        }
        buffer.flip();

        return new WriteMultipleRegistersRequest(
                startAddress,
                values.size(),
                buffer.array()
        );
    }
}