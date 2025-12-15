package com.wangbin.collector.core.collector.protocol.modbus.utils;

import com.wangbin.collector.common.enums.DataType;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Modbus数据类型转换器
 */
public class ModbusConverter {

    /**
     * 将值转换为寄存器数组
     */
    public static short[] valueToRegisters(Object value, String dataType, int registerCount, ByteOrder byteOrder) {
        if (value == null) {
            return new short[registerCount];
        }

        if (dataType == null) {
            return new short[]{((Number) value).shortValue()};
        }

        switch (dataType.toUpperCase()) {
            case "INT16":
            case "UINT16":
                return new short[]{((Number) value).shortValue()};
            case "INT32":
                return int32ToRegisters(((Number) value).intValue(), byteOrder);
            case "UINT32":
                return uint32ToRegisters(((Number) value).longValue() & 0xFFFFFFFFL, byteOrder);
            case "FLOAT":
            case "FLOAT32":
                return floatToRegisters(((Number) value).floatValue(), byteOrder);
            case "DOUBLE":
            case "FLOAT64":
                return doubleToRegisters(((Number) value).doubleValue(), byteOrder);
            case "BOOLEAN":
                return new short[]{((Boolean) value) ? (short) 1 : (short) 0};
            case "STRING":
                return stringToRegisters(value.toString(), registerCount, byteOrder);
            default:
                return new short[]{((Number) value).shortValue()};
        }
    }

    /**
     * 转换32位整数为寄存器
     */
    public static short[] int32ToRegisters(int value, ByteOrder byteOrder) {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.order(byteOrder);
        buffer.putInt(value);
        buffer.flip();
        return new short[]{buffer.getShort(), buffer.getShort()};
    }

    /**
     * 转换无符号32位整数为寄存器
     */
    public static short[] uint32ToRegisters(long value, ByteOrder byteOrder) {
        return int32ToRegisters((int) value, byteOrder);
    }

    /**
     * 转换浮点数为寄存器
     */
    public static short[] floatToRegisters(float value, ByteOrder byteOrder) {
        int intBits = Float.floatToIntBits(value);
        return int32ToRegisters(intBits, byteOrder);
    }

    /**
     * 转换双精度浮点数为寄存器
     */
    public static short[] doubleToRegisters(double value, ByteOrder byteOrder) {
        long longBits = Double.doubleToLongBits(value);
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.order(byteOrder);
        buffer.putLong(longBits);
        buffer.flip();
        return new short[]{
                buffer.getShort(),
                buffer.getShort(),
                buffer.getShort(),
                buffer.getShort()
        };
    }

    /**
     * 转换字符串为寄存器
     */
    public static short[] stringToRegisters(String str, int registerCount, ByteOrder byteOrder) {
        short[] registers = new short[registerCount];
        int byteCount = registerCount * 2;
        byte[] bytes = str.getBytes();

        byte[] paddedBytes = new byte[byteCount];
        int length = Math.min(bytes.length, byteCount);
        System.arraycopy(bytes, 0, paddedBytes, 0, length);

        ByteBuffer buffer = ByteBuffer.wrap(paddedBytes);
        buffer.order(byteOrder);

        for (int i = 0; i < registerCount; i++) {
            registers[i] = buffer.getShort();
        }

        return registers;
    }

    /**
     * 获取数据类型对应的寄存器数量
     */
    public static int getRegisterCount(String dataType) {
        if (dataType == null) return 1;

        switch (dataType.toUpperCase()) {
            case "INT16":
            case "UINT16":
            case "BOOLEAN":
                return 1;
            case "INT32":
            case "UINT32":
            case "FLOAT":
            case "FLOAT32":
                return 2;
            case "INT64":
            case "UINT64":
            case "DOUBLE":
            case "FLOAT64":
                return 4;
            default:
                return 1;
        }
    }
}