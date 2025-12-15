package com.wangbin.collector.core.collector.protocol.modbus.utils;

import com.wangbin.collector.common.enums.DataType;
import com.digitalpetri.modbus.Crc16;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

public class ModbusUtils {

    // =============== 线圈/离散输入解析 ===============

    /**
     * 将Modbus返回的线圈/离散输入字节数组解析为布尔列表
     */
    public static List<Boolean> getCoilValues(byte[] coilBytes, int quantity) {
        List<Boolean> values = new ArrayList<>(quantity);
        if (coilBytes == null || quantity <= 0) {
            return values;
        }

        // ✅ 修正：从i=0开始循环
        for (int i = 0; i < quantity; i++) {
            values.add(parseCoilValue(coilBytes, i));
        }

        return values;
    }

    /**
     * 解析单个线圈值（从字节数组中）
     */
    public static Boolean getCoilValue(byte[] coilBytes) {
        return getCoilValue(coilBytes, 1);
    }

    /**
     * 解析指定位置的线圈值
     */
    public static Boolean getCoilValue(byte[] coilBytes, int quantity) {
        if (quantity < 1) {
            return null;
        }
        int index = quantity - 1;
        if (coilBytes == null || index / 8 >= coilBytes.length) {
            return null;
        }

        int byteIndex = index / 8;
        int bitIndex = index % 8;

        return ((coilBytes[byteIndex] >> bitIndex) & 0x01) == 1;
    }

    /**
     * 解析单个线圈值（按位索引）
     */
    public static Boolean parseCoilValue(byte[] coilBytes, int bitIndex) {
        if (coilBytes == null || coilBytes.length == 0) {
            return null;
        }

        int byteIndex = bitIndex / 8;
        int bitOffset = bitIndex % 8;

        if (byteIndex >= coilBytes.length) {
            return null;
        }

        return ((coilBytes[byteIndex] >> bitOffset) & 0x01) == 1;
    }

    /**
     * 解析指定位
     */
    public static boolean parseBit(byte[] raw, int offset) {
        int byteIndex = offset / 8;
        int bitIndex = offset % 8;

        if (byteIndex >= raw.length) {
            return false;
        }
        return ((raw[byteIndex] >> bitIndex) & 0x01) == 1;
    }

    // =============== 寄存器值解析 ===============

    /**
     * 根据数据类型解析字节数组（增强版）
     */
    public static Object parseRegisterValue(byte[] raw, String dataType) {
        // 与convertByteToValue功能相同，只是方法名更清晰
        return convertByteToValue(raw, dataType);
    }

    /**
     * 根据数据类型解析字节数组
     */
    public static Object convertByteToValue(byte[] raw, String dataType) {
        if (raw == null || raw.length == 0) {
            return null;
        }

        if (dataType == null) {
            dataType = "UINT16";
        }

        try {
            DataType typeEnum = DataType.valueOf(dataType.toUpperCase());

            if (raw.length < typeEnum.getMinBytes()) {
                throw new IllegalArgumentException(
                        String.format("数据不足: %s需要%d字节, 实际%d字节",
                                dataType, typeEnum.getMinBytes(), raw.length));
            }

            return switch (typeEnum) {
                case INT16 -> parseInt16(raw);
                case UINT16 -> parseUInt16(raw);
                case INT32 -> parseInt32(raw);
                case UINT32 -> parseUInt32(raw);
                case FLOAT32 -> parseFloat32(raw);
                case FLOAT32_SWAP -> parseFloat32WordSwap(raw);
                case FLOAT32_LITTLE -> parseFloat32LittleEndian(raw);
                case FLOAT64 -> parseFloat64(raw);
                case INT64 -> parseLong(raw);
                case UINT64 -> parseUnsignedLong(raw);
                case BOOLEAN -> parseBoolean(raw);
                case STRING -> parseString(raw);
            };
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("不支持的数据类型: " + dataType, e);
        }
    }

    /**
     * 根据偏移量解析寄存器值
     */
    public static Object parseValue(byte[] raw, int offsetRegister, String dataType) {
        if (raw == null || raw.length == 0) {
            return null;
        }

        switch (dataType.toLowerCase()) {
            case "int16":
            case "short":
                return parseInt16(raw, offsetRegister);
            case "uint16":
                return parseUInt16(raw, offsetRegister);
            case "int32":
                return parseInt32(raw, offsetRegister, false);
            case "int32_swap":
                return parseInt32(raw, offsetRegister, true);
            case "uint32":
                return parseUInt32(raw, offsetRegister, false);
            case "uint32_swap":
                return parseUInt32(raw, offsetRegister, true);
            case "float":
            case "float32":
                return parseFloat32(raw, offsetRegister, false);
            case "float_swap":
            case "float32_swap":
                return parseFloat32(raw, offsetRegister, true);
            case "double":
            case "float64":
                return parseFloat64(raw, offsetRegister, false);
            case "double_swap":
            case "float64_swap":
                return parseFloat64(raw, offsetRegister, true);
            default:
                return null;
        }
    }

    // =============== 基本类型解析 ===============

    public static Integer parseInt16(byte[] bytes) {
        if (bytes == null || bytes.length < 2) return null;
        return ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getShort() + 0;
    }

    public static Integer parseUInt16(byte[] bytes) {
        if (bytes == null || bytes.length < 2) return null;
        return ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getShort() & 0xFFFF;
    }

    public static Integer parseInt32(byte[] bytes) {
        if (bytes == null || bytes.length < 4) return null;
        return ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getInt();
    }

    public static Long parseUInt32(byte[] bytes) {
        if (bytes == null || bytes.length < 4) return null;
        long value = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getInt();
        return value & 0xFFFFFFFFL;
    }

    public static Float parseFloat32(byte[] bytes) {
        if (bytes == null || bytes.length < 4) return null;
        return ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getFloat();
    }

    public static Double parseFloat64(byte[] bytes) {
        if (bytes == null || bytes.length < 8) return null;
        return ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getDouble();
    }

    public static Long parseInt64(byte[] bytes) {
        if (bytes == null || bytes.length < 8) return null;
        return ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getLong();
    }

    // =============== 特殊格式解析 ===============

    public static Float parseFloat32LittleEndian(byte[] bytes) {
        if (bytes == null || bytes.length < 4) return null;
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getFloat();
    }

    public static Float parseFloat32WordSwap(byte[] bytes) {
        if (bytes == null || bytes.length < 4) return null;
        byte[] swapped = new byte[]{ bytes[2], bytes[3], bytes[0], bytes[1] };
        return parseFloat32(swapped);
    }

    public static Float parseFloat32ByteWordSwap(byte[] bytes) {
        if (bytes == null || bytes.length < 4) return null;
        byte[] swapped = new byte[]{ bytes[3], bytes[2], bytes[1], bytes[0] };
        return parseFloat32(swapped);
    }

    // =============== 带偏移量的解析 ===============

    private static byte[] slice(byte[] raw, int offsetBytes, int length) {
        byte[] result = new byte[length];
        if (offsetBytes + length <= raw.length) {
            System.arraycopy(raw, offsetBytes, result, 0, length);
        }
        return result;
    }

    public static short parseInt16(byte[] raw, int offsetRegister) {
        int offsetBytes = offsetRegister * 2;
        byte[] b = slice(raw, offsetBytes, 2);
        return ByteBuffer.wrap(b).order(ByteOrder.BIG_ENDIAN).getShort();
    }

    public static int parseUInt16(byte[] raw, int offsetRegister) {
        return parseInt16(raw, offsetRegister) & 0xFFFF;
    }

    public static int parseInt32(byte[] raw, int offsetRegister, boolean swap) {
        int offsetBytes = offsetRegister * 2;
        byte[] b = slice(raw, offsetBytes, 4);

        if (swap) {
            byte t0 = b[0]; b[0] = b[2]; b[2] = t0;
            byte t1 = b[1]; b[1] = b[3]; b[3] = t1;
        }

        return ByteBuffer.wrap(b).order(ByteOrder.BIG_ENDIAN).getInt();
    }

    public static long parseUInt32(byte[] raw, int offsetRegister, boolean swap) {
        int index = offsetRegister * 2;
        if (index + 3 >= raw.length) {
            throw new IllegalArgumentException("数据长度不足以解析uint32");
        }

        int high, low;
        if (!swap) {
            high = ((raw[index] & 0xFF) << 8) | (raw[index + 1] & 0xFF);
            low  = ((raw[index + 2] & 0xFF) << 8) | (raw[index + 3] & 0xFF);
        } else {
            low  = ((raw[index] & 0xFF) << 8) | (raw[index + 1] & 0xFF);
            high = ((raw[index + 2] & 0xFF) << 8) | (raw[index + 3] & 0xFF);
        }

        return ((long) high << 16) | (long) low;
    }

    public static float parseFloat32(byte[] raw, int offsetRegister, boolean swap) {
        int offsetBytes = offsetRegister * 2;
        byte[] b = slice(raw, offsetBytes, 4);

        if (swap) {
            byte t0 = b[0]; b[0] = b[2]; b[2] = t0;
            byte t1 = b[1]; b[1] = b[3]; b[3] = t1;
        }

        return ByteBuffer.wrap(b).order(ByteOrder.BIG_ENDIAN).getFloat();
    }

    public static double parseFloat64(byte[] raw, int offsetRegister, boolean swap) {
        int offsetBytes = offsetRegister * 2;
        byte[] b = slice(raw, offsetBytes, 8);

        if (swap) {
            for (int i = 0; i < 4; i++) {
                byte t0 = b[i * 2];
                b[i * 2] = b[(3 - i) * 2];
                b[(3 - i) * 2] = t0;

                byte t1 = b[i * 2 + 1];
                b[i * 2 + 1] = b[(3 - i) * 2 + 1];
                b[(3 - i) * 2 + 1] = t1;
            }
        }

        return ByteBuffer.wrap(b).order(ByteOrder.BIG_ENDIAN).getDouble();
    }

    // =============== 值转换方法 ===============

    /**
     * 获取数据类型对应的寄存器数量
     */
    public static int getRegisterCount(String dataType) {
        if (dataType == null) return 1;

        String type = dataType.toUpperCase();
        switch (type) {
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

    /**
     * 将值转换为寄存器数组
     */
    public static short[] valueToRegisters(Object value, String dataType, ByteOrder byteOrder) {
        if (value == null) {
            return new short[0];
        }

        if (dataType == null) {
            return new short[]{((Number) value).shortValue()};
        }

        String type = dataType.toUpperCase();
        switch (type) {
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
                return stringToRegisters(value.toString(), getRegisterCount(dataType), byteOrder);
            default:
                return new short[]{((Number) value).shortValue()};
        }
    }

    /**
     * 构建写寄存器请求数据
     */
    public static byte[] buildWriteRegistersData(short[] values) {
        ByteBuffer buffer = ByteBuffer.allocate(values.length * 2);
        for (short value : values) {
            buffer.putShort(value);
        }
        buffer.flip();
        return buffer.array();
    }

    /**
     * 构建线圈字节数组
     */
    public static byte[] buildCoilBytes(List<Boolean> values) {
        if (values == null || values.isEmpty()) {
            return new byte[0];
        }

        int quantity = values.size();
        int byteCount = (quantity + 7) / 8;
        byte[] coilBytes = new byte[byteCount];

        for (int i = 0; i < quantity; i++) {
            if (values.get(i)) {
                int byteIndex = i / 8;
                int bitIndex = i % 8;
                coilBytes[byteIndex] |= (byte) (1 << bitIndex);
            }
        }

        return coilBytes;
    }

    // =============== RTU专用方法 ===============

    /**
     * 计算Modbus RTU CRC16校验码
     */
    public static byte[] calcModbusCrc(byte[] data, int length) {
        Crc16 crc16 = new Crc16();
        for (int i = 0; i < length; i++) {
            crc16.update(data[i] & 0xFF);
        }
        int crc = crc16.getValue();

        // Modbus RTU: Low byte first
        return new byte[] {
                (byte) (crc & 0xFF),
                (byte) ((crc >> 8) & 0xFF)
        };
    }

    /**
     * 构建RTU诊断请求帧
     */
    public static byte[] buildRtuDiagnosticRequest(int slaveId, int subFunction, int data) {
        byte[] requestData = new byte[8];
        requestData[0] = (byte) slaveId;
        requestData[1] = 0x08; // Diagnostics function code
        requestData[2] = (byte) ((subFunction >> 8) & 0xFF);
        requestData[3] = (byte) (subFunction & 0xFF);
        requestData[4] = (byte) ((data >> 8) & 0xFF);
        requestData[5] = (byte) (data & 0xFF);

        byte[] crc = calcModbusCrc(requestData, 6);
        requestData[6] = crc[0]; // CRC Lo
        requestData[7] = crc[1]; // CRC Hi

        return requestData;
    }

    /**
     * 构建RTU读取异常状态请求帧
     */
    public static byte[] buildRtuExceptionStatusRequest(int slaveId) {
        byte[] requestData = new byte[4];
        requestData[0] = (byte) slaveId;
        requestData[1] = 0x07; // Read Exception Status

        byte[] crc = calcModbusCrc(requestData, 2);
        requestData[2] = crc[0]; // CRC Lo
        requestData[3] = crc[1]; // CRC Hi

        return requestData;
    }

    // =============== 私有辅助方法 ===============

    private static short[] int32ToRegisters(int value, ByteOrder byteOrder) {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.order(byteOrder);
        buffer.putInt(value);
        buffer.flip();
        return new short[]{buffer.getShort(), buffer.getShort()};
    }

    private static short[] uint32ToRegisters(long value, ByteOrder byteOrder) {
        return int32ToRegisters((int) value, byteOrder);
    }

    private static short[] floatToRegisters(float value, ByteOrder byteOrder) {
        int intBits = Float.floatToIntBits(value);
        return int32ToRegisters(intBits, byteOrder);
    }

    private static short[] doubleToRegisters(double value, ByteOrder byteOrder) {
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

    private static short[] stringToRegisters(String str, int registerCount, ByteOrder byteOrder) {
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

    private static Long parseLong(byte[] bytes) {
        if (bytes == null || bytes.length < 8) {
            return null;
        }

        return ((long) bytes[0] << 56) |
                ((long) (bytes[1] & 0xFF) << 48) |
                ((long) (bytes[2] & 0xFF) << 40) |
                ((long) (bytes[3] & 0xFF) << 32) |
                ((long) (bytes[4] & 0xFF) << 24) |
                ((long) (bytes[5] & 0xFF) << 16) |
                ((long) (bytes[6] & 0xFF) << 8) |
                ((long) (bytes[7] & 0xFF));
    }

    private static BigInteger parseUnsignedLong(byte[] bytes) {
        if (bytes == null || bytes.length < 8) {
            return null;
        }

        byte[] paddedBytes = new byte[9];
        System.arraycopy(bytes, 0, paddedBytes, 1, 8);
        return new BigInteger(paddedBytes);
    }

    private static boolean parseBoolean(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return false;
        }

        if (bytes.length == 1) {
            return bytes[0] != 0;
        } else if (bytes.length == 2) {
            short value = (short) (((bytes[0] & 0xFF) << 8) | (bytes[1] & 0xFF));
            return value != 0;
        } else {
            for (byte b : bytes) {
                if (b != 0) {
                    return true;
                }
            }
            return false;
        }
    }

    private static String parseString(byte[] bytes) {
        return parseString(bytes, "UTF-8");
    }

    private static String parseString(byte[] bytes, String charsetName) {
        if (bytes == null || bytes.length == 0) {
            return "";
        }

        try {
            int length = bytes.length;
            while (length > 0 && bytes[length - 1] == 0) {
                length--;
            }

            if (length == 0) {
                return "";
            }

            return new String(bytes, 0, length, charsetName);
        } catch (java.io.UnsupportedEncodingException e) {
            return new String(bytes).replaceAll("\0+$", "");
        }
    }

    private static String parseStringWithLengthPrefix(byte[] bytes) {
        if (bytes == null || bytes.length < 1) {
            return "";
        }

        int length = bytes[0] & 0xFF;
        if (length == 0 || bytes.length < length + 1) {
            return "";
        }

        try {
            return new String(bytes, 1, length, "UTF-8");
        } catch (java.io.UnsupportedEncodingException e) {
            return new String(bytes, 1, length);
        }
    }

    private static String parseFixedLengthString(byte[] bytes) {
        return parseFixedLengthString(bytes, "UTF-8");
    }

    private static String parseFixedLengthString(byte[] bytes, String charsetName) {
        if (bytes == null) {
            return "";
        }

        try {
            String result = new String(bytes, charsetName);
            return result.trim().replaceAll("\0+$", "");
        } catch (java.io.UnsupportedEncodingException e) {
            String result = new String(bytes);
            return result.trim().replaceAll("\0+$", "");
        }
    }

    // =============== 字节序相关方法 ===============

    public enum Endian {
        BIG,       // ABCD
        LITTLE,    // DCBA
        SWAP,      // CDAB
        REVERSE    // BADC
    }

    public static short toInt16(byte[] raw) {
        return ByteBuffer.wrap(raw).order(ByteOrder.BIG_ENDIAN).getShort();
    }

    public static int toUInt16(byte[] raw) {
        return Short.toUnsignedInt(ByteBuffer.wrap(raw).order(ByteOrder.BIG_ENDIAN).getShort());
    }

    public static int toInt32(byte[] raw, Endian endian) {
        raw = reorder(raw, endian);
        return ByteBuffer.wrap(raw).order(ByteOrder.BIG_ENDIAN).getInt();
    }

    public static long toUInt32(byte[] raw, Endian endian) {
        raw = reorder(raw, endian);
        return Integer.toUnsignedLong(ByteBuffer.wrap(raw).order(ByteOrder.BIG_ENDIAN).getInt());
    }

    public static float toFloat(byte[] raw, Endian endian) {
        raw = reorder(raw, endian);
        return ByteBuffer.wrap(raw).order(ByteOrder.BIG_ENDIAN).getFloat();
    }

    public static double toDouble(byte[] raw, Endian endian) {
        raw = reorder(raw, endian);
        return ByteBuffer.wrap(raw).order(ByteOrder.BIG_ENDIAN).getDouble();
    }

    public static String toStringValue(byte[] raw) {
        return new String(raw).trim();
    }

    public static Object parse(byte[] raw, String dataType, Endian endian) {
        switch (dataType.toUpperCase()) {
            case "INT16":
            case "SHORT":
                return toInt16(raw);
            case "UINT16":
                return toUInt16(raw);
            case "INT32":
                return toInt32(raw, endian);
            case "UINT32":
                return toUInt32(raw, endian);
            case "FLOAT":
                return toFloat(raw, endian);
            case "DOUBLE":
                return toDouble(raw, endian);
            case "STRING":
                return toStringValue(raw);
            default:
                throw new IllegalArgumentException("不支持的数据类型: " + dataType);
        }
    }

    private static byte[] reorder(byte[] raw, Endian endian) {
        byte[] r = Arrays.copyOf(raw, raw.length);
        return switch (endian) {
            case BIG -> r;
            case LITTLE -> {
                reverse(r);
                yield r;
            }
            case SWAP -> swapWords(r);
            case REVERSE -> reverseWords(r);
            default -> r;
        };
    }

    private static void reverse(byte[] arr) {
        for (int i = 0; i < arr.length / 2; i++) {
            byte tmp = arr[i];
            arr[i] = arr[arr.length - i - 1];
            arr[arr.length - i - 1] = tmp;
        }
    }

    private static byte[] swapWords(byte[] raw) {
        return new byte[]{ raw[2], raw[3], raw[0], raw[1] };
    }

    private static byte[] reverseWords(byte[] raw) {
        return new byte[]{ raw[1], raw[0], raw[3], raw[2] };
    }
}