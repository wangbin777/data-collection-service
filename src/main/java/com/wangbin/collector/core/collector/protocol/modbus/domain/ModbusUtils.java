package com.wangbin.collector.core.collector.protocol.modbus.domain;

import com.wangbin.collector.common.enums.DataType;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ModbusUtils {

    /**
     * 将 Modbus 返回的线圈/离散输入字节数组解析为布尔列表
     *
     * @param coilBytes 字节数组
     * @param quantity  需要解析的线圈数量
     * @return 布尔列表
     */
    public static List<Boolean> getCoilValues(byte[] coilBytes, int quantity) {
        List<Boolean> values = new ArrayList<>(quantity);
        if (coilBytes == null || quantity <= 0) {
            return values;
        }
        for (int i = 1; i <= quantity; i++) {
            values.add(getCoilValue(coilBytes,quantity));
        }

        return values;
    }

    /**
     * 将 Modbus 返回的线圈/离散输入字节数组解析为布尔值
     * @param coilBytes 字节数组
     * @return
     */
    public static Boolean getCoilValue(byte[] coilBytes) {
        return getCoilValue(coilBytes,1);
    }

    /**
     * 将 Modbus 返回的线圈/离散输入字节数组解析为布尔值
     * @param coilBytes 字节数组
     * @param quantity 需要解析的线圈数量
     * @return
     */
    public static Boolean getCoilValue(byte[] coilBytes,int quantity) {
        if(quantity <1){
            return null;
        }
        int index = quantity-1;
        if (coilBytes == null || index / 8 >= coilBytes.length) {
            return null;
        }

        int byteIndex = index / 8;
        int bitIndex = index % 8;

        return ((coilBytes[byteIndex] >> bitIndex) & 0x01) == 1;
    }


    // 解析某个 bit（offset 位）
    public static boolean parseBit(byte[] raw, int offset) {
        int byteIndex = offset / 8;
        int bitIndex = offset % 8;

        if (byteIndex >= raw.length) {
            return false; // 防止越界
        }
        return ((raw[byteIndex] >> bitIndex) & 0x01) == 1;
    }


    /**
     * 转换寄存器ByteBuf为值
     * @param raw
     * @param dataType
     * @return
     */
    public static Object convertByteToValue(byte[] raw, String dataType) {
        if (raw == null || raw.length == 0) {
            return null;
        }

        if (dataType == null) {
            dataType = "UINT16"; // 默认类型
        }

        // 获取数据类型对应的枚举（不区分大小写）
        DataType typeEnum;
        try {
            typeEnum = DataType.valueOf(dataType.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unsupported data type: " + dataType);
        }

        // 验证数据长度
        if (raw.length < typeEnum.getMinBytes()) {
            throw new IllegalArgumentException(
                    String.format("Insufficient data for %s. Need at least %d bytes, got %d",
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
    }







    /**
     * 从 byte[] 中解析 int16 (signed)
     */
    public static Integer parseInt16(byte[] bytes) {
        if (bytes == null || bytes.length < 2) return null;
        return ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getShort(0) + 0;
    }

    /**
     * uint16 (0 - 65535)
     */
    public static Integer parseUInt16(byte[] bytes) {
        if (bytes == null || bytes.length < 2) return null;
        return ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getShort(0) & 0xFFFF;
    }

    /**
     * int32
     */
    public static Integer parseInt32(byte[] bytes) {
        if (bytes == null || bytes.length < 4) return null;
        return ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getInt(0);
    }

    /**
     * uint32 (long)
     */
    public static Long parseUInt32(byte[] bytes) {
        if (bytes == null || bytes.length < 4) return null;
        long value = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getInt(0);
        return value & 0xFFFFFFFFL;
    }

    public static long parseUInt32(byte[] raw, int offsetRegister, boolean swap) {
        int index = offsetRegister * 2;
        if (index + 3 >= raw.length) {
            throw new IllegalArgumentException("raw 数据长度不足以解析 uint32");
        }

        int high, low;
        if (!swap) {
            high = ((raw[index] & 0xFF) << 8) | (raw[index + 1] & 0xFF);
            low  = ((raw[index + 2] & 0xFF) << 8) | (raw[index + 3] & 0xFF);
        } else {
            // 交换高低寄存器顺序
            low  = ((raw[index] & 0xFF) << 8) | (raw[index + 1] & 0xFF);
            high = ((raw[index + 2] & 0xFF) << 8) | (raw[index + 3] & 0xFF);
        }

        return ((long) high << 16) | (long) low;
    }


    /**
     * float32 (IEEE754)
     */
    public static Float parseFloat32(byte[] bytes) {
        if (bytes == null || bytes.length < 4) return null;
        return ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getFloat(0);
    }

    /**
     * int64
     */
    public static Long parseInt64(byte[] bytes) {
        if (bytes == null || bytes.length < 8) return null;
        return ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getLong(0);
    }

    /**
     * float64 (double)
     */
    public static Double parseFloat64(byte[] bytes) {
        if (bytes == null || bytes.length < 8) return null;
        return ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN).getDouble(0);
    }

    /**
     * 按照小端 byte order 解析 (常见于西门子/某些仪表)
     */
    public static Float parseFloat32LittleEndian(byte[] bytes) {
        if (bytes == null || bytes.length < 4) return null;
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getFloat(0);
    }

    /**
     * Word Swap (CD AB) 类型 float32
     */
    public static Float parseFloat32WordSwap(byte[] bytes) {
        if (bytes == null || bytes.length < 4) return null;
        byte[] swapped = new byte[]{
                bytes[2], bytes[3],
                bytes[0], bytes[1]
        };
        return parseFloat32(swapped);
    }

    /**
     * Byte Swap + Word Swap (DC BA HG FE)
     */
    public static Float parseFloat32ByteWordSwap(byte[] bytes) {
        if (bytes == null || bytes.length < 4) return null;
        byte[] swapped = new byte[]{
                bytes[3], bytes[2],
                bytes[1], bytes[0]
        };
        return parseFloat32(swapped);
    }

    /**
     * 解析有符号64位整数（大端字节序）
     */
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

    /**
     * 解析无符号64位整数（大端字节序）
     * 返回BigInteger处理无符号范围
     */
    private static BigInteger parseUnsignedLong(byte[] bytes) {
        if (bytes == null || bytes.length < 8) {
            return null;
        }

        // 使用BigInteger来处理无符号64位整数
        byte[] paddedBytes = new byte[9]; // 9个字节，第一个字节为0确保正数
        System.arraycopy(bytes, 0, paddedBytes, 1, 8);

        return new BigInteger(paddedBytes);
    }

    /**
     * 解析布尔值
     * 规则：0为false，非0为true
     */
    private static boolean parseBoolean(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return false;
        }

        // 多种布尔值解析策略
        if (bytes.length == 1) {
            // 单字节：0为false，非0为true
            return bytes[0] != 0;
        } else if (bytes.length == 2) {
            // 双字节（常见于Modbus）：0为false，非0为true
            short value = (short) (((bytes[0] & 0xFF) << 8) | (bytes[1] & 0xFF));
            return value != 0;
        } else {
            // 多个字节：任意字节非0则为true
            for (byte b : bytes) {
                if (b != 0) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * 解析字符串
     * 支持ASCII和UTF-8编码
     */
    private static String parseString(byte[] bytes) {
        return parseString(bytes, "UTF-8");
    }

    /**
     * 解析字符串（指定编码）
     */
    private static String parseString(byte[] bytes, String charsetName) {
        if (bytes == null || bytes.length == 0) {
            return "";
        }

        try {
            // 移除尾部的空字符（0x00）
            int length = bytes.length;
            while (length > 0 && bytes[length - 1] == 0) {
                length--;
            }

            if (length == 0) {
                return "";
            }

            return new String(bytes, 0, length, charsetName);
        } catch (java.io.UnsupportedEncodingException e) {
            // 回退到平台默认编码
            return new String(bytes).replaceAll("\0+$", ""); // 移除尾部空字符
        }
    }

    /**
     * 解析带长度前缀的字符串
     * 第一个字节为字符串长度
     */
    private static String parseStringWithLengthPrefix(byte[] bytes) {
        if (bytes == null || bytes.length < 1) {
            return "";
        }

        int length = bytes[0] & 0xFF; // 第一个字节为长度（无符号）
        if (length == 0 || bytes.length < length + 1) {
            return "";
        }

        try {
            return new String(bytes, 1, length, "UTF-8");
        } catch (java.io.UnsupportedEncodingException e) {
            return new String(bytes, 1, length);
        }
    }

    /**
     * 解析固定长度的字符串（用空格或空字符填充）
     */
    private static String parseFixedLengthString(byte[] bytes) {
        return parseFixedLengthString(bytes, "UTF-8");
    }

    private static String parseFixedLengthString(byte[] bytes, String charsetName) {
        if (bytes == null) {
            return "";
        }

        try {
            String result = new String(bytes, charsetName);
            // 去除尾部空格和空字符
            return result.trim().replaceAll("\0+$", "");
        } catch (java.io.UnsupportedEncodingException e) {
            String result = new String(bytes);
            return result.trim().replaceAll("\0+$", "");
        }
    }














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

    // 通用: 根据类型解析
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
                throw new IllegalArgumentException("Unsupported dataType: " + dataType);
        }
    }

    // 字节序调整
    private static byte[] reorder(byte[] raw, Endian endian) {
        byte[] r = Arrays.copyOf(raw, raw.length);
        return switch (endian) {
            case BIG -> // ABCD
                    r;
            case LITTLE -> {
                reverse(r);
                yield r; // DCBA
            }
            case SWAP -> // CDAB
                    swapWords(r);
            case REVERSE -> // BADC
                    reverseWords(r);
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
        // ABCD -> CDAB (word swap)
        return new byte[]{ raw[2], raw[3], raw[0], raw[1] };
    }

    private static byte[] reverseWords(byte[] raw) {
        // ABCD -> BADC
        return new byte[]{ raw[1], raw[0], raw[3], raw[2] };
    }
















    // ---------- 工具：从 byte[] 复制连续 n 字节 ----------
    private static byte[] slice(byte[] raw, int offsetBytes, int length) {
        byte[] result = new byte[length];
        if (offsetBytes + length <= raw.length) {
            System.arraycopy(raw, offsetBytes, result, 0, length);
        }
        return result;
    }

    // ---------- 解析 int16 ----------
    public static short parseInt16(byte[] raw, int offsetRegister) {
        int offsetBytes = offsetRegister * 2;
        byte[] b = slice(raw, offsetBytes, 2);
        return ByteBuffer.wrap(b).order(ByteOrder.BIG_ENDIAN).getShort();
    }

    // ---------- 解析 uint16 ----------
    public static int parseUInt16(byte[] raw, int offsetRegister) {
        return parseInt16(raw, offsetRegister) & 0xFFFF;
    }

    // ---------- 解析 int32 ----------
    public static int parseInt32(byte[] raw, int offsetRegister, boolean swap) {
        int offsetBytes = offsetRegister * 2;
        byte[] b = slice(raw, offsetBytes, 4);

        if (swap) {
            // word swap
            byte t0 = b[0]; b[0] = b[2]; b[2] = t0;
            byte t1 = b[1]; b[1] = b[3]; b[3] = t1;
        }

        return ByteBuffer.wrap(b).order(ByteOrder.BIG_ENDIAN).getInt();
    }

    // ---------- 解析 float ----------
    public static float parseFloat32(byte[] raw, int offsetRegister, boolean swap) {
        int offsetBytes = offsetRegister * 2;
        byte[] b = slice(raw, offsetBytes, 4);

        if (swap) {
            byte t0 = b[0]; b[0] = b[2]; b[2] = t0;
            byte t1 = b[1]; b[1] = b[3]; b[3] = t1;
        }

        return ByteBuffer.wrap(b).order(ByteOrder.BIG_ENDIAN).getFloat();
    }

    // ---------- 解析 double ----------
    public static double parseFloat64(byte[] raw, int offsetRegister, boolean swap) {
        int offsetBytes = offsetRegister * 2;
        byte[] b = slice(raw, offsetBytes, 8);

        if (swap) {
            // word swap (4-word swap)
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

    // ---------- 根据 dataType 自动解析 ----------
    public static Object parseValue(byte[] raw, int offsetRegister, String dataType) {
        return switch (dataType.toLowerCase()) {
            case "int16", "short" -> parseInt16(raw, offsetRegister);
            case "uint16" -> parseUInt16(raw, offsetRegister);
            case "int32" -> parseInt32(raw, offsetRegister, false);
            case "int32_swap" -> parseInt32(raw, offsetRegister, true);
            case "uint32" -> parseUInt32(raw, offsetRegister, false);         // 新增
            case "uint32_swap" -> parseUInt32(raw, offsetRegister, true);    // 可选，字节顺序交换
            case "float", "float32" -> parseFloat32(raw, offsetRegister, false);
            case "float_swap", "float32_swap" -> parseFloat32(raw, offsetRegister, true);
            case "double", "float64" -> parseFloat64(raw, offsetRegister, false);
            case "double_swap", "float64_swap" -> parseFloat64(raw, offsetRegister, true);
            default -> null;
        };
    }

}
