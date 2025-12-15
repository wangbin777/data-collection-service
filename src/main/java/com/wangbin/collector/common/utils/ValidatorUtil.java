package com.wangbin.collector.common.utils;

import lombok.extern.slf4j.Slf4j;

import java.util.regex.Pattern;

/**
 * 验证工具类
 */
@Slf4j
public class ValidatorUtil {

    // 正则表达式
    private static final Pattern IP_PATTERN = Pattern.compile(
            "^((25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\\.){3}(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$");

    private static final Pattern PORT_PATTERN = Pattern.compile("^([1-9][0-9]{0,3}|[1-5][0-9]{4}|6[0-4][0-9]{3}|65[0-4][0-9]{2}|655[0-2][0-9]|6553[0-5])$");

    private static final Pattern EMAIL_PATTERN = Pattern.compile(
            "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$");

    private static final Pattern PHONE_PATTERN = Pattern.compile(
            "^1[3-9]\\d{9}$");

    private static final Pattern ID_CARD_PATTERN = Pattern.compile(
            "^[1-9]\\d{5}(18|19|20)\\d{2}((0[1-9])|(1[0-2]))(([0-2][1-9])|10|20|30|31)\\d{3}[0-9Xx]$");

    private static final Pattern URL_PATTERN = Pattern.compile(
            "^(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]");

    private static final Pattern NUMBER_PATTERN = Pattern.compile("^-?\\d+(\\.\\d+)?$");

    private static final Pattern INTEGER_PATTERN = Pattern.compile("^-?\\d+$");

    private static final Pattern POSITIVE_INTEGER_PATTERN = Pattern.compile("^[1-9]\\d*$");

    private static final Pattern NEGATIVE_INTEGER_PATTERN = Pattern.compile("^-[1-9]\\d*$");

    private static final Pattern FLOAT_PATTERN = Pattern.compile("^-?\\d+\\.\\d+$");

    private static final Pattern DATE_PATTERN = Pattern.compile(
            "^\\d{4}-\\d{2}-\\d{2}$");

    private static final Pattern DATETIME_PATTERN = Pattern.compile(
            "^\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}$");

    private static final Pattern HEX_PATTERN = Pattern.compile("^[0-9a-fA-F]+$");

    private static final Pattern MAC_PATTERN = Pattern.compile(
            "^([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})$");

    private ValidatorUtil() {
        // 工具类，防止实例化
    }

    /**
     * 验证IP地址
     */
    public static boolean isValidIp(String ip) {
        if (ip == null || ip.trim().isEmpty()) {
            return false;
        }
        return IP_PATTERN.matcher(ip).matches();
    }

    /**
     * 验证端口号
     */
    public static boolean isValidPort(int port) {
        return port > 0 && port <= 65535;
    }

    public static boolean isValidPort(String port) {
        if (port == null || port.trim().isEmpty()) {
            return false;
        }
        if (!PORT_PATTERN.matcher(port).matches()) {
            return false;
        }
        int portNum = Integer.parseInt(port);
        return isValidPort(portNum);
    }

    /**
     * 验证邮箱
     */
    public static boolean isValidEmail(String email) {
        if (email == null || email.trim().isEmpty()) {
            return false;
        }
        return EMAIL_PATTERN.matcher(email).matches();
    }

    /**
     * 验证手机号
     */
    public static boolean isValidPhone(String phone) {
        if (phone == null || phone.trim().isEmpty()) {
            return false;
        }
        return PHONE_PATTERN.matcher(phone).matches();
    }

    /**
     * 验证身份证号
     */
    public static boolean isValidIdCard(String idCard) {
        if (idCard == null || idCard.trim().isEmpty()) {
            return false;
        }
        return ID_CARD_PATTERN.matcher(idCard).matches();
    }

    /**
     * 验证URL
     */
    public static boolean isValidUrl(String url) {
        if (url == null || url.trim().isEmpty()) {
            return false;
        }
        return URL_PATTERN.matcher(url).matches();
    }

    /**
     * 验证是否为数字
     */
    public static boolean isNumber(String number) {
        if (number == null || number.trim().isEmpty()) {
            return false;
        }
        return NUMBER_PATTERN.matcher(number).matches();
    }

    /**
     * 验证是否为整数
     */
    public static boolean isInteger(String number) {
        if (number == null || number.trim().isEmpty()) {
            return false;
        }
        return INTEGER_PATTERN.matcher(number).matches();
    }

    /**
     * 验证是否为正整数
     */
    public static boolean isPositiveInteger(String number) {
        if (number == null || number.trim().isEmpty()) {
            return false;
        }
        return POSITIVE_INTEGER_PATTERN.matcher(number).matches();
    }

    /**
     * 验证是否为负整数
     */
    public static boolean isNegativeInteger(String number) {
        if (number == null || number.trim().isEmpty()) {
            return false;
        }
        return NEGATIVE_INTEGER_PATTERN.matcher(number).matches();
    }

    /**
     * 验证是否为浮点数
     */
    public static boolean isFloat(String number) {
        if (number == null || number.trim().isEmpty()) {
            return false;
        }
        return FLOAT_PATTERN.matcher(number).matches();
    }

    /**
     * 验证日期格式（yyyy-MM-dd）
     */
    public static boolean isDate(String date) {
        if (date == null || date.trim().isEmpty()) {
            return false;
        }
        return DATE_PATTERN.matcher(date).matches();
    }

    /**
     * 验证日期时间格式（yyyy-MM-dd HH:mm:ss）
     */
    public static boolean isDateTime(String dateTime) {
        if (dateTime == null || dateTime.trim().isEmpty()) {
            return false;
        }
        return DATETIME_PATTERN.matcher(dateTime).matches();
    }

    /**
     * 验证十六进制字符串
     */
    public static boolean isHex(String hex) {
        if (hex == null || hex.trim().isEmpty()) {
            return false;
        }
        return HEX_PATTERN.matcher(hex).matches();
    }

    /**
     * 验证MAC地址
     */
    public static boolean isMacAddress(String mac) {
        if (mac == null || mac.trim().isEmpty()) {
            return false;
        }
        return MAC_PATTERN.matcher(mac).matches();
    }

    /**
     * 验证字符串长度
     */
    public static boolean isValidLength(String str, int min, int max) {
        if (str == null) {
            return false;
        }
        int length = str.length();
        return length >= min && length <= max;
    }

    /**
     * 验证字符串是否为空
     */
    public static boolean isNotEmpty(String str) {
        return str != null && !str.trim().isEmpty();
    }

    /**
     * 验证字符串数组是否都不为空
     */
    public static boolean areNotEmpty(String... strs) {
        if (strs == null || strs.length == 0) {
            return false;
        }
        for (String str : strs) {
            if (!isNotEmpty(str)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 验证是否为布尔值
     */
    public static boolean isBoolean(String bool) {
        if (bool == null) {
            return false;
        }
        return "true".equalsIgnoreCase(bool) || "false".equalsIgnoreCase(bool) ||
                "1".equals(bool) || "0".equals(bool);
    }

    /**
     * 验证值是否在范围内
     */
    public static boolean isInRange(int value, int min, int max) {
        return value >= min && value <= max;
    }

    public static boolean isInRange(double value, double min, double max) {
        return value >= min && value <= max;
    }

    public static boolean isInRange(String value, double min, double max) {
        if (!isNumber(value)) {
            return false;
        }
        double num = Double.parseDouble(value);
        return isInRange(num, min, max);
    }

    /**
     * 验证设备ID格式
     */
    public static boolean isValidDeviceId(String deviceId) {
        if (!isNotEmpty(deviceId)) {
            return false;
        }
        // 设备ID规则：字母开头，允许字母、数字、下划线，长度3-50
        return deviceId.matches("^[a-zA-Z][a-zA-Z0-9_]{2,49}$");
    }

    /**
     * 验证产品Key格式
     */
    public static boolean isValidProductKey(String productKey) {
        if (!isNotEmpty(productKey)) {
            return false;
        }
        // 产品Key规则：字母数字组成，长度10-32
        return productKey.matches("^[a-zA-Z0-9]{10,32}$");
    }

    /**
     * 验证设备名称格式
     */
    public static boolean isValidDeviceName(String deviceName) {
        if (!isNotEmpty(deviceName)) {
            return false;
        }
        // 设备名称规则：字母开头，允许字母、数字、下划线、中划线，长度1-64
        return deviceName.matches("^[a-zA-Z][a-zA-Z0-9_-]{0,63}$");
    }

    /**
     * 验证数据点地址格式
     */
    public static boolean isValidDataPointAddress(String address) {
        if (!isNotEmpty(address)) {
            return false;
        }
        // 支持多种地址格式：寄存器地址、OID、变量名等
        // 基本规则：非空，长度不超过255
        return address.length() <= 255;
    }

    /**
     * 验证JSON字符串
     */
    public static boolean isValidJson(String json) {
        return JsonUtil.isValidJson(json);
    }

    /**
     * 验证是否为有效的时间戳（秒）
     */
    public static boolean isValidTimestamp(long timestamp) {
        // 假设时间戳在2000-01-01到2100-01-01之间
        long minTimestamp = 946684800L; // 2000-01-01 00:00:00
        long maxTimestamp = 4102444800L; // 2100-01-01 00:00:00
        return timestamp >= minTimestamp && timestamp <= maxTimestamp;
    }

    /**
     * 验证是否为有效的时间戳（毫秒）
     */
    public static boolean isValidTimestampMs(long timestampMs) {
        long minTimestampMs = 946684800000L; // 2000-01-01 00:00:00
        long maxTimestampMs = 4102444800000L; // 2100-01-01 00:00:00
        return timestampMs >= minTimestampMs && timestampMs <= maxTimestampMs;
    }

    /**
     * 验证协议类型
     */
    public static boolean isValidProtocolType(String protocolType) {
        if (!isNotEmpty(protocolType)) {
            return false;
        }
        // 检查是否为支持的协议类型
        String[] supportedProtocols = {
                "MODBUS_TCP", "MODBUS_RTU", "OPC_DA", "OPC_UA",
                "SNMP", "MQTT", "COAP", "IEC104", "IEC61850",
                "HTTP", "HTTPS", "WEBSOCKET", "CUSTOM_TCP"
        };

        for (String protocol : supportedProtocols) {
            if (protocol.equals(protocolType)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 验证采集模式
     */
    public static boolean isValidCollectionMode(String mode) {
        if (!isNotEmpty(mode)) {
            return false;
        }
        return "POLLING".equals(mode) || "SUBSCRIPTION".equals(mode) ||
                "EVENT".equals(mode) || "TRIGGER".equals(mode);
    }

    /**
     * 验证数据质量
     */
    public static boolean isValidDataQuality(int quality) {
        return quality >= 0 && quality <= 100;
    }

    /**
     * 验证连接状态
     */
    public static boolean isValidConnectionStatus(String status) {
        if (!isNotEmpty(status)) {
            return false;
        }
        return "DISCONNECTED".equals(status) || "CONNECTING".equals(status) ||
                "CONNECTED".equals(status) || "AUTHENTICATING".equals(status) ||
                "AUTHENTICATED".equals(status) || "ERROR".equals(status) ||
                "RECONNECTING".equals(status);
    }
}