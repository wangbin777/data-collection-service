package com.wangbin.collector.core.collector.protocol.iec.util;

import org.openmuc.j60870.ie.*;
import com.wangbin.collector.core.collector.protocol.iec.domain.Iec104Address;
import com.wangbin.collector.core.collector.protocol.iec.domain.Iec104Command;

/**
 * IEC 104 协议工具类
 */
public class Iec104Utils {

    /**
     * 解析IEC 104地址
     */
    public static Iec104Address parseAddress(String addressStr) {
        if (addressStr == null || addressStr.isEmpty()) {
            throw new IllegalArgumentException("IEC 104地址不能为空");
        }

        String[] parts = addressStr.split(":");
        if (parts.length != 2) {
            throw new IllegalArgumentException("IEC 104地址格式错误，应为'类型ID:IO地址': " + addressStr);
        }

        try {
            int typeId = Integer.parseInt(parts[0].trim());
            int ioAddress = Integer.parseInt(parts[1].trim());
            return new Iec104Address(typeId, ioAddress);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("IEC 104地址格式错误，请输入有效的数字: " + addressStr, e);
        }
    }

    /**
     * 解析双点命令状态
     */
    public static IeDoubleCommand.DoubleCommandState parseDoubleCommandState(Object value) {
        if (value instanceof IeDoubleCommand.DoubleCommandState) {
            return (IeDoubleCommand.DoubleCommandState) value;
        }

        String strValue = value.toString().toUpperCase();
        switch (strValue) {
            case "OFF": case "1": case "FALSE":
                return IeDoubleCommand.DoubleCommandState.OFF;
            case "ON": case "2": case "TRUE":
                return IeDoubleCommand.DoubleCommandState.ON;
            case "NOT_PERMITTED_A": case "0":
                return IeDoubleCommand.DoubleCommandState.NOT_PERMITTED_A;
            case "NOT_PERMITTED_B": case "3":
                return IeDoubleCommand.DoubleCommandState.NOT_PERMITTED_B;
            default:
                try {
                    int intValue = Integer.parseInt(strValue);
                    IeDoubleCommand.DoubleCommandState state =
                            IeDoubleCommand.DoubleCommandState.getInstance(intValue);
                    if (state == null) {
                        throw new IllegalArgumentException("无效的双点命令值: " + strValue);
                    }
                    return state;
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("无效的双点命令值: " + strValue);
                }
        }
    }

    /**
     * 创建总召唤ASDU
     */
    public static Iec104Command createGeneralInterrogation(int commonAddress) {
        return new Iec104Command.Builder()
                .type(20)
                .commonAddress(commonAddress)
                .qualifier(new IeQualifierOfInterrogation(20))
                .build();
    }

    /**
     * 判断是否为读取类型
     */
    public static boolean isReadType(int typeId) {
        return typeId == 1 || typeId == 3 || typeId == 9 ||
                typeId == 11 || typeId == 13;
    }

    /**
     * 判断是否为写入类型
     */
    public static boolean isWriteType(int typeId) {
        return typeId == 45 || typeId == 46 ||
                (typeId >= 48 && typeId <= 50);
    }
}
