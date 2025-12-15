package com.wangbin.collector.core.collector.protocol.modbus.utils;

import com.wangbin.collector.core.collector.protocol.modbus.domain.ModbusAddress;
import com.wangbin.collector.core.collector.protocol.modbus.domain.RegisterType;

/**
 * Modbus解析器
 */
public class ModbusParser {

    /**
     * 解析Modbus地址字符串
     */
    public static ModbusAddress parseModbusAddress(String addressStr) {
        if (addressStr == null || addressStr.isEmpty()) {
            throw new IllegalArgumentException("Modbus地址不能为空");
        }

        try {
            int address;
            RegisterType type;

            if (addressStr.contains("x") || addressStr.contains("X") || addressStr.contains(":")) {
                String[] parts = addressStr.split("[xX:]");
                if (parts.length != 2) {
                    throw new IllegalArgumentException("Modbus地址格式错误: " + addressStr);
                }

                int typeCode = Integer.parseInt(parts[0]);
                address = Integer.parseInt(parts[1]);
                type = RegisterType.fromCode(typeCode);
            } else {
                int fullAddress = Integer.parseInt(addressStr);
                int typeCode = fullAddress / 10000;
                address = fullAddress % 10000 - 1;
                type = RegisterType.fromCode(typeCode);
            }

            if (type == null) {
                throw new IllegalArgumentException("不支持的Modbus寄存器类型: " + addressStr);
            }

            return new ModbusAddress(type, address);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Modbus地址格式错误: " + addressStr, e);
        }
    }
}