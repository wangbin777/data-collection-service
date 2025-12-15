package com.wangbin.collector.core.collector.protocol.modbus.domain;

import java.util.Objects;

public class ModbusAddress {
    private final RegisterType registerType;
    private final int address;

    public ModbusAddress(RegisterType registerType, int address) {
        this.registerType = registerType;
        this.address = address;
    }

    public RegisterType getRegisterType() {
        return registerType;
    }

    public int getAddress() {
        return address;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ModbusAddress that = (ModbusAddress) o;
        return address == that.address && registerType == that.registerType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(registerType, address);
    }

    @Override
    public String toString() {
        return registerType + ":" + address;
    }
}
