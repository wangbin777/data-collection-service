package com.wangbin.collector.core.collector.protocol.snmp.util;

import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.core.collector.protocol.snmp.domain.SnmpAddress;
import com.wangbin.collector.core.collector.protocol.snmp.domain.SnmpDataType;

/**
 * 将 DataPoint 地址解析为 SnmpAddress。
 */
public final class SnmpAddressParser {

    private SnmpAddressParser() {
    }

    public static SnmpAddress parse(DataPoint point) {
        if (point == null) {
            throw new IllegalArgumentException("数据点不能为空");
        }
        String oid = point.getAddress();
        if (oid == null || oid.isBlank()) {
            Object configOid = point.getAdditionalConfig("oid");
            if (configOid != null) {
                oid = configOid.toString();
            }
        }
        if (oid == null || oid.isBlank()) {
            throw new IllegalArgumentException("SNMP 点位缺少 OID: " + point.getPointName());
        }
        SnmpDataType dataType = resolveDataType(point);
        return new SnmpAddress(oid.trim(), dataType);
    }

    private static SnmpDataType resolveDataType(DataPoint point) {
        if (point.getAdditionalConfig("snmpType") != null) {
            return SnmpDataType.fromText(point.getAdditionalConfig("snmpType").toString());
        }
        if (point.getAdditionalConfig("dataType") != null) {
            return SnmpDataType.fromText(point.getAdditionalConfig("dataType").toString());
        }
        return SnmpDataType.fromText(point.getDataType());
    }
}
