package com.wangbin.collector.core.collector.protocol.modbus.base;

import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.core.collector.protocol.base.BaseCollector;
import com.wangbin.collector.core.collector.protocol.modbus.domain.ModbusAddress;
import com.wangbin.collector.core.collector.protocol.modbus.domain.GroupedPoint;
import com.wangbin.collector.core.collector.protocol.modbus.domain.RegisterType;
import com.wangbin.collector.core.collector.protocol.modbus.plan.ModbusReadPlan;
import com.wangbin.collector.core.collector.protocol.modbus.plan.ModbusReadPlanBuilder;
import com.wangbin.collector.core.collector.protocol.modbus.utils.ModbusGroupingUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Modbus采集器抽象基类
 */
@Slf4j
public abstract class AbstractModbusCollector extends BaseCollector {

    protected int timeout = 3000;
    /*protected int slaveId = 1;*/
    protected volatile List<ModbusReadPlan> readPlans = List.of();

    // 订阅缓存
    protected final Map<RegisterType, Map<Integer, DataPoint>> registerCache = new ConcurrentHashMap<>();

    // =============== 公共方法 ===============

    /**
     * 构建执行计划
     * @param points
     */
    @Override
    protected void buildReadPlans(String deviceId, List<DataPoint> points) {
        this.readPlans = ModbusReadPlanBuilder.build(deviceId,
                points,
                this::resolveUnitId,
                this::parseModbusAddress
        );
        log.info("Modbus ReadPlan 构建完成，计划数: {}", readPlans.size());
    }

    /**
     * 解析Modbus地址字符串
     */
    public ModbusAddress parseModbusAddress(String addressStr) {
        if (addressStr == null || addressStr.isEmpty()) {
            throw new IllegalArgumentException("Modbus地址不能为空");
        }

        try {
            int address;
            RegisterType type;
            int typeCode;

            // 处理分隔符格式: "3x40001", "3X40001", "3:40001"
            if (addressStr.contains("x") || addressStr.contains("X") || addressStr.contains(":")) {
                String[] parts = addressStr.split("[xX:]");
                if (parts.length != 2) {
                    throw new IllegalArgumentException("Modbus地址格式错误，应为'类型x地址'或'类型:地址': " + addressStr);
                }

                typeCode = Integer.parseInt(parts[0].trim());
                address = Integer.parseInt(parts[1].trim());
            } else {
                // 处理传统格式: "440001" (4表示类型，40001表示地址)
                int fullAddress = Integer.parseInt(addressStr.trim());
                typeCode = fullAddress / 10000;
                address = fullAddress % 10000 - 1;  // 转换为0-based地址
            }

            // 获取寄存器类型
            type = RegisterType.fromCode(typeCode);
            if (type == null) {
                throw new IllegalArgumentException("不支持的Modbus寄存器类型代码: " + typeCode +
                        " (地址: " + addressStr + ")");
            }

            // 验证地址有效性
            if (address < 0) {
                throw new IllegalArgumentException("Modbus地址不能小于0: " + addressStr);
            }
            if (address > 65535) {
                throw new IllegalArgumentException("Modbus地址不能超过65535: " + addressStr);
            }

            return new ModbusAddress(type, address);

        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Modbus地址格式错误，请输入有效的数字: " + addressStr, e);
        }
    }

    /**
     * 获取UnitId
     * @param point
     * @return
     */
    protected int resolveUnitId(DataPoint point) {
        if (point.getUnitId() != null) {
            return point.getUnitId();
        }

        Object v = deviceInfo.getProtocolConfig().get("slaveId");
        return v instanceof Integer ? (Integer) v : 1;
    }

    /**
     * 获取设备状态信息
     */
    protected Map<String, Object> getBaseDeviceStatus(String protocolType) {
        Map<String, Object> status = new HashMap<>();
        status.put("protocol", protocolType);
        status.put("slaveId", "1");
        status.put("timeout", timeout);
        status.put("clientConnected", isConnected());

        // 统计订阅信息
        int totalSubscribed = 0;
        Map<String, Integer> subscribedByType = new HashMap<>();
        for (Map.Entry<RegisterType, Map<Integer, DataPoint>> entry : registerCache.entrySet()) {
            int count = entry.getValue().size();
            subscribedByType.put(entry.getKey().name(), count);
            totalSubscribed += count;
        }
        status.put("subscribedPoints", totalSubscribed);
        status.put("subscribedByType", subscribedByType);

        return status;
    }

    /**
     * 订阅点位
     */
    @Override
    protected void doSubscribe(List<DataPoint> points) {
        log.info("Modbus订阅: 数量={}", points.size());

        for (DataPoint point : points) {
            try {
                ModbusAddress address = parseModbusAddress(point.getAddress());
                RegisterType type = address.getRegisterType();

                registerCache.computeIfAbsent(type, k -> new ConcurrentHashMap<>())
                        .put(address.getAddress(), point);
            } catch (Exception e) {
                log.error("订阅点位失败: {}", point.getAddress(), e);
            }
        }
    }

    /**
     * 取消订阅
     */
    @Override
    protected void doUnsubscribe(List<DataPoint> points) {
        log.info("取消Modbus订阅: 数量={}", points.size());

        if (points.isEmpty()) {
            registerCache.clear();
        } else {
            for (DataPoint point : points) {
                try {
                    ModbusAddress address = parseModbusAddress(point.getAddress());
                    RegisterType type = address.getRegisterType();

                    Map<Integer, DataPoint> typeCache = registerCache.get(type);
                    if (typeCache != null) {
                        typeCache.remove(address.getAddress());

                        if (typeCache.isEmpty()) {
                            registerCache.remove(type);
                        }
                    }
                } catch (Exception e) {
                    log.error("取消订阅点位失败: {}", point.getAddress(), e);
                }
            }
        }
    }

    /**
     * 批量写入点位
     */
    @Override
    protected Map<String, Boolean> doWritePoints(Map<DataPoint, Object> points) throws Exception {
        Map<String, Boolean> results = new HashMap<>();

        for (Map.Entry<DataPoint, Object> entry : points.entrySet()) {
            DataPoint point = entry.getKey();
            Object value = entry.getValue();

            try {
                boolean success = doWritePoint(point, value);
                results.put(point.getPointId(), success);
            } catch (Exception e) {
                results.put(point.getPointId(), false);
                log.error("写入点位失败: {}", point.getPointName(), e);
            }
        }

        return results;
    }

    /**
     * 检查连接状态
     */
    public abstract boolean isConnected();
}