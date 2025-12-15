package com.wangbin.collector.core.collector.protocol.modbus.base;

import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.core.collector.protocol.base.BaseCollector;
import com.wangbin.collector.core.collector.protocol.modbus.domain.ModbusAddress;
import com.wangbin.collector.core.collector.protocol.modbus.domain.GroupedPoint;
import com.wangbin.collector.core.collector.protocol.modbus.domain.RegisterType;
import com.wangbin.collector.core.collector.protocol.modbus.utils.ModbusGroupingUtil;
import com.wangbin.collector.core.collector.protocol.modbus.utils.ModbusParser;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Modbus采集器抽象基类
 */
@Slf4j
public abstract class AbstractModbusCollector extends BaseCollector {

    protected int timeout = 3000;
    protected int slaveId = 1;

    // 订阅缓存
    protected final Map<RegisterType, Map<Integer, DataPoint>> registerCache = new ConcurrentHashMap<>();

    // =============== 公共方法 ===============

    /**
     * 解析Modbus地址
     */
    protected ModbusAddress parseModbusAddress(String addressStr) {
        return ModbusParser.parseModbusAddress(addressStr);
    }

    /**
     * 按连续地址分组
     */
    protected List<List<GroupedPoint>> groupContinuousPoints(List<GroupedPoint> points) {
        return ModbusGroupingUtil.groupByContinuousAddress(points);
    }

    /**
     * 获取设备状态信息
     */
    protected Map<String, Object> getBaseDeviceStatus(String protocolType) {
        Map<String, Object> status = new HashMap<>();
        status.put("protocol", protocolType);
        status.put("slaveId", slaveId);
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