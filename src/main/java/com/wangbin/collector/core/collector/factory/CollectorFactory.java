package com.wangbin.collector.core.collector.factory;

import com.wangbin.collector.common.domain.entity.DeviceInfo;
import com.wangbin.collector.common.exception.CollectorException;
import com.wangbin.collector.core.collector.protocol.base.ProtocolCollector;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.AutowireCapableBeanFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * 采集器工厂
 */
@Slf4j
@Component
public class CollectorFactory {

    @Autowired
    private AutowireCapableBeanFactory beanFactory;

    private final Map<String, CollectorCreator> collectorCreators = new HashMap<>();

    public CollectorFactory() {
        // 注册所有采集器创建器
        registerCollectorCreators();
    }

    /**
     * 创建采集器
     */
    public ProtocolCollector createCollector(DeviceInfo deviceInfo) throws CollectorException {
        String protocolType = deviceInfo.getProtocolType();

        if (protocolType == null || protocolType.isEmpty()) {
            throw new IllegalArgumentException("协议类型不能为空");
        }

        CollectorCreator creator = collectorCreators.get(protocolType.toUpperCase());
        if (creator == null) {
            throw new CollectorException(
                    String.format("不支持的协议类型: %s", protocolType),
                    deviceInfo.getDeviceId(), null);
        }

        try {
            ProtocolCollector collector = creator.create(deviceInfo);
            collector.init(deviceInfo);

            log.info("采集器创建成功: {} [{}]",
                    deviceInfo.getDeviceId(), protocolType);

            return collector;
        } catch (Exception e) {
            log.error("采集器创建失败: {} [{}]",
                    deviceInfo.getDeviceId(), protocolType, e);
            throw new CollectorException("采集器创建失败",
                    deviceInfo.getDeviceId(), null, e);
        }
    }

    /**
     * 注册采集器
     */
    public void registerCollector(String protocolType, CollectorCreator creator) {
        collectorCreators.put(protocolType.toUpperCase(), creator);
        log.info("注册采集器: {}", protocolType);
    }

    /**
     * 获取支持的协议类型
     */
    public String[] getSupportedProtocols() {
        return collectorCreators.keySet().toArray(new String[0]);
    }

    /**
     * 是否支持协议类型
     */
    public boolean supportsProtocol(String protocolType) {
        return collectorCreators.containsKey(protocolType.toUpperCase());
    }

    /**
     * 注册所有采集器创建器
     */
    private void registerCollectorCreators() {
        // Modbus协议
        registerCollector("MODBUS_TCP", deviceInfo -> instantiateCollector(
                "MODBUS_TCP",
                "com.wangbin.collector.core.collector.protocol.modbus.ModbusTcpCollector"));

        registerCollector("MODBUS_RTU", deviceInfo -> instantiateCollector(
                "MODBUS_RTU",
                "com.wangbin.collector.core.collector.protocol.modbus.ModbusRtuCollector"));

        // OPC协议
        registerCollector("OPC_DA", deviceInfo -> instantiateCollector(
                "OPC_DA",
                "com.wangbin.collector.core.collector.protocol.opc.OpcDaCollector"));

        registerCollector("OPC_UA", deviceInfo -> instantiateCollector(
                "OPC_UA",
                "com.wangbin.collector.core.collector.protocol.opc.OpcUaCollector"));

        // SNMP协议
        registerCollector("SNMP", deviceInfo -> instantiateCollector(
                "SNMP",
                "com.wangbin.collector.core.collector.protocol.snmp.SnmpCollector"));

        // MQTT协议
        registerCollector("MQTT", deviceInfo -> instantiateCollector(
                "MQTT",
                "com.wangbin.collector.core.collector.protocol.mqtt.MqttCollector"));

        // IEC协议
        registerCollector("IEC104", deviceInfo -> instantiateCollector(
                "IEC104",
                "com.wangbin.collector.core.collector.protocol.iec.Iec104Collector"));

        registerCollector("IEC61850", deviceInfo -> instantiateCollector(
                "IEC61850",
                "com.wangbin.collector.core.collector.protocol.iec.Iec61850Collector"));

        // 自定义协议
        registerCollector("CUSTOM_TCP", deviceInfo -> instantiateCollector(
                "CUSTOM_TCP",
                "com.wangbin.collector.core.collector.protocol.custom.CustomProtocolCollector"));

        // HTTP协议
        registerCollector("HTTP", deviceInfo -> instantiateCollector(
                "HTTP",
                "com.wangbin.collector.core.collector.protocol.http.HttpCollector"));

        // WebSocket协议
        registerCollector("WEBSOCKET", deviceInfo -> instantiateCollector(
                "WEBSOCKET",
                "com.wangbin.collector.core.collector.protocol.websocket.WebSocketCollector"));

        log.info("采集器工厂初始化完成，支持 {} 种协议", collectorCreators.size());
    }

    /**
     * 使用Spring BeanFactory创建采集器实例，确保AOP等BeanPostProcessor生效
     */
    private ProtocolCollector instantiateCollector(String protocolType, String className) {
        try {
            Class<?> clazz = Class.forName(className);
            if (beanFactory != null) {
                return (ProtocolCollector) beanFactory.createBean(clazz);
            }
            return (ProtocolCollector) clazz.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException(String.format("%s采集器加载失败", protocolType), e);
        }
    }

    /**
     * 采集器创建器接口
     */
    @FunctionalInterface
    public interface CollectorCreator {
        ProtocolCollector create(DeviceInfo deviceInfo) throws Exception;
    }
}

