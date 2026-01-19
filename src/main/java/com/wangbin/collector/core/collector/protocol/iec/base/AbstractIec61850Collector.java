package com.wangbin.collector.core.collector.protocol.iec.base;

import com.beanit.iec61850bean.*;
import com.wangbin.collector.common.domain.entity.DeviceConnection;
import com.wangbin.collector.common.domain.entity.DeviceInfo;
import com.wangbin.collector.core.collector.protocol.base.BaseCollector;
import com.wangbin.collector.core.collector.protocol.iec.domain.Iec61850Address;
import com.wangbin.collector.core.config.CollectorProperties;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * IEC 61850 采集公共逻辑。
 */
@Slf4j
public abstract class AbstractIec61850Collector extends BaseCollector {

    protected ClientSap clientSap;
    protected ClientAssociation association;
    protected ServerModel serverModel;

    protected String host;
    protected int port = 102;
    protected int timeout = 10000;
    protected CollectorProperties.Iec61850Config iec61850Config;

    private final Map<String, BasicDataAttribute> attributeCache = new ConcurrentHashMap<>();

    protected void initIec61850Config(DeviceInfo deviceInfo) {
        this.iec61850Config = collectorProperties != null
                ? collectorProperties.getIec61850()
                : new CollectorProperties.Iec61850Config();

        this.host = deviceInfo.getIpAddress();
        this.port = deviceInfo.getPort() != null ? deviceInfo.getPort() : 102;
        DeviceConnection connectionConfig = deviceInfo.getConnectionConfig();
        this.timeout = connectionConfig.getTimeout();
    }

    protected ClientEventListener createClientEventListener() {
        return new ClientEventListener() {
            @Override
            public void newReport(Report report) {
                handleReport(report);
            }

            @Override
            public void associationClosed(IOException e) {
                handleAssociationClosed(e);
            }
        };
    }

    protected void connectAssociation() throws Exception {
        initIec61850Config(deviceInfo);
        clientSap = new ClientSap();
        clientSap.setResponseTimeout(timeout);
        InetAddress inetAddress = InetAddress.getByName(host);
        association = clientSap.associate(inetAddress, port, null, createClientEventListener());
        association.setResponseTimeout(timeout);
        log.info("IEC61850 连接成功: {}:{}", host, port);
        reloadServerModel();
    }

    protected void closeAssociation() {
        if (association != null) {
            try {
                association.close();
            } catch (Exception e) {
                log.warn("关闭IEC61850连接异常", e);
            } finally {
                association = null;
            }
        }
        clientSap = null;
        serverModel = null;
        attributeCache.clear();
    }

    protected synchronized void reloadServerModel() throws ServiceError, IOException {
        if (association == null) {
            throw new IllegalStateException("IEC61850尚未建立连接");
        }
        serverModel = association.retrieveModel();
        attributeCache.clear();
        log.info("IEC61850 模型加载完成, logical devices={}", serverModel.getChildren().size());
    }

    protected Object readAttribute(Iec61850Address address) throws Exception {
        BasicDataAttribute attribute = resolveAttribute(address);
        association.getDataValues(attribute);
        lastActivityTime = System.currentTimeMillis();
        return convertValue(attribute);
    }

    protected boolean writeAttribute(Iec61850Address address, Object value) throws Exception {
        BasicDataAttribute attribute = resolveAttribute(address);
        applyWriteValue(attribute, value);
        association.setDataValues(attribute);
        lastActivityTime = System.currentTimeMillis();
        return true;
    }

    protected ModelNode findModelNode(Iec61850Address address) {
        if (serverModel == null) {
            throw new IllegalStateException("IEC61850 模型未加载");
        }
        return serverModel.findModelNode(address.getObjectReference(), address.getFunctionalConstraint());
    }

    protected BasicDataAttribute resolveAttribute(Iec61850Address address) {
        String key = address.getCacheKey();
        BasicDataAttribute cached = attributeCache.get(key);
        if (cached != null) {
            return cached;
        }
        ModelNode node = findModelNode(address);
        if (node == null) {
            throw new IllegalArgumentException("未找到IEC61850节点: " + address.getObjectReference()
                    + "@" + address.getFunctionalConstraint());
        }
        if (!(node instanceof BasicDataAttribute attribute)) {
            throw new IllegalArgumentException("地址不是基础数据属性: " + address.getObjectReference());
        }
        attributeCache.putIfAbsent(key, attribute);
        return attribute;
    }

    protected Object convertValue(BasicDataAttribute attribute) {
        if (attribute instanceof BdaBoolean bdaBoolean) {
            return bdaBoolean.getValue();
        }
        if (attribute instanceof BdaFloat32 bdaFloat32) {
            return bdaFloat32.getFloat();
        }
        if (attribute instanceof BdaFloat64 bdaFloat64) {
            return bdaFloat64.getDouble();
        }
        if (attribute instanceof BdaInt8 bdaInt8) {
            return (int) bdaInt8.getValue();
        }
        if (attribute instanceof BdaInt16 bdaInt16) {
            return bdaInt16.getValue();
        }
        if (attribute instanceof BdaInt32 bdaInt32) {
            return bdaInt32.getValue();
        }
        if (attribute instanceof BdaInt64 bdaInt64) {
            return bdaInt64.getValue();
        }
        if (attribute instanceof BdaInt8U bdaInt8U) {
            return (int) bdaInt8U.getValue();
        }
        if (attribute instanceof BdaInt16U bdaInt16U) {
            return bdaInt16U.getValue();
        }
        if (attribute instanceof BdaInt32U bdaInt32U) {
            return bdaInt32U.getValue();
        }
        if (attribute instanceof BdaVisibleString visibleString) {
            return visibleString.getStringValue();
        }
        if (attribute instanceof BdaOctetString bdaOctetString) {
            byte[] value = bdaOctetString.getValue();
            return value != null ? HexFormat.of().formatHex(value) : "";
        }
        if (attribute instanceof BdaDoubleBitPos doubleBitPos) {
            return doubleBitPos.getDoubleBitPos().name();
        }
        return attribute.getValueString();
    }

    protected void applyWriteValue(BasicDataAttribute attribute, Object raw) {
        if (attribute instanceof BdaBoolean bdaBoolean) {
            bdaBoolean.setValue(parseBoolean(raw));
            return;
        }
        if (attribute instanceof BdaFloat32 bdaFloat32) {
            bdaFloat32.setFloat(parseNumber(raw).floatValue());
            return;
        }
        if (attribute instanceof BdaFloat64 bdaFloat64) {
            bdaFloat64.setDouble(parseNumber(raw).doubleValue());
            return;
        }
        if (attribute instanceof BdaInt8 bdaInt8) {
            bdaInt8.setValue(parseNumber(raw).byteValue());
            return;
        }
        if (attribute instanceof BdaInt16 bdaInt16) {
            bdaInt16.setValue(parseNumber(raw).shortValue());
            return;
        }
        if (attribute instanceof BdaInt32 bdaInt32) {
            bdaInt32.setValue(parseNumber(raw).intValue());
            return;
        }
        if (attribute instanceof BdaInt64 bdaInt64) {
            bdaInt64.setValue(parseNumber(raw).longValue());
            return;
        }
        if (attribute instanceof BdaInt8U bdaInt8U) {
            bdaInt8U.setValue(parseNumber(raw).shortValue());
            return;
        }
        if (attribute instanceof BdaInt16U bdaInt16U) {
            bdaInt16U.setValue(parseNumber(raw).intValue());
            return;
        }
        if (attribute instanceof BdaInt32U bdaInt32U) {
            bdaInt32U.setValue(parseNumber(raw).intValue());
            return;
        }
        if (attribute instanceof BdaVisibleString visibleString) {
            visibleString.setValue(String.valueOf(raw));
            return;
        }
        if (attribute instanceof BdaOctetString octetString) {
            octetString.setValue(String.valueOf(raw).getBytes());
            return;
        }
        throw new IllegalArgumentException("不支持写入的类型: " + attribute.getClass().getSimpleName());
    }

    private Number parseNumber(Object raw) {
        if (raw instanceof Number number) {
            return number;
        }
        try {
            return Double.parseDouble(String.valueOf(raw));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("不是有效的数值: " + raw);
        }
    }

    private boolean parseBoolean(Object raw) {
        if (raw instanceof Boolean bool) {
            return bool;
        }
        String text = String.valueOf(raw).trim().toLowerCase();
        if ("1".equals(text) || "true".equals(text) || "on".equals(text)) {
            return true;
        }
        if ("0".equals(text) || "false".equals(text) || "off".equals(text)) {
            return false;
        }
        throw new IllegalArgumentException("不是有效的布尔值: " + raw);
    }

    protected void handleReport(Report report) {
        lastActivityTime = System.currentTimeMillis();
        if (report == null) {
            return;
        }
        List<FcModelNode> values = report.getValues();
        if (values == null) {
            return;
        }
        for (FcModelNode node : values) {
            if (node == null) {
                continue;
            }
            if (node instanceof BasicDataAttribute attribute) {
                processReportAttribute(attribute);
            } else {
                for (BasicDataAttribute child : node.getBasicDataAttributes()) {
                    processReportAttribute(child);
                }
            }
        }
    }

    private void processReportAttribute(BasicDataAttribute attribute) {
        if (attribute == null) {
            return;
        }
        String reference = attribute.getReference().toString();
        Fc fc = attribute.getFc();
        String key = reference + "@" + fc;
        attributeCache.put(key, attribute);
        Object value = convertValue(attribute);
        handleReportValue(attribute, value);
        Map<String, Object> payload = new HashMap<>();
        payload.put("reference", reference);
        payload.put("fc", fc.name());
        payload.put("value", value);
        log.debug("IEC61850 报告: {}", payload);
    }

    protected void handleReportValue(BasicDataAttribute attribute, Object value) {
        // 默认不下发，由子类重写
    }

    protected void handleAssociationClosed(IOException e) {
        log.warn("IEC61850 连接被关闭: {}", e != null ? e.getMessage() : "远端关闭");
        connected = false;
        connectionStatus = "DISCONNECTED";
        lastDisconnectTime = System.currentTimeMillis();
        if (e != null) {
            lastError = e.getMessage();
        }
    }
}
