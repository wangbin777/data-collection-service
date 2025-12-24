package com.wangbin.collector.core.collector.protocol.snmp.base;

import com.wangbin.collector.common.domain.entity.DeviceInfo;
import com.wangbin.collector.core.collector.protocol.base.BaseCollector;
import com.wangbin.collector.core.collector.protocol.snmp.domain.SnmpAddress;
import com.wangbin.collector.core.collector.protocol.snmp.domain.SnmpDataType;
import com.wangbin.collector.core.collector.protocol.snmp.util.SnmpUtils;
import com.wangbin.collector.core.config.CollectorProperties;
import com.wangbin.collector.core.connection.adapter.ConnectionAdapter;
import com.wangbin.collector.core.connection.adapter.SnmpConnectionAdapter;
import com.wangbin.collector.core.connection.model.ConnectionConfig;
import lombok.extern.slf4j.Slf4j;
import org.snmp4j.PDU;
import org.snmp4j.Snmp;
import org.snmp4j.Target;
import org.snmp4j.event.ResponseEvent;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.UdpAddress;
import org.snmp4j.smi.Variable;
import org.snmp4j.smi.VariableBinding;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * SNMP 公共能力抽象。
 */
@Slf4j
public abstract class AbstractSnmpCollector extends BaseCollector {

    protected SnmpConnectionAdapter snmpConnection;
    protected ConnectionConfig managedConnectionConfig;

    protected CollectorProperties.SnmpConfig snmpConfig;

    protected String host;
    protected int port = 161;
    protected String community = "public";
    protected int timeout = 5000;
    protected int retries = 1;
    protected int version = SnmpConstants.version2c;
    protected String versionText = "2c";

    protected void initSnmpConfig(DeviceInfo deviceInfo) {
        this.snmpConfig = collectorProperties != null
                ? collectorProperties.getSnmp()
                : new CollectorProperties.SnmpConfig();

        Map<String, Object> config = deviceInfo.getProtocolConfig() != null
                ? deviceInfo.getProtocolConfig()
                : Collections.emptyMap();

        host = deviceInfo.getIpAddress();
        if (host == null || host.isBlank()) {
            host = Objects.toString(config.getOrDefault("ipAddress", "127.0.0.1"));
        }

        port = parseInt(config.get("port"),
                deviceInfo.getPort() != null ? deviceInfo.getPort() : port);
        community = Objects.toString(
                config.getOrDefault("community", snmpConfig != null
                        ? snmpConfig.getCommunity()
                        : community));
        timeout = parseInt(config.get("timeout"),
                snmpConfig != null ? snmpConfig.getTimeout() : timeout);
        retries = parseInt(config.get("retries"),
                snmpConfig != null ? snmpConfig.getRetries() : retries);

        versionText = Objects.toString(
                config.getOrDefault("version",
                        snmpConfig != null ? snmpConfig.getVersion() : "2c"),
                "2c").trim();

        switch (versionText) {
            case "1":
                version = SnmpConstants.version1;
                break;
            case "3":
                version = SnmpConstants.version3;
                log.warn("SNMPv3暂未实现安全参数，当前默认按照v2c处理");
                version = SnmpConstants.version2c;
                break;
            case "2c":
            default:
                version = SnmpConstants.version2c;
        }
    }

    protected void initSnmpConnection() throws Exception {
        this.managedConnectionConfig = buildConnectionConfig();
        ConnectionAdapter adapter = connectionManager.createConnection(managedConnectionConfig);
        connectionManager.connect(deviceInfo.getDeviceId());
        if (!(adapter instanceof SnmpConnectionAdapter snmpAdapter)) {
            throw new IllegalStateException("SNMP连接适配器类型不匹配");
        }
        this.snmpConnection = snmpAdapter;
    }

    protected void closeSnmpConnection() {
        if (connectionManager != null && deviceInfo != null) {
            connectionManager.removeConnection(deviceInfo.getDeviceId());
        }
        snmpConnection = null;
        managedConnectionConfig = null;
    }

    protected ConnectionConfig buildConnectionConfig() {
        ConnectionConfig config = new ConnectionConfig();
        config.setDeviceId(deviceInfo.getDeviceId());
        config.setDeviceName(deviceInfo.getDeviceName());
        config.setProtocolType(getProtocolType());
        config.setConnectionType("SNMP");
        config.setHost(host);
        config.setPort(port);
        config.setReadTimeout(timeout);
        config.setWriteTimeout(timeout);
        Map<String, Object> connectionExtras = deviceInfo.getConnectionConfig();
        config.setConnectTimeout(parseInt(
                connectionExtras != null ? connectionExtras.get("connectTimeout") : null,
                5000));
        config.setGroupId(deviceInfo.getGroupId());
        config.setMaxPendingMessages(parseInt(
                connectionExtras != null
                        ? connectionExtras.get("maxPendingMessages")
                        : null, 1024));
        config.setDispatchBatchSize(parseInt(
                connectionExtras != null
                        ? connectionExtras.get("dispatchBatchSize")
                        : null, 1));
        config.setDispatchFlushInterval(parseLong(
                connectionExtras != null ? connectionExtras.get("dispatchFlushInterval") : null, 0L));
        String overflow = connectionExtras != null
                ? Objects.toString(connectionExtras.get("overflowStrategy"), "BLOCK")
                : "BLOCK";
        config.setOverflowStrategy(overflow);

        Map<String, Object> protocol = new HashMap<>();
        protocol.put("community", community);
        protocol.put("snmpVersion", versionText);
        protocol.put("snmpRetries", retries);
        config.setProtocolConfig(protocol);
        if (connectionExtras != null) {
            config.setExtraParams(new HashMap<>(connectionExtras));
        }
        return config;
    }

    protected Map<String, Variable> performGet(List<SnmpAddress> addresses) throws IOException {
        try {
            return executeSnmp((snmp, target) -> {
                Map<String, Variable> values = new LinkedHashMap<>();
                for (List<SnmpAddress> chunk : SnmpUtils.partition(addresses, 10)) {
                    PDU pdu = createPdu(PDU.GET, chunk);
                    ResponseEvent<UdpAddress> event = snmp.send(pdu, target);
                    PDU response = validateResponse(event);
                    for (int i = 0; i < response.size(); i++) {
                        VariableBinding binding = response.get(i);
                        values.put(binding.getOid().toDottedString(), binding.getVariable());
                    }
                }
                return values;
            });
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("SNMP GET 执行失败", e);
        }
    }

    protected void performSet(Map<SnmpAddress, Object> values) throws IOException {
        if (values.isEmpty()) {
            return;
        }
        try {
            executeSnmp((snmp, target) -> {
                List<VariableBinding> bindings = new java.util.ArrayList<>();
                for (Map.Entry<SnmpAddress, Object> entry : values.entrySet()) {
                    SnmpAddress address = entry.getKey();
                    Variable variable = SnmpUtils.toVariable(entry.getValue(), address.getDataType());
                    bindings.add(new VariableBinding(new OID(address.getOid()), variable));
                }
                for (List<VariableBinding> chunk : SnmpUtils.partitionBindings(bindings, 10)) {
                    PDU pdu = new PDU();
                    chunk.forEach(pdu::add);
                    pdu.setType(PDU.SET);
                    ResponseEvent<UdpAddress> event = snmp.send(pdu, target);
                    validateResponse(event);
                }
                return null;
            });
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("SNMP SET 执行失败", e);
        }
    }

    protected List<VariableBinding> performWalk(String rootOid, int maxNodes) throws IOException {
        try {
            return executeSnmp((snmp, target) -> {
                List<VariableBinding> nodes = new java.util.ArrayList<>();
                OID currentRoot = new OID(rootOid);
                OID current = currentRoot;
                int count = 0;
                while (count < maxNodes) {
                    PDU pdu = new PDU();
                    pdu.add(new VariableBinding(current));
                    pdu.setType(PDU.GETNEXT);
                    ResponseEvent<UdpAddress> event = snmp.send(pdu, target);
                    PDU response = validateResponse(event);
                    if (response.size() == 0) {
                        break;
                    }
                    VariableBinding vb = response.get(0);
                    if (!vb.getOid().startsWith(currentRoot)) {
                        break;
                    }
                    nodes.add(vb);
                    current = vb.getOid();
                    count++;
                }
                return nodes;
            });
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("SNMP WALK 执行失败", e);
        }
    }

    protected PDU createPdu(int type, List<SnmpAddress> addresses) {
        PDU pdu = new PDU();
        for (SnmpAddress address : addresses) {
            pdu.add(new VariableBinding(new OID(address.getOid())));
        }
        pdu.setType(type);
        return pdu;
    }

    protected PDU validateResponse(ResponseEvent<UdpAddress> event) throws IOException {
        if (event == null || event.getResponse() == null) {
            throw new IOException("SNMP请求超时或无响应");
        }
        PDU response = event.getResponse();
        if (response.getErrorStatus() != PDU.noError) {
            throw new IOException("SNMP错误: " + response.getErrorStatusText());
        }
        return response;
    }

    protected Object convertVariable(Variable variable, SnmpDataType dataType) {
        return SnmpUtils.variableToJava(variable, dataType);
    }

    protected Variable convertForWrite(Object value, SnmpDataType dataType) {
        return SnmpUtils.toVariable(value, dataType);
    }

    protected <T> T executeSnmp(SnmpConnectionAdapter.SnmpCallable<T> callable) throws Exception {
        if (snmpConnection == null) {
            throw new IllegalStateException("SNMP连接尚未建立");
        }
        return snmpConnection.execute(callable, timeout);
    }

    protected Snmp getSnmpClient() {
        return snmpConnection != null ? snmpConnection.getSnmp() : null;
    }

    protected Target<UdpAddress> getSnmpTarget() {
        return snmpConnection != null ? snmpConnection.getTarget() : null;
    }

    private int parseInt(Object raw, int defaultValue) {
        if (raw == null) {
            return defaultValue;
        }
        if (raw instanceof Number number) {
            return number.intValue();
        }
        try {
            return Integer.parseInt(raw.toString());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private long parseLong(Object raw, long defaultValue) {
        if (raw == null) {
            return defaultValue;
        }
        if (raw instanceof Number number) {
            return number.longValue();
        }
        try {
            return Long.parseLong(raw.toString());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}
