package com.wangbin.collector.core.collector.protocol.snmp.base;

import com.wangbin.collector.common.domain.entity.DeviceInfo;
import com.wangbin.collector.core.collector.protocol.base.BaseCollector;
import com.wangbin.collector.core.collector.protocol.snmp.domain.SnmpAddress;
import com.wangbin.collector.core.collector.protocol.snmp.domain.SnmpDataType;
import com.wangbin.collector.core.collector.protocol.snmp.util.SnmpUtils;
import com.wangbin.collector.core.config.CollectorProperties;
import lombok.extern.slf4j.Slf4j;
import org.snmp4j.*;
import org.snmp4j.event.ResponseEvent;
import org.snmp4j.mp.SnmpConstants;
import org.snmp4j.smi.OID;
import org.snmp4j.smi.OctetString;
import org.snmp4j.smi.UdpAddress;
import org.snmp4j.smi.Variable;
import org.snmp4j.smi.VariableBinding;
import org.snmp4j.transport.DefaultUdpTransportMapping;

import java.io.IOException;
import java.util.*;

/**
 * SNMP 公共能力抽象。
 */
@Slf4j
public abstract class AbstractSnmpCollector extends BaseCollector {

    protected Snmp snmp;
    protected Target<UdpAddress> target;
    protected TransportMapping<UdpAddress> transport;

    protected CollectorProperties.SnmpConfig snmpConfig;

    protected String host;
    protected int port = 161;
    protected String community = "public";
    protected int timeout = 5000;
    protected int retries = 1;
    protected int version = SnmpConstants.version2c;

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

        String versionText = Objects.toString(
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

    protected void openSnmpSession() throws IOException {
        transport = new DefaultUdpTransportMapping();
        transport.listen();
        snmp = new Snmp(transport);
        target = buildCommunityTarget();
        log.info("SNMP会话建立完成 host={} port={} version={} timeout={} retries={}",
                host, port, version, timeout, retries);
    }

    protected void closeSnmpSession() {
        if (snmp != null) {
            try {
                snmp.close();
            } catch (Exception e) {
                log.warn("关闭SNMP实例异常", e);
            } finally {
                snmp = null;
            }
        }
        if (transport != null) {
            try {
                transport.close();
            } catch (Exception e) {
                log.warn("关闭SNMP传输异常", e);
            } finally {
                transport = null;
            }
        }
        target = null;
    }

    private CommunityTarget<UdpAddress> buildCommunityTarget() {
        CommunityTarget<UdpAddress> communityTarget = new CommunityTarget<>();
        communityTarget.setCommunity(new OctetString(community));
        communityTarget.setVersion(version);
        communityTarget.setRetries(retries);
        communityTarget.setTimeout(timeout);
        communityTarget.setAddress(new UdpAddress(host + "/" + port));
        return communityTarget;
    }

    protected Map<String, Variable> performGet(List<SnmpAddress> addresses) throws IOException {
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
    }

    protected void performSet(Map<SnmpAddress, Object> values) throws IOException {
        if (values.isEmpty()) {
            return;
        }
        List<VariableBinding> bindings = new ArrayList<>();
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
    }

    protected List<VariableBinding> performWalk(String rootOid, int maxNodes) throws IOException {
        List<VariableBinding> nodes = new ArrayList<>();
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
}
