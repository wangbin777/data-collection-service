package com.wangbin.collector.core.collector.protocol.opc.ua.base;

import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.common.domain.entity.DeviceConnection;
import com.wangbin.collector.common.domain.entity.DeviceInfo;
import com.wangbin.collector.core.collector.protocol.base.BaseCollector;
import com.wangbin.collector.core.collector.protocol.opc.ua.domain.OpcUaAddress;
import com.wangbin.collector.core.collector.protocol.opc.ua.util.OpcUaAddressParser;
import com.wangbin.collector.core.connection.adapter.ConnectionAdapter;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.subscriptions.MonitoredItemServiceOperationResult;
import org.eclipse.milo.opcua.sdk.client.subscriptions.OpcUaMonitoredItem;
import org.eclipse.milo.opcua.sdk.client.subscriptions.OpcUaSubscription;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.builtin.StatusCode;
import org.eclipse.milo.opcua.stack.core.types.builtin.Variant;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.UInteger;
import org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned;
import org.eclipse.milo.opcua.stack.core.types.enumerated.DataChangeTrigger;
import org.eclipse.milo.opcua.stack.core.types.enumerated.DeadbandType;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MonitoringMode;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.eclipse.milo.opcua.stack.core.types.structured.DataChangeFilter;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoringFilter;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * OPC UA 抽象基类。
 */
@Slf4j
public abstract class AbstractOpcUaCollector extends BaseCollector {

    // 使用统一连接管理
    private ConnectionAdapter<OpcUaClient> connectionAdapter;
    protected OpcUaClient client;
    protected String endpointUrl;
    protected String securityPolicy;
    protected String username;
    protected String password;
    protected int requestTimeout = 5000;
    protected double subscriptionInterval = 1000;

    protected final Map<String, OpcUaSubscription> subscriptions = new ConcurrentHashMap<>();

    @Override
    protected void doConnect() throws Exception {
        // 初始化配置
        initOpcUaConfig(deviceInfo);
        // 创建并建立连接
        connectionAdapter = connectionManager.createConnection(deviceInfo);
        connectionAdapter.connect();
        
        // 获取OPC UA客户端（无需类型转换）
        client = connectionAdapter.getClient();
    }

    @Override
    protected void doDisconnect() {
        if (connectionAdapter != null) {
            try {
                connectionAdapter.disconnect();
                connectionManager.removeConnection(deviceInfo.getDeviceId());
            } catch (Exception e) {
                log.error("断开OPC UA连接失败", e);
            }
        }
        subscriptions.clear();
    }

    protected void initOpcUaConfig(DeviceInfo deviceInfo) {
        DeviceConnection connection = requireConnectionConfig();
        endpointUrl = connection.getUrl();
        securityPolicy = connection.getSecurityPolicy();
        username = connection.getUsername();
        password = connection.getPassword();
        requestTimeout = connection.getReadTimeout() != null ? connection.getReadTimeout() : 0;
        subscriptionInterval = connection.getSubscriptionInterval();
    }

    protected Object readValue(OpcUaAddress address) throws Exception {
        NodeId nodeId = address.toNodeId();
        DataValue value = client.readValue(0, TimestampsToReturn.Both, nodeId);
        return value.getValue().getValue();
    }

    protected Map<String, Object> readValues(List<DataPoint> points) throws Exception {
        List<NodeId> nodeIds = new ArrayList<>();
        for (DataPoint point : points) {
            OpcUaAddress address = OpcUaAddressParser.parse(point);
            nodeIds.add(address.toNodeId());
        }
        List<DataValue> values = client.readValues(0, TimestampsToReturn.Both, nodeIds);
        Map<String, Object> result = new HashMap<>();
        for (int i = 0; i < points.size(); i++) {
            DataValue value = values.get(i);
            result.put(points.get(i).getPointId(),
                    value != null ? value.getValue().getValue() : null);
        }
        return result;
    }

    protected boolean writeValue(OpcUaAddress address, Object rawValue) throws Exception {
        Variant variant = OpcUaAddressParser.toVariant(rawValue, address.getDataType());
        List<StatusCode> results = client.writeValues(
                List.of(address.toNodeId()),
                List.of(DataValue.valueOnly(variant))
        );
        return !results.isEmpty() && results.get(0).isGood();
    }

    protected OpcUaSubscription createSubscription() throws Exception {
        OpcUaSubscription subscription = new OpcUaSubscription(client, subscriptionInterval);
        subscription.create();
        String key = subscription.getSubscriptionId()
                .map(UInteger::toString)
                .orElse(UUID.randomUUID().toString());
        subscriptions.put(key, subscription);
        return subscription;
    }

    protected OpcUaMonitoredItem addMonitoredItem(OpcUaSubscription subscription,
                                                  OpcUaAddress address,
                                                  Consumer<OpcUaMonitoredItem> configurator) throws Exception {
        OpcUaMonitoredItem item = OpcUaMonitoredItem.newDataItem(address.toNodeId(), MonitoringMode.Reporting);

        Double publishingInterval = subscription.getPublishingInterval();
        double sampling = address.getSamplingInterval() > 0
                ? address.getSamplingInterval()
                : (publishingInterval != null ? publishingInterval : subscriptionInterval);
        item.setSamplingInterval(sampling);
        item.setQueueSize(Unsigned.uint(Math.max(1, address.getQueueSize())));
        MonitoringFilter filter = buildFilter(address);
        if (filter != null) {
            item.setFilter(filter);
        }
        if (configurator != null) {
            configurator.accept(item);
        }

        subscription.addMonitoredItem(item);
        List<MonitoredItemServiceOperationResult> results = subscription.createMonitoredItems(candidate -> candidate == item);
        MonitoredItemServiceOperationResult result = results.isEmpty() ? null : results.get(0);
        if (result == null || !result.isGood()) {
            throw new IllegalStateException("Failed to create OPC UA monitored item: "
                    + (result != null ? result.serviceResult() : "unknown"));
        }
        return item;
    }

    private MonitoringFilter buildFilter(OpcUaAddress address) {
        if (address.getDeadband() <= 0) {
            return null;
        }
        return new DataChangeFilter(
                DataChangeTrigger.StatusValue,
                Unsigned.uint(DeadbandType.Absolute.getValue()),
                address.getDeadband()
        );
    }
}
