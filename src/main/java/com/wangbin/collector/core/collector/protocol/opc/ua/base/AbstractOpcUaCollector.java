package com.wangbin.collector.core.collector.protocol.opc.ua.base;

import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.common.domain.entity.DeviceInfo;
import com.wangbin.collector.core.collector.protocol.base.BaseCollector;
import com.wangbin.collector.core.collector.protocol.opc.ua.domain.OpcUaAddress;
import com.wangbin.collector.core.collector.protocol.opc.ua.util.OpcUaAddressParser;
import com.wangbin.collector.core.config.CollectorProperties;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.milo.opcua.sdk.client.DiscoveryClient;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.OpcUaClientConfigBuilder;
import org.eclipse.milo.opcua.sdk.client.identity.AnonymousProvider;
import org.eclipse.milo.opcua.sdk.client.identity.IdentityProvider;
import org.eclipse.milo.opcua.sdk.client.identity.UsernameProvider;
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
import org.eclipse.milo.opcua.stack.core.types.structured.EndpointDescription;
import org.eclipse.milo.opcua.stack.core.types.structured.MonitoringFilter;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * OPC UA 抽象基类。
 */
@Slf4j
public abstract class AbstractOpcUaCollector extends BaseCollector {

    protected OpcUaClient client;
    protected CollectorProperties.OpcUaConfig opcUaConfig;

    protected String endpointUrl;
    protected String securityPolicy;
    protected String username;
    protected String password;
    protected int requestTimeout = 5000;
    protected double subscriptionInterval = 1000;

    protected final Map<String, OpcUaSubscription> subscriptions = new ConcurrentHashMap<>();

    protected void initOpcUaConfig(DeviceInfo deviceInfo) {
        this.opcUaConfig = collectorProperties != null
                ? collectorProperties.getOpcUa()
                : new CollectorProperties.OpcUaConfig();

        Map<String, Object> protocol = deviceInfo.getProtocolConfig();
        endpointUrl = protocol != null && protocol.get("endpointUrl") != null
                ? protocol.get("endpointUrl").toString()
                : protocol != null && protocol.get("endpoint") != null
                ? protocol.get("endpoint").toString()
                : "opc.tcp://127.0.0.1:4840";
        securityPolicy = protocol != null && protocol.get("securityPolicy") != null
                ? protocol.get("securityPolicy").toString()
                : opcUaConfig.getSecurityPolicy();
        username = protocol != null && protocol.get("username") != null
                ? protocol.get("username").toString()
                : null;
        password = protocol != null && protocol.get("password") != null
                ? protocol.get("password").toString()
                : null;
        requestTimeout = protocol != null && protocol.get("requestTimeout") != null
                ? Integer.parseInt(protocol.get("requestTimeout").toString())
                : opcUaConfig.getRequestTimeout();
        subscriptionInterval = opcUaConfig.getSubscriptionInterval();
    }

    protected void connectClient() throws Exception {
        List<EndpointDescription> endpoints = discoverEndpoints(endpointUrl);
        EndpointDescription endpoint = selectEndpoint(endpoints);

        OpcUaClientConfigBuilder builder = new OpcUaClientConfigBuilder();
        builder.setEndpoint(endpoint);
        builder.setRequestTimeout(Unsigned.uint(requestTimeout));
        builder.setIdentityProvider(resolveIdentityProvider());

        client = OpcUaClient.create(builder.build());
        client.connect();
        log.info("OPC UA连接成功 endpoint={}", endpointUrl);
    }

    protected void disconnectClient() {
        subscriptions.values().forEach(subscription -> {
            try {
                subscription.delete();
            } catch (Exception e) {
                log.debug("删除订阅失败", e);
            }
        });
        subscriptions.clear();
        if (client != null) {
            try {
                client.disconnect();
            } catch (Exception e) {
                log.warn("OPC UA断开异常", e);
            }
            client = null;
        }
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

    private List<EndpointDescription> discoverEndpoints(String url) throws Exception {
        try {
            return DiscoveryClient.getEndpoints(url).get();
        } catch (Exception e) {
            if (!url.endsWith("/discovery")) {
                return DiscoveryClient.getEndpoints(url + "/discovery").get();
            }
            throw e;
        }
    }

    private EndpointDescription selectEndpoint(List<EndpointDescription> endpoints) {
        if (endpoints == null || endpoints.isEmpty()) {
            throw new IllegalStateException("没有可用的OPC UA端点: " + endpointUrl);
        }
        if (securityPolicy == null || securityPolicy.isBlank()) {
            return endpoints.get(0);
        }
        return endpoints.stream()
                .filter(e -> e.getSecurityPolicyUri() != null && e.getSecurityPolicyUri().contains(securityPolicy))
                .findFirst()
                .orElse(endpoints.get(0));
    }

    private IdentityProvider resolveIdentityProvider() {
        if (username != null && !username.isBlank()) {
            return new UsernameProvider(username, password != null ? password : "");
        }
        return new AnonymousProvider();
    }
}
