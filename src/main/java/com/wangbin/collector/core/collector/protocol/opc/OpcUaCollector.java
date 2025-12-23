package com.wangbin.collector.core.collector.protocol.opc;

import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.core.collector.protocol.opc.ua.base.AbstractOpcUaCollector;
import com.wangbin.collector.core.collector.protocol.opc.ua.domain.OpcUaAddress;
import com.wangbin.collector.core.collector.protocol.opc.ua.domain.OpcUaDataType;
import com.wangbin.collector.core.collector.protocol.opc.ua.util.OpcUaAddressParser;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.milo.opcua.sdk.client.subscriptions.OpcUaMonitoredItem;
import org.eclipse.milo.opcua.sdk.client.subscriptions.OpcUaSubscription;
import org.eclipse.milo.opcua.stack.core.types.builtin.DataValue;
import org.eclipse.milo.opcua.stack.core.types.builtin.NodeId;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.eclipse.milo.opcua.stack.core.types.structured.ReferenceDescription;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * OPC UA collector implementation aligned with AbstractOpcUaCollector.
 */
@Slf4j
public class OpcUaCollector extends AbstractOpcUaCollector {

    private final Map<String, OpcUaAddress> addressCache = new ConcurrentHashMap<>();
    private final Map<String, OpcUaMonitoredItem> monitoredItems = new ConcurrentHashMap<>();
    private final Map<String, OpcUaSubscription> pointSubscriptions = new ConcurrentHashMap<>();

    @Override
    public String getCollectorType() {
        return "OPC_UA";
    }

    @Override
    public String getProtocolType() {
        return "OPC_UA";
    }

    @Override
    protected void doConnect() throws Exception {
        initOpcUaConfig(deviceInfo);
        connectClient();
    }

    @Override
    protected void doDisconnect() {
        monitoredItems.clear();
        pointSubscriptions.clear();
        addressCache.clear();
        disconnectClient();
    }

    @Override
    protected Object doReadPoint(DataPoint point) throws Exception {
        OpcUaAddress address = resolveAddress(point);
        return readValue(address);
    }

    @Override
    protected Map<String, Object> doReadPoints(List<DataPoint> points) throws Exception {
        Map<String, Object> values = new HashMap<>();
        if (points == null || points.isEmpty()) {
            return values;
        }
        List<NodeId> nodeIds = new ArrayList<>(points.size());
        for (DataPoint point : points) {
            OpcUaAddress address = resolveAddress(point);
            nodeIds.add(address.toNodeId());
        }
        List<DataValue> dataValues = client.readValues(0, TimestampsToReturn.Both, nodeIds);
        for (int i = 0; i < points.size(); i++) {
            DataValue value = dataValues.get(i);
            values.put(points.get(i).getPointId(), value.getValue() != null ? value.getValue().getValue() : null);
        }
        return values;
    }

    @Override
    protected boolean doWritePoint(DataPoint point, Object value) throws Exception {
        OpcUaAddress address = resolveAddress(point);
        return writeValue(address, value);
    }

    @Override
    protected Map<String, Boolean> doWritePoints(Map<DataPoint, Object> points) throws Exception {
        Map<String, Boolean> results = new HashMap<>();
        if (points == null || points.isEmpty()) {
            return results;
        }
        for (Map.Entry<DataPoint, Object> entry : points.entrySet()) {
            boolean success = doWritePoint(entry.getKey(), entry.getValue());
            results.put(entry.getKey().getPointId(), success);
        }
        return results;
    }

    @Override
    protected void doSubscribe(List<DataPoint> points) throws Exception {
        if (points == null || points.isEmpty()) {
            return;
        }
        OpcUaSubscription subscription = ensureSubscription();
        for (DataPoint point : points) {
            OpcUaAddress address = resolveAddress(point);
            if (!address.needSubscribe()) {
                continue;
            }
            OpcUaMonitoredItem existing = monitoredItems.remove(point.getPointId());
            if (existing != null) {
                removeMonitoredItem(point.getPointId(), existing);
            }
            OpcUaMonitoredItem item = addMonitoredItem(subscription, address,
                    monitoredItem -> monitoredItem.setDataValueListener(
                            (monitored, value) -> handleNotification(point, address, value)));
            monitoredItems.put(point.getPointId(), item);
            pointSubscriptions.put(point.getPointId(), subscription);
        }
        log.info("OPC UA subscription registered device={} active={}",
                deviceInfo.getDeviceId(), monitoredItems.size());
    }

    @Override
    protected void doUnsubscribe(List<DataPoint> points) throws Exception {
        if (points == null || points.isEmpty()) {
            for (OpcUaSubscription subscription : subscriptions.values()) {
                try {
                    subscription.delete();
                } catch (Exception e) {
                    log.warn("Failed to delete OPC UA subscription", e);
                }
            }
            subscriptions.clear();
            monitoredItems.clear();
            pointSubscriptions.clear();
            return;
        }
        for (DataPoint point : points) {
            OpcUaMonitoredItem item = monitoredItems.remove(point.getPointId());
            if (item != null) {
                removeMonitoredItem(point.getPointId(), item);
            }
            pointSubscriptions.remove(point.getPointId());
        }
    }

    @Override
    protected Map<String, Object> doGetDeviceStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("endpoint", endpointUrl);
        status.put("securityPolicy", securityPolicy);
        status.put("connected", client != null);
        status.put("subscriptions", subscriptions.size());
        status.put("monitoredItems", monitoredItems.size());
        status.put("requestTimeout", requestTimeout);
        status.put("subscriptionInterval", subscriptionInterval);
        return status;
    }

    @Override
    protected Object doExecuteCommand(int unitId, String command, Map<String, Object> params) throws Exception {
        String normalized = command != null ? command.toLowerCase(Locale.ROOT) : "";
        Map<String, Object> safeParams = params != null ? params : Collections.emptyMap();
        return switch (normalized) {
            case "read" -> executeCommandRead(safeParams);
            case "write" -> executeCommandWrite(safeParams);
            case "browse" -> executeCommandBrowse(safeParams);
            default -> throw new IllegalArgumentException("Unsupported OPC UA command: " + command);
        };
    }

    @Override
    protected void buildReadPlans(String deviceId, List<DataPoint> points) {
        addressCache.clear();
        if (points == null) {
            return;
        }
        for (DataPoint point : points) {
            try {
                addressCache.put(point.getPointId(), OpcUaAddressParser.parse(point));
            } catch (Exception e) {
                log.warn("Failed to parse OPC UA point address pointId={}", point.getPointId(), e);
            }
        }
        log.info("OPC UA loaded points device={} count={}", deviceId, addressCache.size());
    }

    private OpcUaAddress resolveAddress(DataPoint point) {
        return addressCache.computeIfAbsent(point.getPointId(), id -> OpcUaAddressParser.parse(point));
    }

    private OpcUaSubscription ensureSubscription() throws Exception {
        if (subscriptions.isEmpty()) {
            return createSubscription();
        }
        return subscriptions.values().iterator().next();
    }

    private void removeMonitoredItem(String pointId, OpcUaMonitoredItem item) {
        OpcUaSubscription subscription = pointSubscriptions.remove(pointId);
        if (subscription == null) {
            subscription = subscriptions.values().stream()
                    .filter(sub -> sub.getMonitoredItems().contains(item))
                    .findFirst()
                    .orElse(null);
        }
        if (subscription == null) {
            return;
        }
        try {
            subscription.removeMonitoredItem(item);
            subscription.deleteMonitoredItems();
        } catch (Exception e) {
            log.warn("Failed to remove OPC UA monitored item", e);
        }
    }

    private void handleNotification(DataPoint point, OpcUaAddress address, DataValue value) {
        Object payload = value != null && value.getValue() != null ? value.getValue().getValue() : null;
        log.info("OPC UA push device={} pointId={} value={}",
                deviceInfo.getDeviceId(), point.getPointId(), payload);
    }

    private Object executeCommandRead(Map<String, Object> params) throws Exception {
        List<String> nodeIds = extractNodeIds(params);
        if (nodeIds.isEmpty()) {
            throw new IllegalArgumentException("nodeId or nodeIds is required");
        }
        List<NodeId> readTargets = nodeIds.stream()
                .map(this::safeParseNodeId)
                .collect(Collectors.toList());
        List<DataValue> values = client.readValues(0, TimestampsToReturn.Both, readTargets);
        List<Map<String, Object>> response = new ArrayList<>(nodeIds.size());
        for (int i = 0; i < nodeIds.size(); i++) {
            DataValue value = values.get(i);
            response.add(Map.of(
                    "nodeId", nodeIds.get(i),
                    "value", value.getValue() != null ? value.getValue().getValue() : null,
                    "status", value.getStatusCode() != null ? value.getStatusCode().toString() : "null",
                    "sourceTimestamp", value.getSourceTime() != null ? value.getSourceTime().getJavaDate() : null
            ));
        }
        return response;
    }

    private Object executeCommandWrite(Map<String, Object> params) throws Exception {
        String nodeIdText = Objects.toString(params.get("nodeId"), "");
        if (nodeIdText.isBlank()) {
            throw new IllegalArgumentException("nodeId is required");
        }
        Object value = params.get("value");
        if (value == null) {
            throw new IllegalArgumentException("value is required");
        }
        OpcUaDataType dataType = OpcUaDataType.fromText(Objects.toString(params.get("dataType"), null));
        OpcUaAddress tempAddress = new OpcUaAddress(
                safeParseNodeId(nodeIdText),
                dataType,
                -1,
                1,
                -1,
                false
        );
        boolean success = writeValue(tempAddress, value);
        return Map.of("nodeId", nodeIdText, "status", success ? "success" : "error");
    }

    private Object executeCommandBrowse(Map<String, Object> params) throws Exception {
        String nodeIdText = Objects.toString(params.getOrDefault("nodeId", "ns=0;i=84"));
        NodeId nodeId = safeParseNodeId(nodeIdText);
        List<ReferenceDescription> references = client.getAddressSpace().browse(nodeId);
        List<Map<String, Object>> nodes = new ArrayList<>();
        for (ReferenceDescription ref : references) {
            NodeId targetNodeId = ref.getNodeId()
                    .toNodeId(client.getNamespaceTable())
                    .orElse(null);
            String targetId = targetNodeId != null
                    ? targetNodeId.toParseableString()
                    : ref.getNodeId().toParseableString();
            nodes.add(Map.of(
                    "browseName", ref.getBrowseName().getName(),
                    "displayName", ref.getDisplayName().getText(),
                    "nodeClass", ref.getNodeClass().name(),
                    "targetNodeId", targetId
            ));
        }
        return nodes;
    }

    private List<String> extractNodeIds(Map<String, Object> params) {
        Object multi = params.get("nodeIds");
        if (multi instanceof Collection<?> collection && !collection.isEmpty()) {
            return collection.stream().filter(Objects::nonNull).map(Object::toString).collect(Collectors.toList());
        }
        Object single = params.get("nodeId");
        if (single != null) {
            return List.of(single.toString());
        }
        return Collections.emptyList();
    }

    private NodeId safeParseNodeId(String text) {
        try {
            return NodeId.parse(text);
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid NodeId: " + text, e);
        }
    }
}
