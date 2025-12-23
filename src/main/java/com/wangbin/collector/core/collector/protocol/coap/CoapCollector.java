package com.wangbin.collector.core.collector.protocol.coap;

import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.core.collector.protocol.coap.base.AbstractCoapCollector;
import com.wangbin.collector.core.collector.protocol.coap.domain.CoapPoint;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.californium.core.CoapClient;
import org.eclipse.californium.core.CoapResponse;
import org.eclipse.californium.core.WebLink;
import org.eclipse.californium.core.coap.MediaTypeRegistry;
import org.eclipse.californium.elements.exception.ConnectorException;

import org.eclipse.californium.core.CoapObserveRelation;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * CoAP 采集器实现。
 * 如需将观察/Trap 推送给上层缓存或作进一步处理，可在 handleNotification() 或 SNMP 轮询回调内接入业务管道
 */
@Slf4j
public class CoapCollector extends AbstractCoapCollector {

    private final Map<String, DataPoint> observeMapping = new ConcurrentHashMap<>();

    @Override
    public String getCollectorType() {
        return "COAP";
    }

    @Override
    public String getProtocolType() {
        return "COAP";
    }

    @Override
    protected void doConnect() throws Exception {
        initCoapConfig(deviceInfo);
        openClient();
    }

    @Override
    protected void doDisconnect() {
        closeClient();
        observeMapping.clear();
    }

    @Override
    protected Object doReadPoint(DataPoint point) throws Exception {
        CoapPoint coapPoint = parsePoint(point);
        CoapResponse response = send(coapPoint, new byte[0]);
        return convertResponse(response, coapPoint);
    }

    @Override
    protected Map<String, Object> doReadPoints(List<DataPoint> points) throws Exception {
        Map<String, Object> values = new HashMap<>();
        for (DataPoint point : points) {
            try {
                values.put(point.getPointId(), doReadPoint(point));
            } catch (Exception e) {
                log.error("CoAP批量读取失败 point={}", point.getPointName(), e);
                values.put(point.getPointId(), null);
            }
        }
        return values;
    }

    @Override
    protected boolean doWritePoint(DataPoint point, Object value) throws Exception {
        CoapPoint coapPoint = parsePoint(point);
        byte[] payload = buildPayload(value, coapPoint.isBinary());
        CoapResponse response = send(coapPoint, payload);
        return response != null && response.isSuccess();
    }

    @Override
    protected Map<String, Boolean> doWritePoints(Map<DataPoint, Object> points) throws Exception {
        Map<String, Boolean> results = new HashMap<>();
        for (Map.Entry<DataPoint, Object> entry : points.entrySet()) {
            try {
                boolean success = doWritePoint(entry.getKey(), entry.getValue());
                results.put(entry.getKey().getPointId(), success);
            } catch (Exception e) {
                log.error("CoAP写入失败 point={}", entry.getKey().getPointName(), e);
                results.put(entry.getKey().getPointId(), false);
            }
        }
        return results;
    }

    @Override
    protected void doSubscribe(List<DataPoint> points) {
        for (DataPoint point : points) {
            CoapPoint coapPoint = parsePoint(point);
            if (!coapPoint.isObserve()) {
                continue;
            }
            observeMapping.put(coapPoint.getObserveKey(), point);
            startObserve(coapPoint, createHandler(coapPoint));
        }
        log.info("CoAP观察订阅完成 size={}", observeMapping.size());
    }

    @Override
    protected void doUnsubscribe(List<DataPoint> points) {
        if (points == null || points.isEmpty()) {
            observeRelations.values().forEach(relation -> {
                try {
                    relation.proactiveCancel();
                } catch (Exception e) {
                    log.debug("取消观察异常", e);
                }
            });
            observeRelations.clear();
            observeMapping.clear();
            return;
        }
        for (DataPoint point : points) {
            CoapPoint coapPoint = parsePoint(point);
            observeMapping.remove(coapPoint.getObserveKey());
            stopObserve(coapPoint);
        }
    }

    @Override
    protected Map<String, Object> doGetDeviceStatus() {
        Map<String, Object> status = new HashMap<>();
        status.put("baseUri", baseUri);
        status.put("connected", baseClient != null);
        status.put("timeout", timeout);
        status.put("observeRelations", observeRelations.size());
        status.put("subscribedPoints", observeMapping.size());
        return status;
    }

    @Override
    protected Object doExecuteCommand(int unitId, String command, Map<String, Object> params) throws Exception {
        switch (command.toLowerCase()) {
            case "get":
                return doCommandGet(params);
            case "post":
                return doCommandWrite(params, true);
            case "put":
                return doCommandWrite(params, false);
            case "delete":
                return doCommandDelete(params);
            case "discover":
                return doCommandDiscover();
            default:
                throw new IllegalArgumentException("不支持的CoAP命令: " + command);
        }
    }

    @Override
    protected void buildReadPlans(String deviceId, List<DataPoint> points) {
        log.info("CoAP点位加载完成 device={} count={}", deviceId, points.size());
    }

    @Override
    protected void handleNotification(CoapPoint point, CoapResponse response) {
        DataPoint dataPoint = observeMapping.get(point.getObserveKey());
        if (dataPoint == null) {
            return;
        }
        Object value = convertResponse(response, point);
        log.info("CoAP推送 pointId={} value={}", dataPoint.getPointId(), value);
    }

    private byte[] buildPayload(Object value, boolean binary) {
        if (value == null) {
            return new byte[0];
        }
        if (binary && value instanceof byte[] bytes) {
            return bytes;
        }
        return value.toString().getBytes();
    }

    private Object doCommandGet(Map<String, Object> params) throws Exception {
        String uri = Objects.toString(params.get("uri"), baseUri);
        CoapClient client = new CoapClient(uri);
        CoapResponse response = client.get();
        client.shutdown();
        if (response == null) {
            return Map.of("status", "timeout");
        }
        return Map.of(
                "status", response.isSuccess() ? "success" : "error",
                "code", response.getCode(),
                "payload", response.getResponseText()
        );
    }

    private Object doCommandWrite(Map<String, Object> params, boolean post) throws Exception {
        String uri = Objects.toString(params.get("uri"), baseUri);
        String payload = Objects.toString(params.get("payload"), "");
        String mediaTypeText = Objects.toString(params.get("mediaType"), "text/plain");
        int mediaType = MediaTypeRegistry.parse(mediaTypeText);
        if (mediaType < 0) {
            mediaType = MediaTypeRegistry.TEXT_PLAIN;
        }
        CoapClient client = new CoapClient(uri);
        CoapResponse response = post ? client.post(payload, mediaType) : client.put(payload, mediaType);
        client.shutdown();
        if (response == null) {
            return Map.of("status", "timeout");
        }
        return Map.of("status", response.isSuccess() ? "success" : "error", "code", response.getCode());
    }

    private Object doCommandDelete(Map<String, Object> params) throws Exception {
        String uri = Objects.toString(params.get("uri"), baseUri);
        CoapClient client = new CoapClient(uri);
        CoapResponse response = client.delete();
        client.shutdown();
        if (response == null) {
            return Map.of("status", "timeout");
        }
        return Map.of("status", response.isSuccess() ? "success" : "error", "code", response.getCode());
    }

    private Object doCommandDiscover() throws ConnectorException, java.io.IOException {
        Set<WebLink> resources = baseClient.discover();
        List<Map<String, Object>> list = new ArrayList<>();
        if (resources != null) {
            for (WebLink link : resources) {
                list.add(Map.of(
                        "uri", link.getURI(),
                        "attributes", link.getAttributes().getAttributeKeySet()
                ));
            }
        }
        return list;
    }
}
