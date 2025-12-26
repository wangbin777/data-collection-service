package com.wangbin.collector.core.collector.protocol.coap;

import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.core.collector.protocol.coap.base.AbstractCoapCollector;
import com.wangbin.collector.core.collector.protocol.coap.domain.CoapPoint;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.californium.core.CoapClient;
import org.eclipse.californium.core.CoapResponse;
import org.eclipse.californium.core.WebLink;
import org.eclipse.californium.core.coap.MediaTypeRegistry;
import org.eclipse.californium.core.CoapObserveRelation;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * CoAP 閲囬泦鍣ㄥ疄鐜般€? * 濡傞渶灏嗚瀵?Trap 鎺ㄩ€佺粰涓婂眰缂撳瓨鎴栦綔杩涗竴姝ュ鐞嗭紝鍙湪 handleNotification() 鎴?SNMP 杞鍥炶皟鍐呮帴鍏ヤ笟鍔＄閬? */
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
        initCoapConnection();
    }

    @Override
    protected void doDisconnect() {
        closeCoapConnection();
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
                log.error("CoAP鎵归噺璇诲彇澶辫触 point={}", point.getPointName(), e);
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
                log.error("CoAP鍐欏叆澶辫触 point={}", entry.getKey().getPointName(), e);
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
        log.info("CoAP瑙傚療璁㈤槄瀹屾垚 size={}", observeMapping.size());
    }

    @Override
    protected void doUnsubscribe(List<DataPoint> points) {
        if (points == null || points.isEmpty()) {
            observeRelations.values().forEach(relation -> {
                try {
                    relation.proactiveCancel();
                } catch (Exception e) {
                    log.debug("鍙栨秷瑙傚療寮傚父", e);
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
        status.put("connected", coapConnection != null && coapConnection.isConnected());
        status.put("timeout", timeout);
        status.put("observeRelations", observeRelations.size());
        status.put("subscribedPoints", observeMapping.size());
        status.put("connectionStats", coapConnection != null ? coapConnection.getStatistics() : Map.of());
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
                throw new IllegalArgumentException("涓嶆敮鎸佺殑CoAP鍛戒护: " + command);
        }
    }

    @Override
    protected void buildReadPlans(String deviceId, List<DataPoint> points) {
        log.info("CoAP鐐逛綅鍔犺浇瀹屾垚 device={} count={}", deviceId, points.size());
    }

    @Override
    protected void handleNotification(CoapPoint point, CoapResponse response) {
        DataPoint dataPoint = observeMapping.get(point.getObserveKey());
        if (dataPoint == null) {
            return;
        }
        Object value = convertResponse(response, point);
        log.info("CoAP鎺ㄩ€?pointId={} value={}", dataPoint.getPointId(), value);
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
        CoapResponse response = coapConnection.execute(adapter -> {
            CoapClient client = adapter.createClient(uri);
            try {
                return client.get();
            } finally {
                client.shutdown();
            }
        }, (long) timeout);
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
        final String requestUri = uri;
        final String body = payload;
        final int resolvedMediaType = mediaType;
        CoapResponse response = coapConnection.execute(adapter -> {
            CoapClient client = adapter.createClient(requestUri);
            try {
                return post ? client.post(body, resolvedMediaType) : client.put(body, resolvedMediaType);
            } finally {
                client.shutdown();
            }
        }, (long) timeout);
        if (response == null) {
            return Map.of("status", "timeout");
        }
        return Map.of("status", response.isSuccess() ? "success" : "error", "code", response.getCode());
    }

    private Object doCommandDelete(Map<String, Object> params) throws Exception {
        String uri = Objects.toString(params.get("uri"), baseUri);
        CoapResponse response = coapConnection.execute(adapter -> {
            CoapClient client = adapter.createClient(uri);
            try {
                return client.delete();
            } finally {
                client.shutdown();
            }
        }, (long) timeout);
        if (response == null) {
            return Map.of("status", "timeout");
        }
        return Map.of("status", response.isSuccess() ? "success" : "error", "code", response.getCode());
    }

    private Object doCommandDiscover() throws Exception {
        Set<WebLink> resources = coapConnection != null
                ? coapConnection.execute(adapter -> {
                    CoapClient client = adapter.getClient();
                    if (client == null) {
                        throw new IllegalStateException("CoAP base client unavailable");
                    }
                    return client.discover();
                }, (long) timeout)
                : null;
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
