package com.wangbin.collector.core.collector.protocol.coap.base;

import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.common.domain.entity.DeviceConnection;
import com.wangbin.collector.core.collector.protocol.base.BaseCollector;
import com.wangbin.collector.core.collector.protocol.coap.domain.CoapPoint;
import com.wangbin.collector.core.collector.protocol.coap.util.CoapAddressParser;
import com.wangbin.collector.core.config.CollectorProperties;
import com.wangbin.collector.core.connection.adapter.CoapConnectionAdapter;
import com.wangbin.collector.core.connection.adapter.ConnectionAdapter;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.californium.core.CoapClient;
import org.eclipse.californium.core.CoapHandler;
import org.eclipse.californium.core.CoapObserveRelation;
import org.eclipse.californium.core.CoapResponse;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * CoAP 公共能力抽象。
 */
@Slf4j
public abstract class AbstractCoapCollector extends BaseCollector {

    protected CoapConnectionAdapter coapConnection;
    protected String baseUri;
    protected int timeout = 5000;
    protected int port = 5683;

    protected final Map<String, CoapObserveRelation> observeRelations = new ConcurrentHashMap<>();

    protected void initCoapConnection() throws Exception {
        ConnectionAdapter adapter = connectionManager.createConnection(deviceInfo);
        connectionManager.connect(deviceInfo.getDeviceId());
        if (!(adapter instanceof CoapConnectionAdapter coapAdapter)) {
            throw new IllegalStateException("CoAP连接适配器类型不匹配");
        }
        this.coapConnection = coapAdapter;
        this.timeout = Math.toIntExact(coapAdapter.getRequestTimeout());
        this.baseUri = coapAdapter.getBaseUri();
        log.info("CoAP连接已建立 uri={} timeout={}", baseUri, timeout);
    }

    protected void closeCoapConnection() {
        observeRelations.values().forEach(relation -> {
            try {
                relation.proactiveCancel();
            } catch (Exception e) {
                log.debug("关闭观察异常", e);
            }
        });
        observeRelations.clear();
        if (connectionManager != null && deviceInfo != null) {
            connectionManager.removeConnection(deviceInfo.getDeviceId());
        }
        coapConnection = null;
    }

    protected CoapPoint parsePoint(DataPoint point) {
        return CoapAddressParser.parse(point);
    }

    protected CoapResponse send(CoapPoint point, byte[] payload) throws Exception {
        if (coapConnection == null) {
            throw new IllegalStateException("CoAP连接尚未建立");
        }
        return coapConnection.execute(adapter -> {
            CoapClient client = adapter.createClient(point.resolveUri(baseUri));
            client.setTimeout((long) timeout);
            return switch (point.getMethod()) {
                case GET -> client.get();
                case POST -> client.post(payload, point.getMediaType());
                case PUT -> client.put(payload, point.getMediaType());
                case DELETE -> client.delete();
            };
        }, (long) timeout);
    }

    protected void startObserve(CoapPoint point, CoapHandler handler) {
        CoapClient client = createClient(point);
        CoapObserveRelation relation = client.observe(handler);
        observeRelations.put(point.getObserveKey(), relation);
    }

    protected void stopObserve(CoapPoint point) {
        CoapObserveRelation relation = observeRelations.remove(point.getObserveKey());
        if (relation != null) {
            relation.proactiveCancel();
        }
    }

    protected Object convertResponse(CoapResponse response, CoapPoint point) {
        if (response == null) {
            return null;
        }
        if (!response.isSuccess()) {
            log.warn("CoAP请求失败 code={} uri={}", response.getCode(), point.getPath());
            return null;
        }
        if (point.isBinary()) {
            return response.getPayload();
        }
        return response.getResponseText();
    }

    private CoapClient createClient(CoapPoint point) {
        String uri = point.resolveUri(baseUri);
        if (coapConnection != null) {
            return coapConnection.createClient(uri);
        }
        return new CoapClient(uri);
    }

    private String normalizePath(String path) {
        if (path == null || path.isBlank() || "/".equals(path)) {
            return "";
        }
        return path.startsWith("/") ? path : "/" + path;
    }

    protected CoapHandler createHandler(CoapPoint point) {
        return new CoapHandler() {
            @Override
            public void onLoad(CoapResponse response) {
                handleNotification(point, response);
            }

            @Override
            public void onError() {
                log.warn("CoAP观察发生错误 point={}", point.getObserveKey());
            }
        };
    }

    protected void handleNotification(CoapPoint point, CoapResponse response) {
        // 子类覆盖处理
    }

}
