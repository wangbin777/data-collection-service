package com.wangbin.collector.core.collector.protocol.coap.base;

import com.wangbin.collector.common.domain.entity.DeviceInfo;
import com.wangbin.collector.core.collector.protocol.base.BaseCollector;
import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.core.collector.protocol.coap.domain.CoapMethod;
import com.wangbin.collector.core.collector.protocol.coap.domain.CoapPoint;
import com.wangbin.collector.core.collector.protocol.coap.util.CoapAddressParser;
import com.wangbin.collector.core.config.CollectorProperties;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.californium.core.CoapClient;
import org.eclipse.californium.core.CoapHandler;
import org.eclipse.californium.core.CoapObserveRelation;
import org.eclipse.californium.core.CoapResponse;
import org.eclipse.californium.core.coap.MediaTypeRegistry;

import java.net.URI;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * CoAP 公共能力抽象。
 */
@Slf4j
public abstract class AbstractCoapCollector extends BaseCollector {

    protected CoapClient baseClient;
    protected String baseUri;
    protected int timeout = 5000;
    protected CollectorProperties.CoapConfig coapConfig;
    protected int port = 5683;

    protected final Map<String, CoapObserveRelation> observeRelations = new ConcurrentHashMap<>();

    protected void initCoapConfig(DeviceInfo deviceInfo) {
        this.coapConfig = collectorProperties != null
                ? collectorProperties.getCoap()
                : new CollectorProperties.CoapConfig();

        Map<String, Object> config = deviceInfo.getProtocolConfig();
        String scheme = config != null && config.get("scheme") != null
                ? config.get("scheme").toString()
                : coapConfig.getScheme();
        if (scheme == null || scheme.isBlank()) {
            scheme = "coap";
        }
        String host = deviceInfo.getIpAddress();
        if (host == null || host.isBlank()) {
            host = Objects.toString(config != null ? config.get("host") : null, "127.0.0.1");
        }
        int defaultPort = deviceInfo.getPort() != null ? deviceInfo.getPort() : port;
        port = config != null && config.get("port") != null
                ? Integer.parseInt(config.get("port").toString())
                : defaultPort;
        timeout = config != null && config.get("timeout") != null
                ? Integer.parseInt(config.get("timeout").toString())
                : coapConfig.getTimeout();
        String basePath = config != null && config.get("basePath") != null
                ? config.get("basePath").toString()
                : "";
        baseUri = scheme + "://" + host + ":" + port + normalizePath(basePath);
    }

    protected void openClient() throws Exception {
        if (baseUri == null) {
            throw new IllegalStateException("CoAP Base URI 未初始化");
        }
        baseClient = new CoapClient(new URI(baseUri));
        baseClient.setTimeout((long) timeout);
        log.info("CoAP客户端已建立 uri={} timeout={}", baseUri, timeout);
    }

    protected void closeClient() {
        observeRelations.values().forEach(relation -> {
            try {
                relation.proactiveCancel();
            } catch (Exception e) {
                log.debug("关闭观察异常", e);
            }
        });
        observeRelations.clear();
        if (baseClient != null) {
            baseClient.shutdown();
            baseClient = null;
        }
    }

    protected CoapPoint parsePoint(DataPoint point) {
        return CoapAddressParser.parse(point);
    }

    protected CoapResponse send(CoapPoint point, byte[] payload) throws Exception {
        CoapClient client = createClient(point);
        client.setTimeout((long) timeout);
        return switch (point.getMethod()) {
            case GET -> client.get();
            case POST -> client.post(payload, point.getMediaType());
            case PUT -> client.put(payload, point.getMediaType());
            case DELETE -> client.delete();
        };
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
