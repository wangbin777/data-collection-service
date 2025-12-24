package com.wangbin.collector.core.collector.protocol.coap.base;

import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.core.collector.protocol.base.BaseCollector;
import com.wangbin.collector.core.collector.protocol.coap.domain.CoapPoint;
import com.wangbin.collector.core.collector.protocol.coap.util.CoapAddressParser;
import com.wangbin.collector.core.config.CollectorProperties;
import com.wangbin.collector.core.connection.adapter.CoapConnectionAdapter;
import com.wangbin.collector.core.connection.adapter.ConnectionAdapter;
import com.wangbin.collector.core.connection.model.ConnectionConfig;
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
    protected ConnectionConfig managedConnectionConfig;
    protected String baseUri;
    protected int timeout = 5000;
    protected CollectorProperties.CoapConfig coapConfig;
    protected int port = 5683;

    protected final Map<String, CoapObserveRelation> observeRelations = new ConcurrentHashMap<>();

    protected void initCoapConnection() throws Exception {
        this.managedConnectionConfig = buildConnectionConfig();
        ConnectionAdapter adapter = connectionManager.createConnection(managedConnectionConfig);
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
        managedConnectionConfig = null;
    }

    protected ConnectionConfig buildConnectionConfig() {
        this.coapConfig = collectorProperties != null
                ? collectorProperties.getCoap()
                : new CollectorProperties.CoapConfig();
        Map<String, Object> protocolCfg = deviceInfo.getProtocolConfig() != null
                ? new HashMap<>(deviceInfo.getProtocolConfig())
                : new HashMap<>();
        String scheme = resolveString(protocolCfg, "scheme", coapConfig.getScheme() != null ? coapConfig.getScheme() : "coap");
        String host = deviceInfo.getIpAddress();
        if (host == null || host.isBlank()) {
            host = Objects.toString(protocolCfg.getOrDefault("host", "127.0.0.1"));
        }
        Object portOverride = protocolCfg.containsKey("port") ? protocolCfg.get("port") : deviceInfo.getPort();
        this.port = portOverride instanceof Number ? ((Number) portOverride).intValue()
                : portOverride != null ? Integer.parseInt(portOverride.toString()) : 5683;
        this.timeout = resolveInt(protocolCfg, "timeout", coapConfig.getTimeout());
        String basePath = resolveString(protocolCfg, "basePath", "");
        this.baseUri = scheme + "://" + host + ":" + port + normalizePath(basePath);

        ConnectionConfig config = new ConnectionConfig();
        config.setDeviceId(deviceInfo.getDeviceId());
        config.setDeviceName(deviceInfo.getDeviceName());
        config.setProtocolType(getProtocolType());
        config.setConnectionType("COAP");
        config.setHost(host);
        config.setPort(port);
        config.setUrl(baseUri);
        config.setReadTimeout(timeout);
        config.setWriteTimeout(timeout);
        config.setConnectTimeout(resolveInt(deviceInfo.getConnectionConfig(), "connectTimeout", 5000));
        config.setGroupId(deviceInfo.getGroupId());
        config.setMaxPendingMessages(resolveInt(deviceInfo.getConnectionConfig(), "maxPendingMessages", 1024));
        config.setDispatchBatchSize(resolveInt(deviceInfo.getConnectionConfig(), "dispatchBatchSize", 1));
        config.setDispatchFlushInterval(resolveLong(deviceInfo.getConnectionConfig(), "dispatchFlushInterval", 0L));
        config.setOverflowStrategy(resolveString(deviceInfo.getConnectionConfig(), "overflowStrategy", "BLOCK"));
        config.setProtocolConfig(protocolCfg);
        if (deviceInfo.getConnectionConfig() != null) {
            config.setExtraParams(new HashMap<>(deviceInfo.getConnectionConfig()));
        }
        return config;
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

    private int resolveInt(Map<String, Object> source, String key, int defaultValue) {
        if (source == null || !source.containsKey(key) || source.get(key) == null) {
            return defaultValue;
        }
        Object value = source.get(key);
        if (value instanceof Number number) {
            return number.intValue();
        }
        try {
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private long resolveLong(Map<String, Object> source, String key, long defaultValue) {
        if (source == null || !source.containsKey(key) || source.get(key) == null) {
            return defaultValue;
        }
        Object value = source.get(key);
        if (value instanceof Number number) {
            return number.longValue();
        }
        try {
            return Long.parseLong(value.toString());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private String resolveString(Map<String, Object> source, String key, String defaultValue) {
        if (source == null || !source.containsKey(key) || source.get(key) == null) {
            return defaultValue;
        }
        String val = source.get(key).toString();
        return val.isBlank() ? defaultValue : val;
    }
}
