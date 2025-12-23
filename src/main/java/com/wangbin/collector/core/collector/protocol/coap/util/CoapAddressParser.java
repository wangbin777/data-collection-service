package com.wangbin.collector.core.collector.protocol.coap.util;

import com.wangbin.collector.common.domain.entity.DataPoint;
import com.wangbin.collector.core.collector.protocol.coap.domain.CoapMethod;
import com.wangbin.collector.core.collector.protocol.coap.domain.CoapPoint;
import org.eclipse.californium.core.coap.MediaTypeRegistry;

import java.util.Map;

/**
 * 将 DataPoint 解析成 CoapPoint。
 */
public final class CoapAddressParser {

    private CoapAddressParser() {
    }

    public static CoapPoint parse(DataPoint point) {
        if (point == null) {
            throw new IllegalArgumentException("数据点不能为空");
        }
        String address = point.getAddress();
        String fullUri = null;
        String path = "";
        if (address != null && address.startsWith("coap://")) {
            fullUri = address;
        } else if (address != null && !address.isBlank()) {
            path = address;
        } else {
            Object pathCfg = point.getAdditionalConfig("path");
            if (pathCfg != null) {
                path = pathCfg.toString();
            }
        }
        if ((fullUri == null || fullUri.isBlank()) && (path == null || path.isBlank())) {
            throw new IllegalArgumentException("CoAP点位缺少路径: " + point.getPointName());
        }

        Map<String, Object> extra = point.getAdditionalConfig();
        CoapMethod method = CoapMethod.fromText(extra != null ? toString(extra.get("method")) : point.getCollectionMode());
        String query = extra != null ? toString(extra.get("query")) : null;
        int mediaType = parseMediaType(extra != null ? toString(extra.get("mediaType")) : null);
        boolean observe = resolveObserve(point, extra);
        boolean binary = resolveBinary(point, extra);

        return CoapPoint.builder()
                .fullUri(fullUri)
                .path(path == null ? "" : path)
                .method(method)
                .mediaType(mediaType)
                .observe(observe)
                .binary(binary)
                .query(query)
                .build();
    }

    private static boolean resolveObserve(DataPoint point, Map<String, Object> extra) {
        if (extra != null && extra.containsKey("observe")) {
            return Boolean.parseBoolean(extra.get("observe").toString());
        }
        return "SUBSCRIPTION".equalsIgnoreCase(point.getCollectionMode());
    }

    private static boolean resolveBinary(DataPoint point, Map<String, Object> extra) {
        if (extra != null && extra.containsKey("binary")) {
            return Boolean.parseBoolean(extra.get("binary").toString());
        }
        if (point.getDataType() != null) {
            String type = point.getDataType().toUpperCase();
            return type.contains("BYTE") || type.contains("BINARY");
        }
        return false;
    }

    private static int parseMediaType(String text) {
        if (text == null || text.isBlank()) {
            return MediaTypeRegistry.TEXT_PLAIN;
        }
        String upper = text.trim().toUpperCase();
        return switch (upper) {
            case "JSON", "APPLICATION/JSON" -> MediaTypeRegistry.APPLICATION_JSON;
            case "CBOR" -> MediaTypeRegistry.APPLICATION_CBOR;
            case "OCTET", "BINARY" -> MediaTypeRegistry.APPLICATION_OCTET_STREAM;
            case "TEXT", "TEXT/PLAIN" -> MediaTypeRegistry.TEXT_PLAIN;
            default -> MediaTypeRegistry.TEXT_PLAIN;
        };
    }

    private static String toString(Object value) {
        return value != null ? value.toString() : null;
    }
}
